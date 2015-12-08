namespace DistributedJournalService.Replica
{
    using System;
    using System.Collections.Generic;
    using System.Fabric;
    using System.IO;
    using System.IO.Log;
    using System.Threading;
    using System.Threading.Tasks;

    using DistributedJournalService.Data;
    using DistributedJournalService.Records;
    using DistributedJournalService.Utilities;

    using JournalFile;
    using JournalFile.Utilities;

    using ProtoBuf;

    internal class CopyStateStream : IOperationDataStream
    {
        private const int ResultCutoffLength = 1024000;

        /// <summary>
        /// The local progress vector, which is the progress vector of the primary.
        /// </summary>
        private readonly ProgressVector localProgress;

        /// <summary>
        /// The logger.
        /// </summary>
        private readonly Logger logger;

        /// <summary>
        /// The copy context data stream from the secondary replica which is requesting the copy stream.
        /// </summary>
        private readonly IOperationDataStream copyContext;

        /// <summary>
        /// The log sequence number of the latest operation which should be copied.
        /// </summary>
        private readonly long upToSequenceNumber;
        
        /// <summary>
        /// The requesting replica's progress vector.
        /// </summary>
        private ProgressVector catchUpProgressVector;

        /// <summary>
        /// The records in the operation log.
        /// </summary>
        private IEnumerator<LogRecord> records;

        private long lowestLsn;

        private long highestLsn;

        private readonly TransactionLog log;

        private readonly IReplicationNotifier replicationNotifier;

        private bool hasNext;

        public CopyStateStream(
            long upToSequenceNumber,
            IOperationDataStream copyContext,
            ProgressVector localProgress,
            TransactionLog operationLog,
            IReplicationNotifier replicationNotifier,
            Logger logger)
        {
            this.upToSequenceNumber = upToSequenceNumber;
            this.copyContext = copyContext;
            this.localProgress = localProgress;
            this.logger = logger;
            this.log = operationLog;
            this.replicationNotifier = replicationNotifier;
            this.records = operationLog.GetAllRecords().GetEnumerator();
        }

        public async Task<OperationData> GetNextAsync(CancellationToken cancellationToken)
        {
            // Get the progress of the remote replica.
            if (this.catchUpProgressVector == null)
            {
                var remoteProgress = await this.GetRemoteReplicaProgress();
                this.catchUpProgressVector = new ProgressVector(this.localProgress.Excluding(remoteProgress));
                this.logger.Log(
                    "CopyStateStream:\n"
                    + $"Local progress:  {this.localProgress} (& up to LSN {this.upToSequenceNumber})\n"
                    + $"Remote progress: {remoteProgress}\n" + $"Delta Progress:  {this.catchUpProgressVector}");
            }

            // Nothing to copy.
            if (this.upToSequenceNumber == 0)
            {
                return null;
            }

            // Find the first entry which needs to be replicated
            var result = default(OperationData);
            using (var stream = MemoryStreamManager.Instance.GetStream("CopyContext.GetNextAsync"))
            {
                var totalSize = 0;
                
                // Wait for the required records to become available.
                this.hasNext = this.records.MoveNext();
                while (this.hasNext)
                {
                    var rawRecord = this.records.Current;
                    var record = Serializer.Deserialize<LogEntry>(rawRecord.Data);

                    var operation = record as OperationLogEntry;
                    if (operation == null)
                    {
                        this.hasNext = this.records.MoveNext();
                        continue;
                    }

                    var currentVersion = operation.DataVersion;
                    var currentLsn = currentVersion.LogSequenceNumber;
                    if (currentLsn > this.upToSequenceNumber)
                    {
                        break;
                    }
                    
                    // Only return records which were actually committed.
                    if (this.catchUpProgressVector.IncludesVersionInPreviousEpoch(currentVersion)
                        || this.catchUpProgressVector.Current.Epoch == currentVersion.Epoch)
                    {
                        if (result == null)
                        {
                            this.lowestLsn = currentLsn;
                        }

                        this.highestLsn = this.highestLsn > currentLsn ? this.highestLsn : currentLsn;

                        // Copy the record to the stream.
                        rawRecord.Data.Seek(0, SeekOrigin.Begin);
                        await rawRecord.Data.CopyToAsync(stream);
                        var data = stream.ToArray();

                        // Reset the stream for next time.
                        stream.Position = 0;
                        stream.SetLength(0);


                        // Copy the stream into the result.
                        if (result == null)
                        {
                            result = new OperationData(data);
                        }
                        else
                        {
                            result.Add(new ArraySegment<byte>(data));
                        }

                        this.hasNext = this.records.MoveNext();

                        // If there are currently no more records, 
                        if (!this.hasNext && this.highestLsn != this.upToSequenceNumber)
                        {
                            this.logger.Log(
                                $"Needing to replicate {this.upToSequenceNumber}, but no records remain and have only seen {this.highestLsn}");
                            var iterations = 0;
                            do
                            {
                                if (iterations++ > 1)
                                {
                                    await Task.Delay(TimeSpan.FromMilliseconds(100), cancellationToken);
                                }
                                if (iterations > 50)
                                {
                                    this.logger.Log(
                                        $"Warning, waited {iterations * 100}ms for commit of {this.upToSequenceNumber}");
                                }

                                await this.replicationNotifier.WaitForCommit(this.upToSequenceNumber);
                                this.records =
                                    this.log.GetRecords(rawRecord.SequenceNumber, LogRecordEnumeratorType.Next)
                                        .GetEnumerator();
                                this.hasNext = this.records.MoveNext();
                            }
                            while (this.hasNext && this.log.LastSequenceNumber == rawRecord.SequenceNumber);
                            

                            var from = rawRecord.SequenceNumber.Identifier();
                            var to = this.hasNext
                                         ? this.records.Current.SequenceNumber.Identifier()
                                         : SequenceNumber.Invalid.Identifier();
                            this.logger.Log(
                                $"CopyStateStream: {this.upToSequenceNumber} was committed, continuing."
                                + $"HasNext: {this.hasNext}. Moving from SequenceNumber: {from} to {to}");
                        }

                        // Send only a certain amount of data at a time.
                        totalSize += data.Length;
                        if (totalSize > ResultCutoffLength)
                        {
                            break;
                        }
                    }
                }

                this.logger.Log(
                    result == null
                        ? "Completed copying state"
                        : $"CopyStateStream.GetNextAsync returning {result.Count} records, from LSN {this.lowestLsn} to {this.highestLsn}");
            }

            return result;
        }

        private async Task<ProgressVector> GetRemoteReplicaProgress()
        {
            var result = new ProgressVector();
            do
            {
                var copyContextCancellationToken = new CancellationTokenSource(TimeSpan.FromMinutes(5)).Token;
                var context = await this.copyContext.GetNextAsync(copyContextCancellationToken);
                if (context == null)
                {
                    break;
                }

                foreach (var item in context)
                {
                    result.Update(item.Deserialize<ProgressIndicator>());
                }
            }
            while (true);

            return result;
        }
    }
}