namespace ReliableJournal.Replica
{
    using System;
    using System.Collections.Generic;
    using System.Fabric;
    using System.IO.Log;
    using System.Threading;
    using System.Threading.Tasks;

    using ReliableJournal.Log;
    using ReliableJournal.Log.Utilities;
    using ReliableJournal.Records;
    using ReliableJournal.Replica.Progress;
    using ReliableJournal.Serialization;
    using ReliableJournal.Utilities;

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

        private readonly Serializer serializer;

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

        private readonly LogFile log;

        private readonly IReplicationNotifier replicationNotifier;

        private bool hasRecord;

        public CopyStateStream(
            long upToSequenceNumber,
            IOperationDataStream copyContext,
            ProgressVector localProgress,
            LogFile operationLog,
            IReplicationNotifier replicationNotifier,
            Logger logger,
            Serializer serializer)
        {
            this.upToSequenceNumber = upToSequenceNumber;
            this.copyContext = copyContext;
            this.localProgress = localProgress;
            this.logger = logger;
            this.serializer = serializer;
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
            var totalSize = 0;

            // Wait for the required records to become available.
            this.hasRecord = this.records.MoveNext();
            while (this.hasRecord)
            {
                var rawRecord = this.records.Current;
                var record = this.serializer.Deserialize<Record>(rawRecord);
                var operation = record as OperationCommittedRecord;
                if (operation == null)
                {
                    this.logger.Log(
                        $"CopyStateStream: {record} is not an {nameof(OperationCommittedRecord)}, it is an {record.GetType()}.");
                    this.hasRecord = this.records.MoveNext();
                    continue;
                }

                var currentVersion = operation.Version;
                var currentLsn = currentVersion.LogSequenceNumber;
                if (currentLsn > this.upToSequenceNumber)
                {
                    break;
                }

                // Only return records which were actually committed.
                if (!this.catchUpProgressVector.IncludesVersionInPreviousEpoch(currentVersion)
                    && this.catchUpProgressVector.Current.Epoch != currentVersion.Epoch)
                {
#warning we need a bucket load of tests around this to motivate correctness. How is the 'zero' epoch handled? Does such a thing even exist?
                    /*this.logger.Log(
                        $"CopyStateStream: {currentVersion} is not in the catchup vector or current epoch ({this.catchUpProgressVector.Current.Epoch.ToDisplayString()})");*/
                    this.hasRecord = this.records.MoveNext();
                    continue;
                }

                if (result == null)
                {
                    this.lowestLsn = currentLsn;
                }

                this.highestLsn = this.highestLsn > currentLsn ? this.highestLsn : currentLsn;

                // Copy the payload into the result.
                var data = this.serializer.Serialize(record);
                if (result == null)
                {
                    result = new OperationData(data);
                }
                else
                {
                    result.Add(data);
                }

                this.hasRecord = this.records.MoveNext();

                // If a sequence number which has not yet been committed has been requested, wait for that before continuing.
                if (!this.hasRecord && this.highestLsn != this.upToSequenceNumber)
                {
                    await this.WaitForCommit(rawRecord.SequenceNumber, cancellationToken);
                }

                // Send only a certain amount of data at a time.
                totalSize += data.Count;
                if (totalSize > ResultCutoffLength)
                {
                    break;
                }
            }

            this.logger.Log(
                result == null
                    ? "Completed copying state"
                    : $"CopyStateStream.GetNextAsync returning {result.Count} records, from LSN {this.lowestLsn} to {this.highestLsn}");

            return result;
        }

        private async Task WaitForCommit(SequenceNumber currentSequenceNumber, CancellationToken cancellationToken)
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
                    this.logger.Log($"Warning, waited {iterations * 100}ms for commit of {this.upToSequenceNumber}");
                }

                await this.replicationNotifier.WaitForCommit(this.upToSequenceNumber);
                this.records = this.log.GetRecords(currentSequenceNumber, LogRecordEnumeratorType.Next).GetEnumerator();
                this.hasRecord = this.records.MoveNext();
            }
            while (this.hasRecord && this.log.LastSequenceNumber == currentSequenceNumber);

            var from = currentSequenceNumber.Identifier();
            var to = this.hasRecord ? this.records.Current.SequenceNumber.Identifier() : SequenceNumber.Invalid.Identifier();
            this.logger.Log(
                $"CopyStateStream: {this.upToSequenceNumber} was committed, continuing."
                + $"HasNext: {this.hasRecord}. Moving from SequenceNumber: {@from} to {to}");
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
                    result.Update(this.serializer.Deserialize<ProgressIndicator>(item));
                }
            }
            while (true);

            return result;
        }
    }
}