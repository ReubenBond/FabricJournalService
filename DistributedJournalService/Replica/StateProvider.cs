namespace DistributedJournalService.Replica
{
    using System;
    using System.Collections.Generic;
    using System.Fabric;
    using System.IO.Log;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;

    using DistributedJournalService.Data;
    using DistributedJournalService.Operations;
    using DistributedJournalService.Records;
    using DistributedJournalService.Utilities;

    using JournalFile;
    using JournalFile.Utilities;

    using ProtoBuf;

    internal sealed class StateProvider : IStateProvider, IDisposable, IReplicationNotifier
    {
        private readonly TransactionLog operationLog;

        private readonly ProgressVector progressVector = new ProgressVector();

        private readonly TransactionLog progressVectorLog;

        private readonly Logger logger;

        private long highestCommittedLogSequenceNumber;

        private bool disposed;

        private SequenceNumber prevRecord = SequenceNumber.Invalid;

        public StateProvider(string logFilePath, Logger logger)
        {
            this.logger = logger;

            this.logger.Log($"StateProvider({logFilePath})");

            // Open the progress vector stream from the log.
            this.progressVectorLog = new TransactionLog(this.logger.Log);
            this.progressVectorLog.Open(logFilePath, "ProgressVector");

            // Open the main stream from the log.
            this.operationLog = new TransactionLog(this.logger.Log);
            this.operationLog.Open(logFilePath, "Operations");
        }

        public Task Initialize()
        {
            this.logger.Log("Initializing StateProvider.");

            // Rebuild the local progress vector.
            var progressRecords = this.progressVectorLog.GetAllRecords();
            foreach (var record in progressRecords)
            {
                var logRecord = Serializer.Deserialize<LogEntry>(record.Data);
                var progressUpdate = logRecord as EpochUpdateLogEntry;
                if (progressUpdate != null)
                {
                    this.logger.Log(
                        $"{nameof(StateProvider)}.{nameof(this.Initialize)}: got progress {progressUpdate.Progress}");
                    this.progressVector.Update(progressUpdate.Progress);
                }
            }
            
            this.highestCommittedLogSequenceNumber = this.progressVector.SequenceNumber;

            // Update the highest committed sequence number with the persitent state.
            foreach (var record in this.operationLog.GetLastRecord().Take(1))
            {
                this.prevRecord = record.SequenceNumber;
                this.logger.Log($"LastRecord: {record.DetailedString()}");
                var logRecord = Serializer.Deserialize<LogEntry>(record.Data);
                var opLogEntry = logRecord as OperationLogEntry;
                if (opLogEntry != null)
                {
                    this.logger.Log($"LastRecord detail: {opLogEntry}");
                    this.UpdateHighestLogSequenceNumber(opLogEntry.DataVersion.LogSequenceNumber);
                }
            }

            this.logger.Log("Initialized StateProvider.");
            return Task.FromResult(0);

#warning find a more scalable approach so we can avoid copying entire progress vector into memory.
        }

        public long GetLastCommittedSequenceNumber()
        {
            var result = Math.Max(this.progressVector.SequenceNumber, this.highestCommittedLogSequenceNumber);
            this.logger.Log($"GetLastCommittedSequenceNumber() => {result}");
            return result;
        }

        /// <summary>
        /// Returns the current progress of this secondary replica encoded as a stream.
        /// </summary>
        /// <returns></returns>
        public IOperationDataStream GetCopyContext()
        {
            this.logger.Log("GetCopyContext()");
            return new CopyContextStream(this.progressVector, this.logger);
        }

        /// <summary>
        /// Gets the copy stream from the primary replica.
        /// </summary>
        /// <param name="upToSequenceNumber">The upper-bound of the data to return in the copy stream.</param>
        /// <param name="copyContext">The copy context from the secondary which will consume the returned copy stream.</param>
        /// <returns></returns>
        public IOperationDataStream GetCopyState(long upToSequenceNumber, IOperationDataStream copyContext)
        {
            this.logger.Log($"GetCopyState({upToSequenceNumber})");
            return new CopyStateStream(
                upToSequenceNumber,
                copyContext,
                this.progressVector,
                this.operationLog,
                this,
                this.logger);
        }

        public Task<bool> OnDataLossAsync(CancellationToken cancellationToken)
        {
            this.logger.Log("OnDataLossAsync()");
            return Task.FromResult(true);
        }

        public async Task UpdateEpochAsync(
            Epoch epoch,
            long previousEpochLastSequenceNumber,
            CancellationToken cancellationToken)
        {
            var update = new ProgressIndicator(epoch, previousEpochLastSequenceNumber);
            this.logger.Log($"UpdateEpochAsync: {update}");

            this.progressVector.Update(update);
            await this.PersistEpochUpdate(update);

            this.logger.Log($"ProgressVector: {this.progressVector}");
        }

        private async Task PersistEpochUpdate(ProgressIndicator update)
        {
            // Write the new progress vector component to the log.
            using (var stream = MemoryStreamManager.Instance.GetStream(nameof(this.UpdateEpochAsync)))
            {
                Serializer.Serialize(stream, new EpochUpdateLogEntry(update));
                stream.Position = 0;

                await
                    this.progressVectorLog.Append(
                        new ArraySegment<byte>(stream.GetBuffer(), 0, (int)stream.Length),
                        SequenceNumber.Invalid,
                        SequenceNumber.Invalid,
                        RecordAppendOptions.ForceFlush);
            }
        }

        public void Dispose()
        {
            if (!this.disposed)
            {
                this.operationLog?.Dispose();
                this.disposed = true;
            }
        }

        public Task AppendOperationData(OperationData operationData, long logSequenceNumber)
        {
            var op = operationData[0];
            //this.logger.Log($"StateProvider.AppendOperation({op.Count}b, lsn: {logSequenceNumber})");
            Operation deserializedOperation;
            using (
                var stream = MemoryStreamManager.Instance.GetStream(
                    "StateProvider.AppendOperation",
                    op.Array,
                    op.Offset,
                    op.Count))
            {
                deserializedOperation = Serializer.Deserialize<Operation>(stream);
            }

            return this.AppendOperation(deserializedOperation, logSequenceNumber);
        }

        public async Task AppendOperation(Operation operation, long logSequenceNumber, Epoch epoch = default(Epoch))
        {
            if (epoch == default(Epoch))
            {
                epoch = this.progressVector.Current.Epoch;
            }

            using (var outputStream = MemoryStreamManager.Instance.GetStream("StateProvider.AppendOperation.Serialize"))
            {
                var version = new DataVersion(epoch, logSequenceNumber);
                var entry = new OperationLogEntry(operation, version);
                Serializer.Serialize(outputStream, entry);
                this.prevRecord =
                    await
                    this.operationLog.Append(
                        new ArraySegment<byte>(outputStream.GetBuffer(), 0, (int)outputStream.Length),
                        this.prevRecord,
                        SequenceNumber.Invalid,
                        RecordAppendOptions.ForceFlush);
                this.UpdateHighestLogSequenceNumber(logSequenceNumber);
            }
        }

        private void UpdateHighestLogSequenceNumber(long logSequenceNumber)
        {
            var highestLsn = this.highestCommittedLogSequenceNumber;
            while (highestLsn < logSequenceNumber)
            {
                var newValue = Interlocked.CompareExchange(
                    ref this.highestCommittedLogSequenceNumber,
                    logSequenceNumber,
                    highestLsn);
                if (newValue == highestLsn)
                {
                    //this.logger.Log($"LSN updated from {highestLsn} to {logSequenceNumber}");
                    return;
                }

                highestLsn = this.highestCommittedLogSequenceNumber;
            }

            //this.logger.Log($"LSN NOT updated from {highestLsn} to {logSequenceNumber}");
        }

        public async Task WaitForCommit(long logSequenceNumber)
        {
            var highestLsn = Interlocked.Read(ref this.highestCommittedLogSequenceNumber);
            while (highestLsn < logSequenceNumber)
            {
                highestLsn = Interlocked.Read(ref this.highestCommittedLogSequenceNumber);
                await Task.Yield();
            }
        }

        public IEnumerable<OperationLogEntry> GetOperations()
        {
            foreach (var record in this.operationLog.GetAllRecords())
            {
                yield return Serializer.Deserialize<OperationLogEntry>(record.Data);
            }
        }
    }
}