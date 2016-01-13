namespace ReliableJournal.Replica
{
    using System;
    using System.Collections.Generic;
    using System.Fabric;
    using System.IO.Log;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;

    using ReliableJournal.Log;
    using ReliableJournal.Log.Utilities;
    using ReliableJournal.Records;
    using ReliableJournal.Replica.Progress;
    using ReliableJournal.Serialization;
    using ReliableJournal.Utilities;

    internal sealed class StateProvider : IStateProvider, IDisposable, IReplicationNotifier
    {
        private readonly LogFile operationLog;

        private readonly ProgressVector progressVector = new ProgressVector();

        private readonly LogFile progressVectorLog;

        private readonly Logger logger;

        private readonly Serializer serializer;

        private long highestCommittedLogSequenceNumber;

        private bool disposed;

        private SequenceNumber prevRecord = SequenceNumber.Invalid;

        public StateProvider(string logFilePath, Logger logger, Serializer serializer)
        {
            this.logger = logger;
            this.serializer = serializer;

            this.logger.Log($"StateProvider({logFilePath})");

            // Open the progress vector stream from the log.
            this.progressVectorLog = new LogFile(this.logger.Log);
            this.progressVectorLog.Open(logFilePath, "ProgressVector");

            // Open the main stream from the log.
            this.operationLog = new LogFile(this.logger.Log);
            this.operationLog.Open(logFilePath, "Operations");
        }

        /// <summary>
        /// Get the current epoch.
        /// </summary>
        public Epoch CurrentEpoch => this.progressVector.Current.Epoch;

        public Task Initialize()
        {
            this.logger.Log("Initializing StateProvider.");

            // Rebuild the local progress vector.
            var records = this.serializer.Deserialize<Record>(this.progressVectorLog.GetAllRecords());
            var progressUpdates = records.OfType<EpochUpdatedRecord>();
            foreach (var progressUpdate in progressUpdates)
            {
                this.logger.Log(
                    $"{nameof(StateProvider)}.{nameof(this.Initialize)}: got progress {progressUpdate.Progress}");
                this.progressVector.Update(progressUpdate.Progress);
            }

            this.highestCommittedLogSequenceNumber = this.progressVector.SequenceNumber;

            // Update the highest committed sequence number with the persitent state.
            var lastRecord = this.operationLog.GetLastRecord();
            if (lastRecord != null)
            {
                this.prevRecord = lastRecord.SequenceNumber;
                this.logger.Log($"LastRecord: {lastRecord.DetailedString()}");
                var opLogEntry = this.serializer.Deserialize<Record>(lastRecord) as OperationCommittedRecord;
                if (opLogEntry != null)
                {
                    this.logger.Log($"LastRecord detail: {opLogEntry}");
                    this.UpdateHighestLogSequenceNumber(opLogEntry.Version.LogSequenceNumber);
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
            return new CopyContextStream(this.progressVector, this.logger, this.serializer);
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
                this.logger,
                this.serializer);
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

        private Task PersistEpochUpdate(ProgressIndicator update)
        {
            // Write the new progress vector component to the log.
            return this.progressVectorLog.Append(
                this.serializer.Serialize<Record>(new EpochUpdatedRecord(update)),
                SequenceNumber.Invalid,
                SequenceNumber.Invalid,
                RecordAppendOptions.ForceFlush);
        }

        public void Dispose()
        {
            if (!this.disposed)
            {
                this.operationLog?.Dispose();
                this.disposed = true;
            }
        }

        public async Task AppendOperation(ArraySegment<byte> record, long logSequenceNumber)
        {
            this.prevRecord =
                await
                this.operationLog.Append(
                    record,
                    this.prevRecord,
                    SequenceNumber.Invalid,
                    RecordAppendOptions.ForceFlush);
            this.UpdateHighestLogSequenceNumber(logSequenceNumber);
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

        public IEnumerable<LogRecord> GetOperations()
        {
            return this.operationLog.GetAllRecords();
        }
    }
}