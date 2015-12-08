namespace JournalFile
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using System.IO.Log;
    using System.Threading.Tasks;

    using JournalFile.Utilities;

    public class TransactionLog : IDisposable
    {
        private LogRecordSequence log;

        private readonly Action<string> logger;
        
        private readonly object storeLock = new object();

        private string logFileBase;

        /// <summary>
        /// The size of each log extent, in bytes.
        /// </summary>
        private const int ExtentSize = 10 * 1024 * 1024;

        public TransactionLog(Action<string> logger)
        {
            this.logger = logger;
        }

        public void Open(string path, string streamId = null)
        {
            lock (this.storeLock)
            {
                this.logFileBase = path;
                var logFileName = string.IsNullOrEmpty(streamId) ? path : $"{path}::{streamId}";
                this.log = new LogRecordSequence(
                    logFileName,
                    FileMode.OpenOrCreate,
                    FileAccess.ReadWrite,
                    FileShare.ReadWrite) { RetryAppend = true };
                this.log.TailPinned += this.TailPinned;
                if (this.log.LogStore.Extents.Count < 2)
                {
                    this.log.LogStore.Extents.Add($"{path}.log.0", ExtentSize);
                    this.log.LogStore.Extents.Add($"{path}.log.1");
                }

                this.log.LogStore.Policy.AutoGrow = true;
                this.log.LogStore.Policy.GrowthRate = new PolicyUnit(5, PolicyUnitType.Extents);
                this.log.LogStore.Policy.NewExtentPrefix = $"{path}.log.";
                this.log.LogStore.Policy.NextExtentSuffix = this.log.LogStore.Extents.Count;
                this.log.LogStore.Policy.MaximumExtentCount = int.MaxValue;
                this.log.LogStore.Policy.Commit();
                this.log.LogStore.Policy.Refresh();
            }
        }

        private void TailPinned(object sender, TailPinnedEventArgs e)
        {
            Debug.WriteLine($"Log {this.logFileBase}: tail pinned! Suggested that we advance base to {e.TargetSequenceNumber.Identifier()}");
        }

        public SequenceNumber BaseSequenceNumber => this.log.BaseSequenceNumber;
        public SequenceNumber RestartSequenceNumber => this.log.RestartSequenceNumber;
        public SequenceNumber LastSequenceNumber => this.log.LastSequenceNumber;

        public IEnumerable<LogRecord> GetAllRecords()
        {
            return this.GetRecords(this.BaseSequenceNumber, LogRecordEnumeratorType.Next);
        }

        public IEnumerable<LogRecord> GetRecords(SequenceNumber start, LogRecordEnumeratorType enumeratorType)
        {
            return this.log.ReadLogRecords(start, enumeratorType);
        }

        public IEnumerable<LogRecord> GetLastRecord()
        {
            return this.log.ReadLogRecords(this.log.LastSequenceNumber, LogRecordEnumeratorType.Previous);
        }

        public Task<SequenceNumber> Append(
            ArraySegment<byte> record,
            SequenceNumber nextUndoRecord,
            SequenceNumber previousRecord,
            RecordAppendOptions options)
        {
            this.EnsureAvailableSpace();
            return this.log.AppendAsync(record, nextUndoRecord, previousRecord, options);
        }

        public Task<SequenceNumber> Append(
            IList<ArraySegment<byte>> record,
            SequenceNumber nextUndoRecord,
            SequenceNumber previousRecord,
            RecordAppendOptions options)
        {
            this.EnsureAvailableSpace();
            return this.log.AppendAsync(record, nextUndoRecord, previousRecord, options);
        }

        /// <summary>
        /// Ensures that there is always at least one empty extent.
        /// </summary>
        private void EnsureAvailableSpace()
        {
            if (this.log.LogStore.FreeBytes >= ExtentSize)
            {
                return;
            }

            lock (this.storeLock)
            {
                if (this.log.LogStore.FreeBytes >= ExtentSize)
                {
                    return;
                }

                var path = $"{this.logFileBase}.log.{this.log.LogStore.Extents.Count}";
                this.logger($"About to extend collection via new log extent: '{path}' (len: {path.Length})");
                this.log.LogStore.Extents.Add(path);
            }
        }

        public Task<SequenceNumber> Flush(SequenceNumber upToSequenceNumber)
        {
            return this.log.FlushAsync(upToSequenceNumber);
        }

        /// <summary>
        /// Flushes all records.
        /// </summary>
        /// <returns>The sequence number of the last flushed record.</returns>
        public Task<SequenceNumber> Flush()
        {
            return this.log.FlushAsync(SequenceNumber.Invalid);
        }

        public Transaction StartTransaction()
        {
            return new Transaction(this);
        }

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        public void Dispose()
        {
            this.log?.Dispose();
            this.log = null;
        }
    }
}