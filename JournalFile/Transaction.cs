namespace JournalFile
{
    using System;
    using System.IO.Log;
    using System.Threading.Tasks;

    using JournalFile.Exceptions;

    internal struct TransactionRecord
    {
        public TransactionOperationKind State { get; set; }
    }

    internal enum TransactionOperationKind : byte
    {
        AppendRecord,

        Commit,

        Abort
    }

    internal enum TransactionState
    {
        Open,

        Committed,

        Aborted
    }

    public class Transaction : IDisposable
    {
        private static readonly ArraySegment<byte> AppendRecord =
            new ArraySegment<byte>(new[] { (byte)TransactionOperationKind.AppendRecord });

        private static readonly ArraySegment<byte> CommitTransaction =
            new ArraySegment<byte>(new[] { (byte)TransactionOperationKind.Commit });

        private static readonly ArraySegment<byte> AbortTransaction =
            new ArraySegment<byte>(new[] { (byte)TransactionOperationKind.Abort });

        private readonly TransactionLog log;

        private SequenceNumber firstRecord = SequenceNumber.Invalid;

        private SequenceNumber previousRecord = SequenceNumber.Invalid;

        private TransactionState state;

        public Transaction(TransactionLog log)
        {
            this.log = log;
            this.state = TransactionState.Open;
        }

        public string TransactionId => this.firstRecord.GetHashCode().ToString("X");

        public async Task<SequenceNumber> Append(ArraySegment<byte> data)
        {
            this.ThrowIfTransactionClosed();
            var record = new[] { AppendRecord, data };
            var prev = this.previousRecord;
            var sequence = await this.log.Append(record, prev, prev, RecordAppendOptions.None);

            if (this.firstRecord == SequenceNumber.Invalid)
            {
                this.firstRecord = sequence;
            }

            return this.previousRecord = sequence;
        }

        public async Task<SequenceNumber> CommitAsync()
        {
            this.ThrowIfTransactionClosed();
            var sequence =
                await
                this.log.Append(
                    CommitTransaction,
                    this.previousRecord,
                    this.previousRecord,
                    RecordAppendOptions.ForceFlush);
            this.state = TransactionState.Committed;
            return sequence;
        }

        public async Task<SequenceNumber> AbortAsync()
        {
            this.ThrowIfTransactionClosed();
            var sequence =
                await
                this.log.Append(
                    AbortTransaction,
                    this.previousRecord,
                    this.previousRecord,
                    RecordAppendOptions.ForceFlush);
            this.state = TransactionState.Aborted;
            return sequence;
        }

        private void ThrowIfTransactionClosed()
        {
            if (this.state == TransactionState.Committed)
            {
                var msg =
                    $"Transaction {this.TransactionId} has been committed and no further operations are permitted.";
                throw new TransactionAlreadyCommittedException(msg);
            }

            if (this.state == TransactionState.Aborted)
            {
                var msg = $"Transaction {this.TransactionId} has been aborted and no further operations are permitted.";
                throw new TransactionAlreadyAbortedException(msg);
            }
        }

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        public void Dispose()
        {
            if (this.state == TransactionState.Open)
            {
                var msg = $"Transaction {this.TransactionId} was disposed without being committed or aborted.";
                throw new TransactionIncompleteException(msg);
            }
        }
    }
}