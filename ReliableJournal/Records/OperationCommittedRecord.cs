namespace ReliableJournal.Records
{
    using ProtoBuf;

    [ProtoContract]
    internal class OperationCommittedRecord : Record
    {
        protected OperationCommittedRecord() { }

        protected OperationCommittedRecord(RecordVersion version)
        {
            this.Version = version;
        }

        /// <summary>
        /// Gets or sets the epoch and log sequence number of this record.
        /// </summary>
        [ProtoMember(1)]
        public RecordVersion Version { get; private set; }

        /// <summary>
        /// Returns a string that represents the current object.
        /// </summary>
        /// <returns>
        /// A string that represents the current object.
        /// </returns>
        public override string ToString()
        {
            return $"Version: {this.Version}";
        }
    }

    /// <summary>
    /// Represents a committed operation.
    /// </summary>
    [ProtoContract]
    internal class OperationCommittedRecord<TOperation> : OperationCommittedRecord
    {
        public OperationCommittedRecord() { }

        public OperationCommittedRecord(TOperation operation, RecordVersion version)
            : base(version)
        {
            this.Operation = operation;
        }

        /// <summary>
        /// Gets or sets the operation.
        /// </summary>
        [ProtoMember(1)]
        public TOperation Operation { get; private set; }

        /// <summary>
        /// Returns a string that represents the current object.
        /// </summary>
        /// <returns>
        /// A string that represents the current object.
        /// </returns>
        public override string ToString()
        {
            return $"Operation: {this.Operation}, Version: {this.Version}";
        }
    }
}