namespace DistributedJournalService.Records
{
    using DistributedJournalService.Operations;

    using ProtoBuf;

    /// <summary>
    /// Represents a committed operation.
    /// </summary>
    [ProtoContract]
    internal class OperationLogEntry : LogEntry
    {
        public OperationLogEntry() { }

        public OperationLogEntry(Operation operation, DataVersion version)
        {
            this.Operation = operation;
            this.DataVersion = version;
        }

        /// <summary>
        /// Gets or sets the operation.
        /// </summary>
        [ProtoMember(1)]
        public Operation Operation { get; private set; }

        /// <summary>
        /// Gets or sets the epoch and log sequence number of this entry.
        /// </summary>
        [ProtoMember(2)]
        public DataVersion DataVersion { get; private set; }

        /// <summary>
        /// Returns a string that represents the current object.
        /// </summary>
        /// <returns>
        /// A string that represents the current object.
        /// </returns>
        public override string ToString()
        {
            return $"Operation: {this.Operation}, DataVersion: {this.DataVersion}";
        }
    }
}