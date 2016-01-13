namespace ReliableJournal.Records
{
    using System.Fabric;

    using ProtoBuf;

    using ReliableJournal.Utilities;

    /// <summary>
    /// Represents a version.
    /// </summary>
    [ProtoContract]
    public struct RecordVersion
    {
        public RecordVersion(Epoch epoch, long logSequenceNumber)
        {
            this.Epoch = epoch;
            this.LogSequenceNumber = logSequenceNumber;
        }

        /// <summary>
        /// Gets the log sequence number.
        /// </summary>
        [ProtoMember(1)]
        public long LogSequenceNumber { get; private set; }

        /// <summary>
        /// Gets the epoch.
        /// </summary>
        [ProtoMember(2)]
        public Epoch Epoch { get; private set; }

        /// <summary>
        /// Returns a string that represents the current object.
        /// </summary>
        /// <returns>
        /// A string that represents the current object.
        /// </returns>
        public override string ToString()
        {
            return $"{nameof(RecordVersion)}: {this.LogSequenceNumber}, Epoch: {this.Epoch.ToDisplayString()}";
        }
    }
}