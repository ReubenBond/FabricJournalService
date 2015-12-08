namespace DistributedJournalService.Records
{
    using System.Fabric;

    using ProtoBuf;

    /// <summary>
    /// Represents a version for a
    /// </summary>
    [ProtoContract]
    public class DataVersion
    {
        public DataVersion() { }

        public DataVersion(Epoch epoch, long logSequenceNumber)
        {
            this.EpochDataLossNumber = epoch.DataLossNumber;
            this.EpochConfigurationNumber = epoch.ConfigurationNumber;
            this.LogSequenceNumber = logSequenceNumber;
        }

        /// <summary>
        /// Gets or sets the log sequence number.
        /// </summary>
        [ProtoMember(1)]
        public long LogSequenceNumber { get; private set; }

        /// <summary>
        /// Gets or sets the DataLossNumber component of the epoch.
        /// </summary>
        [ProtoMember(2)]
        public long EpochDataLossNumber { get; private set; }

        /// <summary>
        /// Gets or sets the ConfigurationNumber component of the epoch.
        /// </summary>
        [ProtoMember(3)]
        public long EpochConfigurationNumber { get; private set; }

        /// <summary>
        /// Gets or sets the epoch.
        /// </summary>
        public Epoch Epoch => new Epoch(this.EpochDataLossNumber, this.EpochConfigurationNumber);

        /// <summary>
        /// Returns a string that represents the current object.
        /// </summary>
        /// <returns>
        /// A string that represents the current object.
        /// </returns>
        public override string ToString()
        {
            return $"LogSequenceNumber: {this.LogSequenceNumber}, Epoch: {this.EpochDataLossNumber}.{this.EpochConfigurationNumber}";
        }
    }
}