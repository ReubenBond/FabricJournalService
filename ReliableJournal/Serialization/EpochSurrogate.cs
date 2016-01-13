namespace ReliableJournal.Serialization
{
    using System.Fabric;

    using ProtoBuf;

    [ProtoContract]
    internal struct EpochSurrogate
    {
        /// <summary>
        /// Gets or sets the DataLossNumber component of the epoch.
        /// </summary>
        [ProtoMember(1)]
        private long DataLossNumber { get; set; }

        /// <summary>
        /// Gets or sets the ConfigurationNumber component of the epoch.
        /// </summary>
        [ProtoMember(2)]
        private long ConfigurationNumber { get; set; }

        public static implicit operator EpochSurrogate(Epoch epoch)
        {
            return new EpochSurrogate
            {
                DataLossNumber = epoch.DataLossNumber,
                ConfigurationNumber = epoch.ConfigurationNumber
            };
        }

        public static implicit operator Epoch(EpochSurrogate epoch)
        {
            return new Epoch(epoch.DataLossNumber, epoch.ConfigurationNumber);
        }
    }
}