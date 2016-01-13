namespace ReliableJournal.Records
{
    using ProtoBuf;

    using ReliableJournal.Replica.Progress;

    [ProtoContract]
    internal class EpochUpdatedRecord : Record
    {
        public EpochUpdatedRecord() {}

        public EpochUpdatedRecord(ProgressIndicator progress)
        {
            this.Progress = progress;
        }

        /// <summary>
        /// Gets or sets the progress indicator representing this epoch update.
        /// </summary>
        [ProtoMember(1)]
        public ProgressIndicator Progress { get; private set; }

        /// <summary>
        /// Returns a string that represents the current object.
        /// </summary>
        /// <returns>
        /// A string that represents the current object.
        /// </returns>
        public override string ToString()
        {
            return $"Progress: {this.Progress}";
        }
    }
}