namespace DistributedJournalService.Records
{
    using DistributedJournalService.Data;

    using ProtoBuf;

    [ProtoContract]
    public class EpochUpdateLogEntry : LogEntry
    {
        public EpochUpdateLogEntry() {}

        public EpochUpdateLogEntry(ProgressIndicator progress)
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