namespace DistributedJournalService.Operations
{
    using DistributedJournalService.Data;

    using ProtoBuf;

    [ProtoContract]
    internal class ProgressVectorUpdateOperation : Operation
    {
        public ProgressVectorUpdateOperation() { }

        public ProgressVectorUpdateOperation(ProgressIndicator progress)
        {
            this.Progress = progress;
        }

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
            return $"[{nameof(ProgressVectorUpdateOperation)}] Progress: {this.Progress}";
        }
    }
}