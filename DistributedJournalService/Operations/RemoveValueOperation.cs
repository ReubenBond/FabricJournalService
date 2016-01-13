namespace DistributedJournalService.Operations
{
    using ProtoBuf;

    [ProtoContract]
    internal class RemoveValueOperation : Operation
    {
        public RemoveValueOperation() { }
        public RemoveValueOperation(string key)
        {
            this.Key = key;
        }

        [ProtoMember(1)]
        public string Key { get; private set; }

        /// <summary>
        /// Returns a string that represents the current object.
        /// </summary>
        /// <returns>
        /// A string that represents the current object.
        /// </returns>
        public override string ToString()
        {
            return $"Remove {this.Key}";
        }
    }
}