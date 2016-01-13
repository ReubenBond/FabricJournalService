namespace DistributedJournalService.Operations
{
    using System.Linq;

    using ProtoBuf;

    [ProtoContract]
    internal class SetValueOperation : Operation
    {
        public SetValueOperation() { }

        public SetValueOperation(string key, byte[] value)
        {
            this.Key = key;
            this.Value = value;
        }

        [ProtoMember(1)]
        public string Key { get; private set; }

        [ProtoMember(2)]
        public byte[] Value { get; private set; }

        /// <summary>
        /// Returns a string that represents the current object.
        /// </summary>
        /// <returns>
        /// A string that represents the current object.
        /// </returns>
        public override string ToString()
        {
            return $"Set {this.Key} = {string.Concat(this.Value?.Select(_ => _.ToString("X")) ?? new[] { "null" })}";
        }
    }
}