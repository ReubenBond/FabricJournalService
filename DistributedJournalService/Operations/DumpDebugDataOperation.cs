namespace DistributedJournalService.Operations
{
    using ProtoBuf;

    [ProtoContract]
    internal class DumpDebugDataOperation : Operation
    {
        [ProtoMember(1)]
        public string Directory { get; set; }

        [ProtoMember(2)]
        public string Prefix { get; set; }
    }
}