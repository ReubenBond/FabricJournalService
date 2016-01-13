namespace DistributedJournalService.Operations
{
    using ProtoBuf;

    [ProtoContract]
    [ProtoInclude(1, typeof(SetValueOperation))]
    [ProtoInclude(2, typeof(RemoveValueOperation))]
    [ProtoInclude(3, typeof(GetValueOperation))]
    [ProtoInclude(4, typeof(DumpDebugDataOperation))]
    internal abstract class Operation
    {
        protected Operation() { }
    }
}