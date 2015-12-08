namespace DistributedJournalService.Events
{
    using ProtoBuf;

    [ProtoInclude(1, typeof(SetValueEvent))]
    [ProtoInclude(2, typeof(RemoveValueEvent))]
    [ProtoInclude(3, typeof(GetValueEvent))]
    [ProtoContract]
    internal abstract class Event
    {
        protected Event() { }
    }
}