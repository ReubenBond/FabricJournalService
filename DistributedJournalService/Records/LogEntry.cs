namespace DistributedJournalService.Records
{
    using ProtoBuf;

    [ProtoContract]
    [ProtoInclude(1, typeof(OperationLogEntry))]
    [ProtoInclude(2, typeof(EpochUpdateLogEntry))]
    public class LogEntry
    {
        protected LogEntry() { }
    }
}