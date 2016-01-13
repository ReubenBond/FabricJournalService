namespace ReliableJournal.Records
{
    using ProtoBuf;

    [ProtoContract]
    [ProtoInclude(1, typeof(OperationCommittedRecord))]
    [ProtoInclude(2, typeof(EpochUpdatedRecord))]
    internal abstract class Record
    {
        protected Record() { }
    }
}