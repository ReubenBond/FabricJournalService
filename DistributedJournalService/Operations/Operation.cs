using System;

namespace DistributedJournalService.Operations
{
    using ProtoBuf;

    [ProtoContract]
    [ProtoInclude(1, typeof(AppendEventOperation))]
    [ProtoInclude(2, typeof(ProgressVectorUpdateOperation))]
    [Serializable]
    internal class Operation
    {
        protected Operation() { }
    }
}
