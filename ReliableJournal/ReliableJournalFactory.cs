namespace ReliableJournal
{
    using System.Fabric;

    using ProtoBuf.Meta;

    using ReliableJournal.Replica;

    public static class ReliableJournalFactory
    {
        /// <summary>
        /// Creates and returns a new reliable journal 
        /// </summary>
        /// <typeparam name="TOperation"></typeparam>
        /// <param name="service"></param>
        /// <param name="serializer"></param>
        /// <returns></returns>
        public static IStatefulServiceReplica Create<TOperation>(IEventSourcedService<TOperation> service, RuntimeTypeModel serializer = null)
        {
            return new JournalReplica<TOperation>(service, serializer);
        }
    }
}
