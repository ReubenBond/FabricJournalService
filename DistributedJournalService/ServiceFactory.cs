namespace DistributedJournalService
{
    using System;
    using System.Fabric;

    using DistributedJournalService.Operations;

    using ProtoBuf.Meta;

    using ReliableJournal;
    using ReliableJournal.Replica;

    internal class ServiceFactory : IStatefulServiceFactory
    {
        /// <summary>
        /// Return an instance of a service
        ///             The framework will set the serviceTypeName, serviceName, initializationData, partitionId and instanceId properties on the service
        /// </summary>
        /// <returns/>
        public IStatefulServiceReplica CreateReplica(
            string serviceTypeName,
            Uri serviceName,
            byte[] initializationData,
            Guid partitionId,
            long replicaId)
        {
            return ReliableJournalFactory.Create(new KeyValueStore(), TypeModel.Create());
        }
    }
}