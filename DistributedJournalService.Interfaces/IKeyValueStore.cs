namespace DistributedJournalService.Interfaces
{
    using System.ServiceModel;
    using System.Collections.Generic;
    using System.Threading.Tasks;

    [ServiceContract(Name="IKeyValueStore", Namespace = "http://fabbers")]
    public interface IKeyValueStore
    {
        [OperationContract]
        Task<byte[]> Get(string key);

        [OperationContract]
        Task Set(string key, byte[] value);

        [OperationContract]
        Task Remove(string key);

        [OperationContract]
        Task<List<string>> GetKeys();
    }
}