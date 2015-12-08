namespace DistributedJournalService.Replica
{
    using System.Threading.Tasks;

    internal interface IReplicationNotifier {
        Task WaitForCommit(long logSequenceNumber);
    }
}