namespace DistributedJournalService.Utilities
{
    using Microsoft.IO;

    internal static class MemoryStreamManager
    {
        public static readonly RecyclableMemoryStreamManager Instance = new RecyclableMemoryStreamManager();
    }
}
