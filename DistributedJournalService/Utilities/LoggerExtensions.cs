namespace DistributedJournalService.Utilities
{
    using System.Diagnostics;

    public static class LoggerExtensions
    {
        public static void Log(this Logger logger, string message)
        {
            if (logger == null)
            {
                return;
            }

            var msg = logger.Prefix() + message;
            ServiceEventSource.Current.ServiceMessage(logger.InitializationParameters, msg);
            Debug.WriteLine(msg);
        }
    }
}