namespace ReliableJournal.Utilities
{
    using System.Diagnostics;
    using System.Fabric;

    internal static class LoggerExtensions
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

    internal static class EpochExtensions
    {
        public static string ToDisplayString(this Epoch epoch)
        {
            return $"{epoch.DataLossNumber}.{epoch.ConfigurationNumber}";
        }
    }
}