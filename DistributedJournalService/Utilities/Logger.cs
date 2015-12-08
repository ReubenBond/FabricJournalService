namespace DistributedJournalService.Utilities
{
    using System;
    using System.Fabric;

    public class Logger
    {
        public StatefulServiceInitializationParameters InitializationParameters { get; }

        public Logger(StatefulServiceInitializationParameters initializationParameters)
        {
            this.InitializationParameters = initializationParameters;
        }

        public Func<string> Prefix { get; set; }
    }
}