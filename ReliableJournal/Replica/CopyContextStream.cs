namespace ReliableJournal.Replica
{
    using System.Collections.Generic;
    using System.Fabric;
    using System.Threading;
    using System.Threading.Tasks;

    using ReliableJournal.Replica.Progress;
    using ReliableJournal.Serialization;
    using ReliableJournal.Utilities;

    internal class CopyContextStream : IOperationDataStream
    {
        private readonly Logger logger;

        private readonly Serializer serializer;

        private readonly IEnumerator<ProgressIndicator> progressVector;
        
        public CopyContextStream(ProgressVector progressVector, Logger logger, Serializer serializer)
        {
            this.logger = logger;
            this.serializer = serializer;
            this.progressVector = progressVector.GetEnumerator();
        }

        public Task<OperationData> GetNextAsync(CancellationToken cancellationToken)
        {
            OperationData result = null;
            while (this.progressVector.MoveNext())
            {
                this.logger.Log($"CopyContext.GetNextAsync returning record {this.progressVector.Current}");

                // Copy the stream into the result.
                var progressIndicator = this.serializer.Serialize(this.progressVector.Current);
                if (result == null)
                {
                    result = new OperationData(progressIndicator);
                }
                else
                {
                    result.Add(progressIndicator);
                }
            }

            if (result == null)
            {
                this.logger.Log("CopyContext.GetNextAsync signalling completion");
            }

            return Task.FromResult(result);
        }
    }
}