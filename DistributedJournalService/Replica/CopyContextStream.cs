namespace DistributedJournalService.Replica
{
    using System;
    using System.Collections.Generic;
    using System.Fabric;
    using System.Threading;
    using System.Threading.Tasks;

    using DistributedJournalService.Data;
    using DistributedJournalService.Utilities;

    using ProtoBuf;

    internal class CopyContextStream : IOperationDataStream
    {
        private readonly Logger logger;

        private readonly IEnumerator<ProgressIndicator> progressVector;
        
        public CopyContextStream(ProgressVector progressVector, Logger logger)
        {
            this.logger = logger;
            this.progressVector = progressVector.GetEnumerator();
        }

        public Task<OperationData> GetNextAsync(CancellationToken cancellationToken)
        {
            OperationData result = null;
            using (var stream = MemoryStreamManager.Instance.GetStream("CopyContext.GetNextAsync"))
            {
                while (this.progressVector.MoveNext())
                {
                    // Copy the record to the stream.
                    Serializer.Serialize(stream, this.progressVector.Current);
                    var data = stream.ToArray();
                    stream.Position = 0;
                    stream.SetLength(0);

                    this.logger.Log($"CopyContext.GetNextAsync returning record {this.progressVector.Current}");

                    // Copy the stream into the result.
                    if (result == null)
                    {
                        result = new OperationData(data);
                    }
                    else
                    {
                        result.Add(new ArraySegment<byte>(data));
                    }
                }
            }

            if (result == null)
            {
                this.logger.Log($"CopyContext.GetNextAsync signalling completion");
            }

            return Task.FromResult(result);
        }
    }
}