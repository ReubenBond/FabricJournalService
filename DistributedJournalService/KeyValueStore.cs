using System.Collections.Generic;
using System.Threading.Tasks;

namespace DistributedJournalService
{
    using System;
    using System.Diagnostics;
    using System.Fabric;
    using System.IO;
    using System.Linq;
    using System.Security.Cryptography;
    using System.ServiceModel;
    using System.Text;
    using System.Threading;

    using DistributedJournalService.Interfaces;
    using DistributedJournalService.Operations;
    using DistributedJournalService.Utilities;

    using Microsoft.ServiceFabric.Services.Communication.Runtime;
    using Microsoft.ServiceFabric.Services.Communication.Wcf.Runtime;

    using ReliableJournal;

    [ServiceBehavior(ConcurrencyMode = ConcurrencyMode.Multiple, UseSynchronizationContext = false, InstanceContextMode = InstanceContextMode.Single, IncludeExceptionDetailInFaults = true)]
    internal class KeyValueStore : IEventSourcedService<Operation>, IKeyValueStore
    {
        private readonly Dictionary<string, byte[]> store = new Dictionary<string, byte[]>();

        private IReliableJournal<Operation> journal;

        private long replicaId;

        private Guid partitionId;

        /// <summary>
        /// Creates and returns a communication listener.
        /// </summary>
        /// <param name="initializationParameters">
        /// The service initialization parameters.
        /// </param>
        /// <returns>
        /// A new <see cref="ICommunicationListener"/>.
        /// </returns>
        public ICommunicationListener CreateCommunicationListener(StatefulServiceInitializationParameters initializationParameters)
        {
            this.replicaId = initializationParameters.ReplicaId;
            this.partitionId = initializationParameters.PartitionId;
            return new WcfCommunicationListener(initializationParameters, typeof(IKeyValueStore), this)
            {
                Binding = ServiceBindings.TcpBinding,
                EndpointResourceName = "ServiceEndpoint"
            };
        }

        /// <summary>
        /// Applies the provided <paramref name="operation"/>.
        /// </summary>
        /// <param name="operation">The event.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>A <see cref="Task"/> representing the work performed.</returns>
        public Task Apply(Operation operation, CancellationToken cancellationToken)
        {
#warning TODO: use some better dispatching system here.
            var set = operation as SetValueOperation;
            if (set != null)
            {
                this.store[set.Key] = set.Value;
            }

            var remove = operation as RemoveValueOperation;
            if (remove != null)
            {
                this.store.Remove(remove.Key);
            }

            var get = operation as GetValueOperation;
            if (get != null)
            {
                // do nothing.
            }

            var debug = operation as DumpDebugDataOperation;
            if (debug != null)
            {
                return this.ApplyDumpDebugData(debug.Directory, debug.Prefix);
            }

            return Task.FromResult(0);
        }

        public async Task ApplyDumpDebugData(string directory, string prefix)
        {
            var outputPath = Path.Combine(
                directory,
                $"{prefix}_{this.partitionId.ToString("N")}_{this.replicaId.ToString("X")}.txt");
            using (var file = File.OpenWrite(outputPath))
            using (var writer = new StreamWriter(file, Encoding.UTF8))
                using (var mem = new MemoryStream())
            using (var sha = new SHA512Managed())
            {
                sha.Initialize();
                foreach (var entry in this.store)
                {
                    var keyBytes = Encoding.UTF8.GetBytes(entry.Key);
                    mem.Write(keyBytes, 0, keyBytes.Length);
                    mem.Write(entry.Value, 0, entry.Value.Length);

                    await writer.WriteLineAsync($"{entry.Key} = {entry.Value.ToHexString()}");
                }

                mem.Seek(0, SeekOrigin.Begin);
                var hash = sha.ComputeHash(mem);
                var hashString = $"HASH: {hash.ToHexString()}";
                await writer.WriteLineAsync(hashString);
                Debug.WriteLine(hashString);
                ServiceEventSource.Current.Message(hashString);
            }
        }

        /// <summary>
        /// Opens the service to handle requests.
        /// </summary>
        /// <param name="journal">The journal.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>
        /// The address of this service, which clients can find using service discovery.
        /// </returns>
        public Task Open(IReliableJournal<Operation> journal, CancellationToken cancellationToken)
        {
            this.journal = journal;
            
            Debug.WriteLine($"[{nameof(KeyValueStore)}] Open");

            return Task.FromResult(string.Empty);
        }

        /// <summary>
        /// Closes this service.
        /// </summary>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>A <see cref="Task"/> representing the work performed.</returns>
        /// <remarks>The service may be re-opened at a later time.</remarks>
        public Task Close(CancellationToken cancellationToken)
        {
            Debug.WriteLine($"[{nameof(KeyValueStore)}] Close");
            this.journal = null;
            return Task.FromResult(0);
        }

        /// <summary>
        /// Resets the state of this service.
        /// </summary>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>A <see cref="Task"/> representing the work performed.</returns>
        public Task Reset(CancellationToken cancellationToken)
        {
            Debug.WriteLine($"[{nameof(KeyValueStore)}] Reset");
            this.store.Clear();
            return Task.FromResult(0);
        }

        public async Task<byte[]> Get(string key)
        {
            return await this.journal.Commit(new GetValueOperation(key), () => this.store[key], CancellationToken.None).ConfigureAwait(false);
        }

        public Task Set(string key, byte[] value)
        {
            return this.journal.Commit(new SetValueOperation(key, value), CancellationToken.None);
        }

        public Task Remove(string key)
        {
            return this.journal.Commit(new RemoveValueOperation(key), CancellationToken.None);
        }

        public Task<List<string>> GetKeys()
        {
            return this.journal.Commit(
                new GetValueOperation(null),
                () => this.store.Keys.ToList(),
                CancellationToken.None);
        }

        public Task DumpDebugData(string directory, string prefix)
        {
            return this.journal.Commit(
                new DumpDebugDataOperation { Directory = directory, Prefix = prefix },
                CancellationToken.None);
        }
    }
}
