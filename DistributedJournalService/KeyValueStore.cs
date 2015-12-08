using System.Collections.Generic;
using System.Threading.Tasks;

namespace DistributedJournalService
{
    using System.Diagnostics;
    using System.Fabric;
    using System.Linq;
    using System.ServiceModel;
    using System.Threading;

    using DistributedJournalService.Events;
    using DistributedJournalService.Interfaces;

    using Microsoft.ServiceFabric.Services.Communication.Runtime;
    using Microsoft.ServiceFabric.Services.Communication.Wcf.Runtime;

    [ServiceBehavior(ConcurrencyMode = ConcurrencyMode.Multiple, UseSynchronizationContext = false, InstanceContextMode = InstanceContextMode.Single, IncludeExceptionDetailInFaults = true)]
    internal class KeyValueStore : IEventSourcedService, IKeyValueStore
    {
        private readonly Dictionary<string, byte[]> store = new Dictionary<string, byte[]>();

        private IEventJournal journal;
        
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
            return new WcfCommunicationListener(initializationParameters, typeof(IKeyValueStore), this)
            {
                Binding = ServiceBindings.TcpBinding,
                EndpointResourceName = "ServiceEndpoint"
            };
        }

        /// <summary>
        /// Applies the provided <paramref name="event"/>.
        /// </summary>
        /// <param name="event">The event.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>A <see cref="Task"/> representing the work performed.</returns>
        public Task Apply(Event @event, CancellationToken cancellationToken)
        {
            // TODO: use some better dispatching system here.
            var set = @event as SetValueEvent;
            if (set != null)
            {
                this.store[set.Key] = set.Value;
            }

            var remove = @event as RemoveValueEvent;
            if (remove != null)
            {
                this.store.Remove(remove.Key);
            }

            var get = @event as GetValueEvent;
            if (get != null)
            {
                // do nothing.
            }

            return Task.FromResult(0);
        }

        /// <summary>
        /// Opens the service to handle requests.
        /// </summary>
        /// <param name="journal">The journal.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>
        /// The address of this service, which clients can find using service discovery.
        /// </returns>
        public Task Open(IEventJournal journal, CancellationToken cancellationToken)
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
            return await this.journal.ReplicateAndApply(new GetValueEvent(key), () => this.store[key], CancellationToken.None).ConfigureAwait(false);
        }

        public Task Set(string key, byte[] value)
        {
            return this.journal.ReplicateAndApply(new SetValueEvent(key, value), CancellationToken.None);
        }

        public Task Remove(string key)
        {
            return this.journal.ReplicateAndApply(new RemoveValueEvent(key), CancellationToken.None);
        }

        public Task<List<string>> GetKeys()
        {
            return this.journal.ReplicateAndApply(
                new GetValueEvent(null),
                () => this.store.Keys.ToList(),
                CancellationToken.None);
        }
    }
}
