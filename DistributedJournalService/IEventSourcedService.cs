namespace DistributedJournalService
{
    using System.Fabric;
    using System.Threading;
    using System.Threading.Tasks;

    using DistributedJournalService.Events;

    using Microsoft.ServiceFabric.Services.Communication.Runtime;

    internal interface IEventSourcedService
    {
        ICommunicationListener CreateCommunicationListener(
            StatefulServiceInitializationParameters initializationParameters);

        /// <summary>
        /// Applies the provided <paramref name="event"/>.
        /// </summary>
        /// <param name="event">The event.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>A <see cref="Task"/> representing the work performed.</returns>
        Task Apply(Event @event, CancellationToken cancellationToken);

        /// <summary>
        /// Opens the service to handle requests.
        /// </summary>
        /// <param name="journal">The journal.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>
        /// The address of this service, which clients can find using service discovery.
        /// </returns>
        Task Open(IEventJournal journal, CancellationToken cancellationToken);

        /// <summary>
        /// Closes this service.
        /// </summary>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>A <see cref="Task"/> representing the work performed.</returns>
        /// <remarks>The service may be re-opened at a later time.</remarks>
        Task Close(CancellationToken cancellationToken);

        /// <summary>
        /// Resets the state of this service.
        /// </summary>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>A <see cref="Task"/> representing the work performed.</returns>
        Task Reset(CancellationToken cancellationToken);
    }
}