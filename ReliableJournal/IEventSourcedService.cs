namespace ReliableJournal
{
    using System.Fabric;
    using System.Threading;
    using System.Threading.Tasks;

    using Microsoft.ServiceFabric.Services.Communication.Runtime;

    public interface IEventSourcedService<TOperation>
    {
        ICommunicationListener CreateCommunicationListener(
            StatefulServiceInitializationParameters initializationParameters);

        /// <summary>
        /// Applies the provided <paramref name="operation"/>.
        /// </summary>
        /// <param name="operation">The operation.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>A <see cref="Task"/> representing the work performed.</returns>
        Task Apply(TOperation operation, CancellationToken cancellationToken);

        /// <summary>
        /// Opens the service to handle requests.
        /// </summary>
        /// <param name="journal">The journal.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>
        /// The address of this service, which clients can find using service discovery.
        /// </returns>
        Task Open(IReliableJournal<TOperation> journal, CancellationToken cancellationToken);

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