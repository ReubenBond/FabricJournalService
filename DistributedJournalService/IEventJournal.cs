namespace DistributedJournalService
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;

    using DistributedJournalService.Events;

    internal interface IEventJournal
    {
        /// <summary>
        /// Replicates the provided operation and applies it upon completion, maintaining replication order.
        /// </summary>
        /// <param name="event">The operation.</param>
        /// <param name="cancellationToken">
        /// The cancellation token.
        /// </param>
        /// <returns>A <see cref="Task"/> representing the work performed.</returns>
        Task ReplicateAndApply(Event @event, CancellationToken cancellationToken);

        /// <summary>
        /// Replicates the provided operation and executes the provided action upon completion, maintaining replication order.
        /// </summary>
        /// <typeparam name="T">The result of the action.</typeparam>
        /// <param name="event">The operation.</param>
        /// <param name="action">The action.</param>
        /// <param name="cancellationToken">
        /// The cancellation token.
        /// </param>
        /// <returns>The result of invoking the action.</returns>
        Task<T> ReplicateAndApply<T>(Event @event, Func<T> action, CancellationToken cancellationToken);
    }
}