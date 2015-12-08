namespace DistributedJournalService
{
    using System;
    using System.Fabric;
    using System.Threading;
    using System.Threading.Tasks;

    using DistributedJournalService.Events;
    using DistributedJournalService.Operations;
    using DistributedJournalService.Replica;
    using DistributedJournalService.Utilities;

    internal class EventJournal : IEventJournal
    {
        private readonly OperationReplicator replicator;

        private readonly IEventSourcedService applier;
       
        public EventJournal(OperationReplicator replicator, IEventSourcedService applier)
        {
            this.replicator = replicator;
            this.applier = applier;
        }

        /// <summary>
        /// Replicates the provided operation and applies it upon completion, maintaining replication order.
        /// </summary>
        /// <param name="event">The event.</param>
        /// <param name="cancellationToken">
        /// The cancellation token.
        /// </param>
        /// <returns>A <see cref="Task"/> representing the work performed.</returns>
        public Task ReplicateAndApply(Event @event, CancellationToken cancellationToken)
        {
            var completion = new TaskCompletionSource<int>();
            var operation = new OperationData(new AppendEventOperation(@event).Serialize());
            this.replicator.Replicate(
                operation,
                async lsn =>
                {
                    if (lsn.Status != TaskStatus.RanToCompletion)
                    {
                        lsn.PropagateToCompletion(completion);
                    }

                    try
                    {
                        await this.applier.Apply(@event, cancellationToken).ConfigureAwait(false);
                        completion.TrySetResult(0);
                    }
                    catch (Exception exception)
                    {
                        completion.TrySetException(exception);
                    }
                },
                cancellationToken);
            return completion.Task;
        }

        /// <summary>
        /// Replicates the provided operation and executes the provided action upon completion, maintaining replication order.
        /// </summary>
        /// <typeparam name="T">The result of the action.</typeparam>
        /// <param name="event">The event.</param>
        /// <param name="action">The action.</param>
        /// <param name="cancellationToken">
        /// The cancellation token.
        /// </param>
        /// <returns>The result of invoking the action.</returns>
        public Task<T> ReplicateAndApply<T>(Event @event, Func<T> action, CancellationToken cancellationToken)
        {
            var completion = new TaskCompletionSource<T>();
            var operation = new OperationData(new AppendEventOperation(@event).Serialize());
            this.replicator.Replicate(
                operation,
                async lsn =>
                {
                    if (lsn.Status != TaskStatus.RanToCompletion)
                    {
                        lsn.PropagateToCompletion(completion);
                    }

                    try
                    {
                        await this.applier.Apply(@event, cancellationToken).ConfigureAwait(false);
                        completion.TrySetResult(action());
                    }
                    catch (Exception exception)
                    {
                        completion.TrySetException(exception);
                    }
                },
                cancellationToken);
            return completion.Task;
        }
    }
}