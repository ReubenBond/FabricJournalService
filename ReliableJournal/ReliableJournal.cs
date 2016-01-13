namespace ReliableJournal
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;

    using ReliableJournal.Replica;
    using ReliableJournal.Utilities;

    internal class ReliableJournal<TOperation> : IReliableJournal<TOperation>
    {
        private readonly OperationReplicator<TOperation> replicator;

        private readonly IEventSourcedService<TOperation> applier;
       
        public ReliableJournal(OperationReplicator<TOperation> replicator, IEventSourcedService<TOperation> applier)
        {
            this.replicator = replicator;
            this.applier = applier;
        }

        /// <summary>
        /// Replicates the provided operation and applies it upon completion, maintaining replication order.
        /// </summary>
        /// <param name="operation">The event.</param>
        /// <param name="cancellationToken">
        /// The cancellation token.
        /// </param>
        /// <returns>A <see cref="Task"/> representing the work performed.</returns>
        public Task Commit(TOperation operation, CancellationToken cancellationToken)
        {
            var completion = new TaskCompletionSource<int>();
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
                        await this.applier.Apply(operation, cancellationToken).ConfigureAwait(false);
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
        /// <typeparam name="TResult">The result of the action.</typeparam>
        /// <param name="operation">The event.</param>
        /// <param name="action">The action.</param>
        /// <param name="cancellationToken">
        /// The cancellation token.
        /// </param>
        /// <returns>The result of invoking the action.</returns>
        public Task<TResult> Commit<TResult>(TOperation operation, Func<TResult> action, CancellationToken cancellationToken)
        {
            var completion = new TaskCompletionSource<TResult>();
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
                        await this.applier.Apply(operation, cancellationToken).ConfigureAwait(false);
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