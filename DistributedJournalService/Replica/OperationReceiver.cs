namespace DistributedJournalService.Replica
{
    using System;
    using System.Fabric;
    using System.Threading;
    using System.Threading.Tasks;

    using DistributedJournalService.Utilities;

    /// <summary>
    /// Pulls operations from the replicator, applying each using the provided appliers before acknowledging them.
    /// </summary>
    internal class OperationReceiver
    {
        /// <summary>
        /// Applies the provided <paramref name="operation"/>
        /// </summary>
        /// <param name="operation"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        private delegate Task OperationApplier(IOperation operation, CancellationToken cancellationToken);

        /// <summary>
        /// The task which will complete when copying has completed and replication has been initiated.
        /// </summary>
        private readonly TaskCompletionSource<int> replicationInitiated = new TaskCompletionSource<int>();

        /// <summary>
        /// The task which will complete when copying and replication have completed.
        /// </summary>
        private readonly TaskCompletionSource<int> completed = new TaskCompletionSource<int>();

        /// <summary>
        /// The replicator which provides the copy and replication streams.
        /// </summary>
        private readonly IStateReplicator replicator;

        /// <summary>
        /// The target for operations.
        /// </summary>
        private readonly IOperationApplier applier;

        /// <summary>
        /// The logger.
        /// </summary>
        private readonly Logger logger;

        /// <summary>
        /// Whether or not this pump has started.
        /// </summary>
        private int started;

        public OperationReceiver(IOperationApplier applier, IStateReplicator replicator, Logger logger)
        {
            this.applier = applier;
            this.replicator = replicator;
            this.logger = logger;
        }

        /// <summary>
        /// Gets the <see cref="Task"/> which comlpetes when the copy stream has been completely drained and all operations
        /// applied, and that the replication stream has been retrieved.
        /// </summary>
        public Task ReplicationInitiated => this.replicationInitiated.Task;

        /// <summary>
        /// Gets <see cref="Task"/> which comlpetes when the copy and replications streams have been completely drained and all
        /// operations applied.
        /// </summary>
        public Task Completed => Task.WhenAll(this.ReplicationInitiated, this.completed.Task);

        /// <summary>
        /// Begins pulling and applying operations.
        /// </summary>
        /// <param name="cancellationToken">The cancellation token</param>
        /// <exception cref="InvalidOperationException">
        /// The operation pump has already started.
        /// </exception>
        public void Start(CancellationToken cancellationToken)
        {
            // Each instance of this class can only be used once.
            if (Interlocked.CompareExchange(ref this.started, 1, 0) != 0)
            {
                throw new InvalidOperationException($"{nameof(OperationReceiver)} has already been started.");
            }

            // Begin the process and propagate the result to the completion task.
            this.PullOperations(cancellationToken).PropagateToCompletion(this.completed);
        }

        /// <summary>
        /// Begins pulling and applying operations, first from the copy stream, then from the replication stream.
        /// </summary>
        /// <param name="cancellationToken">The cancellation token</param>
        /// <returns>A <see cref="Task"/> representing the work performed.</returns>
        private async Task PullOperations(CancellationToken cancellationToken)
        {
            try
            {
                // Drain the copy queue.
                await
                    PumpOperations(this.replicator.GetCopyStream(), this.applier.ApplyCopyOperation, cancellationToken)
                        .ConfigureAwait(false);

                // Drain the replication queue.
                await
                    PumpOperations(
                        this.replicator.GetReplicationStream(),
                        this.applier.ApplyReplicationOperation,
                        cancellationToken,
                        this.replicationInitiated).ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                this.logger.Log("Exception during replication " + exception);
                this.replicationInitiated.TrySetException(exception);
                throw;
            }
        }

        /// <summary>
        /// Retrieves, applies, and acknowledges each operation from the provided <paramref name="queue"/>.
        /// </summary>
        /// <param name="queue">The queue.</param>
        /// <param name="apply">The method used to apply each operation.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <param name="initiated">
        /// Optional completion to signify that the queue draining has begun.
        /// </param>
        /// <returns>A <see cref="Task"/> representing the work performed.</returns>
        private static async Task PumpOperations(
            IOperationStream queue,
            OperationApplier apply,
            CancellationToken cancellationToken,
            TaskCompletionSource<int> initiated = null)
        {
            var firstOperation = true;
            do
            {
                cancellationToken.ThrowIfCancellationRequested();

                // Start retrieving the next operation.
                var nextOperation = queue.GetOperationAsync(cancellationToken);

                // If this is the first operation and the caller has requested to be notified that draining has begun,
                // notify the caller.
                if (firstOperation)
                {
                    initiated?.TrySetResult(0);
                    firstOperation = false;
                }

                // Wait for the operation to be retrieved.
                var operation = await nextOperation.ConfigureAwait(false);

                // A null operation signifies that the queue has been completely drained.
                if (operation == null)
                {
                    return;
                }
                
                // Apply and acknowledge the operation.
                await apply(operation, cancellationToken).ConfigureAwait(false);
                operation.Acknowledge();
            }
            while (true);
        }
    }
}