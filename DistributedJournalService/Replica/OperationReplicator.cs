namespace DistributedJournalService.Replica
{
    using System;
    using System.Fabric;
    using System.Threading;
    using System.Threading.Tasks;
    using System.Threading.Tasks.Dataflow;

    using DistributedJournalService.Utilities;

    /// <summary>
    /// Replicates operations from a primary node to secondary nodes.
    /// </summary>
    internal class OperationReplicator
    {
        /// <summary>
        /// The object which handles ordered replication requests.
        /// </summary>
        private readonly ActionBlock<ReplicationRequest> replicationWorker;

        /// <summary>
        /// The object which handles completion of each ordered replication request.
        /// </summary>
        private readonly ActionBlock<ReplicationRequest> completionWorker;

        /// <summary>
        /// The replicator.
        /// </summary>
        private readonly IStateReplicator replicator;

        private readonly StateProvider stateProvider;

        private readonly Logger logger;

        private bool closing;

        public OperationReplicator(IStateReplicator replicator, StateProvider stateProvider, Logger logger)
        {
            this.replicator = replicator;
            this.stateProvider = stateProvider;
            this.logger = logger;
            var options = new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = 1, BoundedCapacity = 128 };
            this.replicationWorker = new ActionBlock<ReplicationRequest>(this.InitiateOrderedReplication, options);
            this.completionWorker = new ActionBlock<ReplicationRequest>(this.CompleteOrderedReplication, options);
        }

        /// <summary>
        /// Stops this instance from processing requests and completes when all outstanding requests have been processed.
        /// </summary>
        /// <returns>A <see cref="Task"/> representing the work performed.</returns>
        public async Task Close()
        {
            this.closing = true;
            this.logger.Log($"{nameof(OperationReplicator)} {nameof(this.Close)}");
            this.replicationWorker.Complete();
            
            // Don't allow replication issues (eg, because this is no longer a primary) to
            // potentially cause this method to complete before all notifications have been processed.
            await this.replicationWorker.Completion.Suppressed().ConfigureAwait(false);

            // Ensure that all notifications have been executed before returning.
            this.completionWorker.Complete();
            await this.completionWorker.Completion.ConfigureAwait(false);
        }

        /// <summary>
        /// Replicates the provided <paramref name="operation"/>, invoking <paramref name="continuation"/> on completion.
        /// </summary>
        /// <param name="operation">The operatin to be replicated.</param>
        /// <param name="continuation">The continuation which will be called when replication completes.</param>
        /// <param name="cancellationToken">The cancellation which can be used to cancel replication.</param>
        public void Replicate(
            OperationData operation,
            Func<Task<long>, Task> continuation,
            CancellationToken cancellationToken)
        {
            var request = new ReplicationRequest(
                operation,
                new TaskCompletionSource<long>(),
                continuation,
                cancellationToken);
            this.replicationWorker.Post(request);
        }

        /// <summary>
        /// Starts replication of the provided <paramref name="request"/>, ensuring that its completion handler will
        /// be invoked in the order of replication.
        /// </summary>
        /// <param name="request">The request.</param>
        /// <returns>A <see cref="Task"/> representing the work performed.</returns>
        private Task InitiateOrderedReplication(ReplicationRequest request)
        {
            if (this.closing) this.logger.Log(nameof(OperationReplicator) + nameof(this.InitiateOrderedReplication));
            try
            {
                // If the request has already been cancelled, return without initiating replication.
                if (request.Cancellation.IsCancellationRequested)
                {
                    request.Cancel();
                    return Task.FromResult(0);
                }
                
                // To ensure that replication completion handlers are invoked in the order they were
                // replicated in, post them to the completion worker from this replication worker.
                this.completionWorker.Post(request);

                // Start replicating the operation. Do not wait for replication to complete, but instead
                // propagate the result to the completion task so that the completion worker can handle
                // it in its due turn.
                long sequenceNumber;
                this.replicator.ReplicateAsync(request.Operation, request.Cancellation, out sequenceNumber)
                    .PropagateToCompletion(request.ReplicationCompleted);
                if (this.closing) this.logger.Log("Completed " + nameof(OperationReplicator) + nameof(this.InitiateOrderedReplication));
                return Task.FromResult(0);
            }
            catch (Exception exception)
            {
                // Replication failed, notify the requester.
                request.ReplicationCompleted.TrySetException(exception);
                throw;
            }
        }

        /// <summary>
        /// Calls the provided requests completion handler and waits for it to complete.
        /// </summary>
        /// <param name="request">The reqest.</param>
        /// <returns>A <see cref="Task"/> representing the work performed.</returns>
        private async Task CompleteOrderedReplication(ReplicationRequest request)
        {
            if (this.closing) this.logger.Log(nameof(OperationReplicator) + nameof(this.CompleteOrderedReplication));
            try
            {
                var logSequenceNumber = await request.ReplicationCompleted.Task.ConfigureAwait(false);
                await this.stateProvider.AppendOperationData(request.Operation, logSequenceNumber).ConfigureAwait(false);
            }
            catch(Exception exception)
            {
                // If the error was caused by the state provider, propagate that exception.
                if (request.ReplicationCompleted.Task.Status == TaskStatus.RanToCompletion)
                {
                    await request.CompletionHandler(Task.FromException<long>(exception)).ConfigureAwait(false);
                    return;
                }
            }
            
            // Wait for the caller's completion handler to complete before continuing to
            // process other replication completion handlers.
            await request.CompletionHandler(request.ReplicationCompleted.Task).Suppressed().ConfigureAwait(false);
        }

        /// <summary>
        /// Represents a request to have an operation replicated.
        /// </summary>
        private struct ReplicationRequest
        {
            public ReplicationRequest(
                OperationData operation,
                TaskCompletionSource<long> replicationCompleted,
                Func<Task<long>, Task> completionHandler,
                CancellationToken cancellation)
            {
                this.Operation = operation;
                this.Cancellation = cancellation;
                this.ReplicationCompleted = replicationCompleted;
                this.CompletionHandler = completionHandler;
            }

            /// <summary>
            /// Gets the operation to be replicated.
            /// </summary>
            public OperationData Operation { get; }

            /// <summary>
            /// Gets the cancellation which can be used to cancel replication.
            /// </summary>
            public CancellationToken Cancellation { get; }

            /// <summary>
            /// Gets the completion source for the task which is used to signal replication completion.
            /// </summary>
            public TaskCompletionSource<long> ReplicationCompleted { get; }

            /// <summary>
            /// Gets the handler which is called upon completion.
            /// </summary>
            public Func<Task<long>, Task> CompletionHandler { get; }

            /// <summary>
            /// Attempts to cancel replication.
            /// </summary>
            public void Cancel()
            {
                this.ReplicationCompleted.TrySetCanceled();
            }
        }
    }
}