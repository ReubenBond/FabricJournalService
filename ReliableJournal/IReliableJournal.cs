namespace ReliableJournal
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;

    public interface IReliableJournal<in TOperation>
    {
        /// <summary>
        /// Replicates the provided operation and applies it upon completion, maintaining replication order.
        /// </summary>
        /// <param name="operation">The operation.</param>
        /// <param name="cancellationToken">
        /// The cancellation token.
        /// </param>
        /// <returns>A <see cref="Task"/> representing the work performed.</returns>
        Task Commit(TOperation operation, CancellationToken cancellationToken);

        /// <summary>
        /// Replicates the provided operation and executes the provided action upon completion, maintaining replication order.
        /// </summary>
        /// <typeparam name="TResult">The result of the action.</typeparam>
        /// <param name="operation">The operation.</param>
        /// <param name="action">The action.</param>
        /// <param name="cancellationToken">
        /// The cancellation token.
        /// </param>
        /// <returns>The result of invoking the action.</returns>
        Task<TResult> Commit<TResult>(TOperation operation, Func<TResult> action, CancellationToken cancellationToken);
    }
}