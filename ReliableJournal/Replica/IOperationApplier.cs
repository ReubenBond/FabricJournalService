namespace ReliableJournal.Replica
{
    using System.Fabric;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// An object which can apply copied and replicated operations.
    /// </summary>
    internal interface IOperationApplier
    {
        /// <summary>
        /// Applies the provided, replicated <paramref name="operation"/>.
        /// </summary>
        /// <param name="operation">The operation.</param>
        /// <param name="cancellationtoken">The cancellation token.</param>
        /// <returns>A <see cref="Task"/> representing the work performed.</returns>
        Task ApplyReplicationOperation(IOperation operation, CancellationToken cancellationtoken);

        /// <summary>
        /// Applies the provided, copied <paramref name="operation"/>.
        /// </summary>
        /// <param name="operation">The operation.</param>
        /// <param name="cancellationtoken">The cancellation token.</param>
        /// <returns>A <see cref="Task"/> representing the work performed.</returns>
        Task ApplyCopyOperation(IOperation operation, CancellationToken cancellationtoken);
    }
}