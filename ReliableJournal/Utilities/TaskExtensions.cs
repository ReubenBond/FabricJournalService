namespace ReliableJournal.Utilities
{
    using System;
    using System.Diagnostics;
    using System.Threading.Tasks;

    internal static class TaskExtensions
    {
        public static void PropagateToCompletion<T>(this Task<T> task, TaskCompletionSource<T> completion)
        {
            task.ContinueWith(
                (Task<T> antecedent) =>
                {
                    switch (antecedent.Status)
                    {
                        case TaskStatus.RanToCompletion:
                            completion.TrySetResult(antecedent.GetAwaiter().GetResult());
                            break;
                        case TaskStatus.Canceled:
                            completion.TrySetCanceled();
                            break;
                        case TaskStatus.Faulted:
                            // ReSharper disable once AssignNullToNotNullAttribute
                            completion.TrySetException(antecedent.Exception);
                            break;
                        default:
                            var msg =
                                $"Antecedent task status is {antecedent.Status} in {nameof(PropagateToCompletion)}, which is unsupported.";
                            completion.TrySetException(new ArgumentOutOfRangeException(msg));
                            break;
                    }

                    return default(object);
                },
                TaskContinuationOptions.ExecuteSynchronously);
        }

        public static void PropagateToCompletion<T>(this Task task, TaskCompletionSource<T> completion)
        {
            task.ContinueWith(
                antecedent =>
                {
                    switch (antecedent.Status)
                    {
                        case TaskStatus.RanToCompletion:
                            completion.TrySetResult(default(T));
                            break;
                        case TaskStatus.Canceled:
                            completion.TrySetCanceled();
                            break;
                        case TaskStatus.Faulted:
                            // ReSharper disable once AssignNullToNotNullAttribute
                            completion.TrySetException(antecedent.Exception);
                            break;
                        default:
                            var msg =
                                $"Antecedent task status is {antecedent.Status} in {nameof(PropagateToCompletion)}, which is unsupported.";
                            completion.TrySetException(new ArgumentOutOfRangeException(msg));
                            break;
                    }

                    return default(object);
                },
                TaskContinuationOptions.ExecuteSynchronously);
        }

        public static async Task Suppressed(this Task task)
        {
            try
            {
                await task.ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                // Ignore...
                //TODO: Log anway.
                Debug.WriteLine("Exception suppressed: " + exception);
            }
        }
    }
}