namespace ReliableJournal.Log.Utilities
{
    using System.IO.Log;
    using System.Linq;

    internal static class SequenceNumberExtensions
    {
        public static string Identifier(this SequenceNumber sequence)
            =>
                sequence == SequenceNumber.Invalid
                    ? "INVALID"
                    : string.Concat(sequence.GetBytes().Select(_ => _.ToString("X")));
    }
}