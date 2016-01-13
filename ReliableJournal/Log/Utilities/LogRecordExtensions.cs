namespace ReliableJournal.Log.Utilities
{
    using System.IO.Log;

    internal static class LogRecordExtensions
    {
        public static string DetailedString(this LogRecord record)
            =>
                $"Seq: {record.SequenceNumber.Identifier()} Prev: {record.Previous.Identifier()} User: {record.User.Identifier()} Data: {record.Data.Length}b";
    }
}