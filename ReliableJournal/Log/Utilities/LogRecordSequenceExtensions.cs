namespace ReliableJournal.Log.Utilities
{
    using System;
    using System.Collections.Generic;
    using System.IO.Log;
    using System.Threading.Tasks;

    internal static class LogRecordSequenceExtensions
    {
        public static Task<SequenceNumber> AppendAsync(
            this LogRecordSequence sequence,
            ArraySegment<byte> data,
            SequenceNumber nextUndoRecord,
            SequenceNumber previousRecord,
            RecordAppendOptions recordAppendOptions)
        {
            return
                Task.Factory.FromAsync(
                    (callback, state) =>
                    ((LogRecordSequence)state).BeginAppend(
                        data,
                        nextUndoRecord,
                        previousRecord,
                        recordAppendOptions,
                        callback,
                        state),
                    sequence.EndAppend,
                    sequence);
        }

        public static Task<SequenceNumber> AppendAsync(
            this LogRecordSequence sequence,
            IList<ArraySegment<byte>> data,
            SequenceNumber userRecord,
            SequenceNumber previousRecord,
            RecordAppendOptions recordAppendOptions)
        {
            return
                Task.Factory.FromAsync(
                    (callback, state) =>
                    ((LogRecordSequence)state).BeginAppend(
                        data,
                        userRecord,
                        previousRecord,
                        recordAppendOptions,
                        callback,
                        state),
                    sequence.EndAppend,
                    sequence);
        }

        public static Task<SequenceNumber> AppendAsync(
            this LogRecordSequence sequence,
            ArraySegment<byte> data,
            SequenceNumber nextUndoRecord,
            SequenceNumber previousRecord,
            RecordAppendOptions recordAppendOptions,
            ReservationCollection reservationCollection)
        {
            return
                Task.Factory.FromAsync(
                    (callback, state) =>
                    ((LogRecordSequence)state).BeginAppend(
                        data,
                        nextUndoRecord,
                        previousRecord,
                        recordAppendOptions,
                        reservationCollection,
                        callback,
                        state),
                    sequence.EndAppend,
                    sequence);
        }

        public static Task<SequenceNumber> AppendAsync(
            this LogRecordSequence sequence,
            IList<ArraySegment<byte>> data,
            SequenceNumber userRecord,
            SequenceNumber previousRecord,
            RecordAppendOptions recordAppendOptions,
            ReservationCollection reservationCollection)
        {
            return
                Task.Factory.FromAsync(
                    (callback, state) =>
                    ((LogRecordSequence)state).BeginAppend(
                        data,
                        userRecord,
                        previousRecord,
                        recordAppendOptions,
                        reservationCollection,
                        callback,
                        state),
                    sequence.EndAppend,
                    sequence);
        }

        public static Task<SequenceNumber> FlushAsync(this LogRecordSequence sequence, SequenceNumber sequenceNumber)
        {
            return
                Task.Factory.FromAsync(
                    (callback, state) => ((LogRecordSequence)state).BeginFlush(sequenceNumber, callback, state),
                    sequence.EndFlush,
                    sequence);
        }

        /// <summary>
        /// Reserves and appends a set of records.
        /// </summary>
        /// <param name="sequence">The log record sequence.</param>
        /// <param name="data">A list of byte array segments that will be concatenated and appended as the record.</param>
        /// <param name="userRecord">The sequence number of the next record in the user-specified order.</param>
        /// <param name="previousRecord">The sequence number of the next record in Previous order.</param>
        /// <param name="recordAppendOptions">A valid value of RecordAppendOptions that specifies how the data should be written.</param>
        /// <param name="reservationCollection">The reservation collection to make reservations in.</param>
        /// <param name="reservations">The reservations to make, in bytes.</param>
        /// <returns>The sequence number of the appended log record.</returns>
        public static Task<SequenceNumber> ReserveAndAppendAsync(
            this LogRecordSequence sequence,
            IList<ArraySegment<byte>> data,
            SequenceNumber userRecord,
            SequenceNumber previousRecord,
            RecordAppendOptions recordAppendOptions,
            ReservationCollection reservationCollection,
            params long[] reservations)
        {
            return
                Task.Factory.FromAsync(
                    (callback, state) =>
                    ((LogRecordSequence)state).BeginReserveAndAppend(
                        data,
                        userRecord,
                        previousRecord,
                        recordAppendOptions,
                        reservationCollection,
                        reservations,
                        callback,
                        state),
                    sequence.EndReserveAndAppend,
                    sequence);
        }

        /// <summary>
        /// Reserves and appends a set of records.
        /// </summary>
        /// <param name="sequence">The log record sequence.</param>
        /// <param name="data">Byte array segments that will be concatenated and appended as the record.</param>
        /// <param name="userRecord">The sequence number of the next record in the user-specified order.</param>
        /// <param name="previousRecord">The sequence number of the next record in Previous order.</param>
        /// <param name="recordAppendOptions">A valid value of RecordAppendOptions that specifies how the data should be written.</param>
        /// <param name="reservationCollection">The reservation collection to make reservations in.</param>
        /// <param name="reservations">The reservations to make, in bytes.</param>
        /// <returns>The sequence number of the appended log record.</returns>
        public static Task<SequenceNumber> ReserveAndAppendAsync(
            this LogRecordSequence sequence,
            ArraySegment<byte> data,
            SequenceNumber userRecord,
            SequenceNumber previousRecord,
            RecordAppendOptions recordAppendOptions,
            ReservationCollection reservationCollection,
            params long[] reservations)
        {
            return
                Task.Factory.FromAsync(
                    (callback, state) =>
                    ((LogRecordSequence)state).BeginReserveAndAppend(
                        data,
                        userRecord,
                        previousRecord,
                        recordAppendOptions,
                        reservationCollection,
                        reservations,
                        callback,
                        state),
                    sequence.EndReserveAndAppend,
                    sequence);
        }

        /// <summary>
        /// Reserves and appends a set of records.
        /// </summary>
        /// <param name="sequence">The log record sequence.</param>
        /// <param name="data">Byte array segments that will be concatenated and appended as the record.</param>
        /// <param name="newBaseSeqNum">The new base sequence number. The specified sequence number must be greater than or equal to the current base sequence number.</param>
        /// <param name="reservationCollection">The reservation collection to make reservations in.</param>
        /// <returns>The sequence number of the appended log record.</returns>
        public static Task<SequenceNumber> WriteRestartArea(
            this LogRecordSequence sequence,
            ArraySegment<byte> data,
            SequenceNumber newBaseSeqNum,
            ReservationCollection reservationCollection)
        {
            return
                Task.Factory.FromAsync(
                    (callback, state) =>
                    ((LogRecordSequence)state).BeginWriteRestartArea(
                        data,
                        newBaseSeqNum,
                        reservationCollection,
                        callback,
                        state),
                    sequence.EndWriteRestartArea,
                    sequence);
        }

        /// <summary>
        /// Reserves and appends a set of records.
        /// </summary>
        /// <param name="sequence">The log record sequence.</param>
        /// <param name="data">Byte array segments that will be concatenated and appended as the record.</param>
        /// <param name="newBaseSeqNum">The new base sequence number. The specified sequence number must be greater than or equal to the current base sequence number.</param>
        /// <param name="reservationCollection">The reservation collection to make reservations in.</param>
        /// <returns>The sequence number of the appended log record.</returns>
        public static Task<SequenceNumber> WriteRestartArea(
            this LogRecordSequence sequence,
            IList<ArraySegment<byte>> data,
            SequenceNumber newBaseSeqNum,
            ReservationCollection reservationCollection)
        {
            return
                Task.Factory.FromAsync(
                    (callback, state) =>
                    ((LogRecordSequence)state).BeginWriteRestartArea(
                        data,
                        newBaseSeqNum,
                        reservationCollection,
                        callback,
                        state),
                    sequence.EndWriteRestartArea,
                    sequence);
        }
    }
}