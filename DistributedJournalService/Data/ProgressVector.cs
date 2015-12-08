namespace DistributedJournalService.Data
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Diagnostics.Contracts;
    using System.Linq;

    using DistributedJournalService.Records;

    using ProtoBuf;

    /// <summary>
    /// Represents a series of epochs and the progress of each.
    /// </summary>
    [Serializable]
    [ProtoContract]
    public class ProgressVector : IReadOnlyList<ProgressIndicator>
    {
        public ProgressVector() { }

        public ProgressVector(IEnumerable<ProgressIndicator> values)
        {
            foreach (var value in values)
            {
                this.Update(value);
            }
        }

        /// <summary>
        /// Gets the progress indicators.
        /// </summary>
        [ProtoMember(1)]
        private List<ProgressIndicator> Progress { get; set; } = new List<ProgressIndicator>();

        /// <summary>
        /// Gets the current data version.
        /// </summary>
        public ProgressIndicator Current => this.Progress?.LastOrDefault() ?? ProgressIndicator.Zero;
        
        /// <summary>
        /// Gets the sequence number of the current version. 
        /// </summary>
        public long SequenceNumber => this.Current.PreviousEpochHighestLogSequenceNumber;

        /// <summary>
        /// Returns an enumerator that iterates through the collection.
        /// </summary>
        /// <returns>
        /// An enumerator that can be used to iterate through the collection.
        /// </returns>
        public IEnumerator<ProgressIndicator> GetEnumerator()
        {
            return this.Progress.GetEnumerator();
        }

        /// <summary>
        /// Returns a string that represents the current object.
        /// </summary>
        /// <returns>
        /// A string that represents the current object.
        /// </returns>
        public override string ToString()
        {
            return string.Join(", ", this.Progress ?? Enumerable.Empty<ProgressIndicator>());
        }

        /// <summary>
        /// Returns an enumerator that iterates through a collection.
        /// </summary>
        /// <returns>
        /// An <see cref="T:System.Collections.IEnumerator"/> object that can be used to iterate through the collection.
        /// </returns>
        IEnumerator IEnumerable.GetEnumerator()
        {
            return ((IEnumerable)this.Progress).GetEnumerator();
        }

        /// <summary>
        /// Updates the progress vector with a new version.
        /// </summary>
        /// <param name="update">The new version.</param>
        /// <remarks>A value indicating whether or not the progress vector was updated.</remarks>
        public void Update(ProgressIndicator update)
        {
            if (update == ProgressIndicator.Zero)
            {
                return;
            }
            
            // If the progress vector is empty, initialize it with the update.
            if (this.Progress.Count == 0)
            {
                this.Progress.Add(update);
                return;
            }

            var current = this.Current;

            // If the current version is from a previous epoch, append the update.
            if (current.Epoch < update.Epoch)
            {
                this.Progress.Add(update);
                return;
            }

            // If the epochs match and the update has a higher sequence, replace latest version with the update.
            if (current.Epoch == update.Epoch
                && current.PreviousEpochHighestLogSequenceNumber < update.PreviousEpochHighestLogSequenceNumber)
            {
                this.Progress[this.Progress.Count - 1] = update;
            }
        }

        /// <summary>
        /// Returns the element of this instance which are not in the other instance.
        /// </summary>
        /// <param name="other">The other instance.</param>
        /// <returns>The elements of this isntance which are not in the other instance.</returns>
        [Pure]
        public IEnumerable<ProgressIndicator> Excluding(ProgressVector other)
        {
            if (this.Progress.Count == 0)
            {
                yield break;
            }

            var firstLocal = this.Progress[0];

            // Skip to the first local version which matches the epoch of the other vector.
            var i = 0;
            while (other.Count > i && other[i].Epoch < firstLocal.Epoch) i++;

            // Enumerate all items in this instance, returning any which aren't in the other instance.
            foreach (var current in this.Progress)
            {
                if (other.Count <= i || other[i] != current)
                {
                    // Return all elements which are not present in the other vector.
                    yield return current;
                }

                i++;
            }
        }

        /// <summary>
        /// Gets the number of elements in the collection.
        /// </summary>
        /// <returns>
        /// The number of elements in the collection. 
        /// </returns>
        public int Count => this.Progress.Count;

        /// <summary>
        /// Gets the element at the specified index in the read-only list.
        /// </summary>
        /// <returns>
        /// The element at the specified index in the read-only list.
        /// </returns>
        /// <param name="index">The zero-based index of the element to get. </param>
        public ProgressIndicator this[int index] => this.Progress[index];

        /// <summary>
        /// Returns a value indicating whether or not this progress vector includes the provided
        /// <paramref name="version"/>.
        /// </summary>
        /// <param name="version">The version.</param>
        /// <returns>
        /// A value indicating whether or not this progress vector includes the provided <paramref name="version"/>.
        /// </returns>
        public bool IncludesVersionInPreviousEpoch(DataVersion version)
        {
            var foundEntry = false;

            foreach (var entry in this.Progress)
            {
                if (entry.Epoch == version.Epoch)
                {
                    // The next entry will indicate the highest committed log sequence number for
                    // the subject's epoch.
                    foundEntry = true;
                    continue;
                }

                if (foundEntry)
                {
                    // If the previous epoch (i.e, the subject's epoch) had a highest log sequence number
                    // which is greater or equal to the subject's log sequence number, then this progress
                    // vector includes the subject.
                    return entry.PreviousEpochHighestLogSequenceNumber >= version.LogSequenceNumber;
                }

                if (entry.Epoch > version.Epoch)
                {
                    // Epochs are ordered, so if this epoch is greater than the subject's epoch, but
                    // the previous epoch was not equal to the subject's epoch, then the subject's epoch
                    // is not included in this progress vector.
                    return false;
                }
            }

            return false;
        }
    }
}