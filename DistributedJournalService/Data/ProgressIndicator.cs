namespace DistributedJournalService.Data
{
    using System;
    using System.Fabric;

    using ProtoBuf;

    [Serializable]
    [ProtoContract]
    public struct ProgressIndicator : IComparable<ProgressIndicator>, IEquatable<ProgressIndicator>
    {
        public ProgressIndicator(Epoch epoch, long previousEpochHighestLogSequenceNumber)
        {
            this.EpochDataLossNumber = epoch.DataLossNumber;
            this.EpochConfigurationNumber = epoch.ConfigurationNumber;
            this.PreviousEpochHighestLogSequenceNumber = previousEpochHighestLogSequenceNumber;
        }
        
        public static ProgressIndicator Zero { get; } = new ProgressIndicator(new Epoch(0, 0), 0);
        
        /// <summary>
        /// Gets the highest log sequence number from the previous epoch.
        /// </summary>
        [ProtoMember(1)]
        public long PreviousEpochHighestLogSequenceNumber { get; private set; }

        /// <summary>
        /// Gets the DataLossNumber component of the epoch.
        /// </summary>
        [ProtoMember(2)]
        private long EpochDataLossNumber { get; set; }

        /// <summary>
        /// Gets the ConfigurationNumber component of the epoch.
        /// </summary>
        [ProtoMember(3)]
        private long EpochConfigurationNumber { get; set;  }

        /// <summary>
        /// Gets the epoch.
        /// </summary>
        public Epoch Epoch => new Epoch(this.EpochDataLossNumber, this.EpochConfigurationNumber);

        /// <summary>
        /// Returns a string that represents the current object.
        /// </summary>
        /// <returns>
        /// A string that represents the current object.
        /// </returns>
        public override string ToString()
        {
            return $"(Epoch: {this.Epoch.DataLossNumber}.{this.Epoch.ConfigurationNumber}, LSN:{this.PreviousEpochHighestLogSequenceNumber})";
        }

        public int CompareTo(ProgressIndicator other)
        {
            if (this.Epoch > other.Epoch)
            {
                return 1;
            }

            if (this.Epoch < other.Epoch)
            {
                return -1;
            }

            if (this.PreviousEpochHighestLogSequenceNumber == other.PreviousEpochHighestLogSequenceNumber) return 0;
            if (this.PreviousEpochHighestLogSequenceNumber > other.PreviousEpochHighestLogSequenceNumber) return 1;
            return -1;
        }

        public bool Equals(ProgressIndicator other)
        {
            return this.Epoch.Equals(other.Epoch) && this.PreviousEpochHighestLogSequenceNumber == other.PreviousEpochHighestLogSequenceNumber;
        }

        /// <summary>
        /// Determines whether the specified object is equal to the current object.
        /// </summary>
        /// <returns>
        /// true if the specified object  is equal to the current object; otherwise, false.
        /// </returns>
        /// <param name="obj">The object to compare with the current object. </param>
        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj))
            {
                return false;
            }

            if (obj.GetType() != this.GetType())
            {
                return false;
            }

            return this.Equals((ProgressIndicator)obj);
        }

        /// <summary>
        /// Serves as the default hash function. 
        /// </summary>
        /// <returns>
        /// A hash code for the current object.
        /// </returns>
        public override int GetHashCode()
        {
            unchecked
            {
                return (this.Epoch.GetHashCode() * 397) ^ this.PreviousEpochHighestLogSequenceNumber.GetHashCode();
            }
        }

        public static bool operator ==(ProgressIndicator left, ProgressIndicator right)
        {
            return Equals(left, right);
        }

        public static bool operator !=(ProgressIndicator left, ProgressIndicator right)
        {
            return !Equals(left, right);
        }

        public static bool operator >(ProgressIndicator left, ProgressIndicator right)
        {
            return left.Epoch > right.Epoch
                   || (left.Epoch == right.Epoch && left.PreviousEpochHighestLogSequenceNumber > right.PreviousEpochHighestLogSequenceNumber);
        }

        public static bool operator <(ProgressIndicator left, ProgressIndicator right)
        {
            return left.Epoch < right.Epoch
                   || (left.Epoch == right.Epoch && left.PreviousEpochHighestLogSequenceNumber < right.PreviousEpochHighestLogSequenceNumber);
        }
    }
}