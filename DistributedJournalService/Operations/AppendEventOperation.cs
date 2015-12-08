namespace DistributedJournalService.Operations
{
    using DistributedJournalService.Events;

    using ProtoBuf;

    [ProtoContract]
    internal class AppendEventOperation : Operation
    {
        public AppendEventOperation() { }

        public AppendEventOperation(Event @event)
        {
            this.Event = @event;
        }

        [ProtoMember(1)]
        public Event Event { get; private set; }

        /// <summary>
        /// Returns a string that represents the current object.
        /// </summary>
        /// <returns>
        /// A string that represents the current object.
        /// </returns>
        public override string ToString()
        {
            return $"[{nameof(AppendEventOperation)}] Event: {this.Event}";
        }
    }
}