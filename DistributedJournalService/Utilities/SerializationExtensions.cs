namespace DistributedJournalService.Utilities
{
    using System;

    using ProtoBuf;

    internal static class SerializationExtensions
    {
        public static ArraySegment<byte> Serialize(this object @event)
        {
            using (var stream = MemoryStreamManager.Instance.GetStream("Serialize"))
            {
                Serializer.Serialize(stream, @event);
                var serialized = stream.ToArray();
                return new ArraySegment<byte>(serialized, 0, serialized.Length);
            }
        }

        public static T Deserialize<T>(this ArraySegment<byte> item)
        {
            using (
                var stream = MemoryStreamManager.Instance.GetStream(
                    "Deserialize",
                    item.Array,
                    item.Offset,
                    item.Count))
            {
                return Serializer.Deserialize<T>(stream);
            }
        }
    }
}