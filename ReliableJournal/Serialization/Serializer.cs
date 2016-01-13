namespace ReliableJournal.Serialization
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;

    using Microsoft.IO;

    using ProtoBuf.Meta;

    internal class Serializer
    {
        private static readonly RecyclableMemoryStreamManager MemoryStreamManager = new RecyclableMemoryStreamManager();
        private readonly RuntimeTypeModel serializer;

        public Serializer(RuntimeTypeModel serializer)
        {
            this.serializer = serializer;
        }

        public ArraySegment<byte> Serialize<T>(T instance)
        {
            using (var stream = MemoryStreamManager.GetStream("Serialize"))
            {
                this.serializer.Serialize(stream, instance);
                var serialized = stream.ToArray();
                return new ArraySegment<byte>(serialized, 0, serialized.Length);
            }
        }

        public IEnumerable<T> Deserialize<T>(IEnumerable<System.IO.Log.LogRecord> records)
        {
            return records?.Select(this.Deserialize<T>) ?? Enumerable.Empty<T>();
        }

        public T Deserialize<T>(System.IO.Log.LogRecord logRecord)
        {
            return this.Deserialize<T>(logRecord.Data);
        }

        public T Deserialize<T>(ArraySegment<byte> item)
        {
            using (var stream = MemoryStreamManager.GetStream("Deserialize", item.Array, item.Offset, item.Count))
            {
                return this.Deserialize<T>(stream);
            }
        }

        /// <summary>
        /// Creates a new instance from a protocol-buffer stream.
        /// </summary>
        /// <typeparam name="T">The type to be created.</typeparam>
        /// <param name="source">The binary stream to apply to the new instance (cannot be null).</param>
        /// <returns>
        /// A new, initialized instance.
        /// </returns>
        public T Deserialize<T>(Stream source)
        {
            return (T)this.serializer.Deserialize(source, null, typeof(T));
        }
    }
}