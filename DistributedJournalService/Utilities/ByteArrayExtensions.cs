namespace DistributedJournalService.Utilities
{
    internal static class ByteArrayExtensions
    {
        private static readonly uint[] LookupTable = CreateLookup();

        private static uint[] CreateLookup()
        {
            var result = new uint[256];
            for (var i = 0; i < 256; i++)
            {
                var s = i.ToString("X2");
                result[i] = s[0] + ((uint)s[1] << 16);
            }
            return result;
        }

        public static string ToHexString(this byte[] bytes)
        {
            if (bytes == null)
            {
                return string.Empty;
            }

            var lookup = LookupTable;
            var result = new char[bytes.Length * 2];
            for (var i = 0; i < bytes.Length; i++)
            {
                var val = lookup[bytes[i]];
                result[2 * i] = (char)val;
                result[2 * i + 1] = (char)(val >> 16);
            }

            return new string(result);
        }
    }
}
