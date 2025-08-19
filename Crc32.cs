using System.Runtime.CompilerServices;

static class Crc32
{
    private static readonly uint[] _table;

    static Crc32()
    {
        _table = new uint[256];
        const uint polynomial = 0xEDB88320;

        for (uint i = 0; i < _table.Length; i++)
        {
            uint crc = i;
            for (int j = 0; j < 8; j++)
            {
                bool bit = (crc & 1) == 1;
                crc >>= 1;
                if (bit)
                    crc ^= polynomial;
            }
            _table[i] = crc;
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static uint Compute(in Span<byte> data)
    {
        uint crc = 0xFFFFFFFF;
        foreach (byte b in data)
        {
            byte index = (byte)((crc ^ b) & 0xFF);
            crc = (crc >> 8) ^ _table[index];
        }
        return ~crc;
    }
}
