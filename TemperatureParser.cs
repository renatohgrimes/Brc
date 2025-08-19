using System.Runtime.CompilerServices;

class TemperatureParser
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool TryParse(in Span<byte> span, out float parse)
    {
        parse = 0f;

        if (span.Length < 2)
            return false;

        var dotIndex = span.Length - 2;

        var result = 0f;

        byte digitByte = span[^1];

        if (digitByte < (byte)'0' || digitByte > (byte)'9')
            return false;

        result += (digitByte - (byte)'0') * 0.1f;

        float multiplier = 1f;
        for (var i = dotIndex - 1; i >= 0; i--)
        {
            digitByte = span[i];

            if (i == 0 && digitByte == (byte)'-')
            {
                result = -result;
                break;
            }

            if (digitByte < (byte)'0' || digitByte > (byte)'9')
                return false;

            int digit = digitByte - (byte)'0';
            result += digit * multiplier;
            multiplier *= 10f;
        }

        parse = result;
        return true;
    }
}
