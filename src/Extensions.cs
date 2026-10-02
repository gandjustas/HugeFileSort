using System.Buffers;
using System.Numerics;
using System.Runtime.CompilerServices;

static class Extensions
{
    extension(IBufferWriter<byte> writer)
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Write(ReadOnlySpan<byte> value)
        {
            var destination = writer.GetSpan(value.Length);
            value.CopyTo(destination);
            writer.Advance(value.Length);
        }
    }

    /// <summary>Пишет LEB128 varint в буфер, возвращает новую позицию.</summary>
    public static int WriteVarint(Span<byte> destination, int pos, ulong value)
    {
        while (value >= 0x80)
        {
            destination[pos++] = (byte)(value | 0x80);
            value >>= 7;
        }
        destination[pos++] = (byte)value;
        return pos;
    }

    /// <summary>
    /// Читает LEB128 varint из буфера. false — данных недостаточно (pos мог сдвинуться —
    /// вызывающий откатывает на начало записи); FormatException — больше 10 байт без терминатора.
    /// </summary>
    public static bool TryReadVarint(ReadOnlySpan<byte> span, ref int pos, out ulong value)
    {
        value = 0;
        ulong result = 0;
        var shift = 0;
        for (var i = 0; i < 10; i++)
        {
            if (pos >= span.Length)
                return false;
            var b = span[pos++];
            result |= (ulong)(b & 0x7F) << shift;
            if ((b & 0x80) == 0)
            {
                value = result;
                return true;
            }
            shift += 7;
        }
        throw new FormatException("Varint is too long, file is corrupted");
    }

}
