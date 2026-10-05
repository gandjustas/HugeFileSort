using System.Buffers;
using System.Collections;
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

        /// <summary>Пишет последовательность поэлементно; сегменты не копируются целиком.</summary>
        public void Write(ReadOnlySequence<byte> value)
        {
            foreach (var segment in value)
                writer.Write(segment.Span);
        }
    }

    /// <summary>Побайтовое сравнение последовательности со спаном; fast path — один сегмент.</summary>
    public static int SequenceCompareTo(this in ReadOnlySequence<byte> x, ReadOnlySpan<byte> y)
    {
        if (x.IsSingleSegment)
            return x.FirstSpan.SequenceCompareTo(y);

        foreach (var segment in x)
        {
            var span = segment.Span;
            var take = Math.Min(span.Length, y.Length);
            var c = span[..take].SequenceCompareTo(y[..take]);
            if (c != 0)
                return c;
            span = span[take..];
            if (span.Length == 0)
                break;
            y = y[take..];
            if (y.Length == 0)
                return 1;
        }
        return x.Length > y.Length ? 1 : x.Length < y.Length ? -1 : 0;
    }

    /// <summary>Побайтовое сравнение последовательностей; fast path — обе в одном сегменте.</summary>
    public static int SequenceCompareTo(this in ReadOnlySequence<byte> x, ReadOnlySequence<byte> y)
    {
        if (x.IsSingleSegment && y.IsSingleSegment)
            return x.FirstSpan.SequenceCompareTo(y.FirstSpan);

        SequenceReader<byte> xr = new(x);
        SequenceReader<byte> yr = new(y);
        while (xr.Remaining > 0 && yr.Remaining > 0)
        {
            var take = Math.Min(xr.UnreadSpan.Length, yr.UnreadSpan.Length);
            var c = xr.UnreadSpan[..take].SequenceCompareTo(yr.UnreadSpan[..take]);
            if (c != 0)
                return c;
            xr.Advance(take);
            yr.Advance(take);
        }
        return xr.Remaining.CompareTo(yr.Remaining);
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

    /// <summary>Читает LEB128 varint из последовательности. false — данных недостаточно.</summary>
    public static bool TryReadVarint(this ref SequenceReader<byte> reader, out ulong value)
    {
        value = 0;
        ulong result = 0;
        var shift = 0;
        for (var i = 0; i < 10; i++)
        {
            if (!reader.TryRead(out var b))
                return false;
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

    /// <summary>
    /// Разворачивает δ-варинты значений записи в поток значений. sequence резолвится по
    /// позициям, SequenceReader создаётся на каждое значение (ref struct нельзя хранить
    /// между MoveNext).
    /// </summary>
    public static IEnumerable<long> UnfoldValues(ReadOnlySequence<byte> values) => new ValuesEnumerable(values);

    private sealed class ValuesEnumerable(ReadOnlySequence<byte> values) : IEnumerable<long>
    {
        public IEnumerator<long> GetEnumerator() => new Enumerator(values);

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

        private sealed class Enumerator(ReadOnlySequence<byte> values) : IEnumerator<long>
        {
            // Смещение в байтах, а не SequencePosition: объекты сегментов пайпа рециклируются,
            // и объект позиции нельзя хранить в долгоживущем состоянии.
            private long offset;
            private long previous;
            private long current;

            public bool MoveNext()
            {
                if (offset >= values.Length)
                    return false;
                SequenceReader<byte> reader = new(values.Slice(offset));
                if (!TryReadVarint(ref reader, out var delta))
                    return false;
                offset += reader.Consumed;
                previous += checked((long)delta);
                current = previous;
                return true;
            }

            public long Current => current;

            object IEnumerator.Current => current;

            public void Reset() => throw new NotSupportedException();

            public void Dispose() { }
        }
    }
}
