using System.Buffers;
using System.IO.Pipelines;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using MessagePack;


static class Extensions
{
    public static async IAsyncEnumerable<T> DeserializeAsyncEnumerable<T>(
        this Stream stream,
        MessagePackSerializerOptions? options = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        // Создаем читатель для асинхронной работы с потоком
        using var reader = new MessagePackStreamReader(stream);

        // Читаем следующий токен (объект) из потока асинхронно
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false) is { } sequence)
        {
            // Десериализуем прочитанный фрагмент в нужный тип
            yield return MessagePackSerializer.Deserialize<T>(sequence, options, cancellationToken);
        }
    }



    public static void Write<T>(this MessagePackWriter writer, T value) where T: INumber<T>
    {
        switch(value)
        {
            case long x: writer.Write(x); break;
            case ulong x: writer.Write(x); break;
            case int x: writer.Write(x); break;
            case uint x: writer.Write(x); break;
            case short x: writer.Write(x); break;
            case ushort x: writer.Write(x); break;
            case sbyte x: writer.Write(x); break;
            case byte x: writer.Write(x); break;
            default: throw new InvalidOperationException($"Can't serialize {typeof(T)} with MessagePack");
        }
    }
    extension(IBufferWriter<byte> writer)
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Write<T>(T value, ReadOnlySpan<char> format) where T : IUtf8SpanFormattable
        {
            var destination = writer.GetSpan(32);
            int bytesWritten;
            while (!value.TryFormat(destination, out bytesWritten, format, null))
            {
                destination = writer.GetSpan(destination.Length * 2);
            }
            writer.Advance(bytesWritten);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Write(in ReadOnlySequence<byte> value)
        {
            var l = (int)value.Length;
            var destination = writer.GetSpan(l);
            value.CopyTo(destination);
            writer.Advance(l);
        }

        /// <summary>Пишет число в формате LEB128 varint: 7 бит на байт, старший бит — флаг продолжения.</summary>
        public void WriteVarint(ulong value)
        {
            var size = 1 + (int)(BitOperations.Log2(value | 1) / 7);
            var destination = writer.GetSpan(size);
            var i = 0;
            while (value >= 0x80)
            {
                destination[i++] = (byte)(value | 0x80);
                value >>= 7;
            }
            destination[i] = (byte)value;
            writer.Advance(i + 1);
        }

    }

    /// <summary>Пишет число в формате LEB128 varint в поток.</summary>
    public static void WriteVarint(this Stream stream, ulong value)
    {
        Span<byte> buffer = stackalloc byte[10];
        var i = 0;
        while (value >= 0x80)
        {
            buffer[i++] = (byte)(value | 0x80);
            value >>= 7;
        }
        buffer[i++] = (byte)value;
        stream.Write(buffer[..i]);
    }

    /// <summary>
    /// Читает LEB128 varint. false — данных недостаточно (нужен ещё ReadAsync);
    /// FormatException — больше 10 байт без терминатора, файл повреждён.
    /// </summary>
    public static bool TryReadVarint(ref SequenceReader<byte> reader, out ulong value)
    {
        value = 0;
        var scratch = new SequenceReader<byte>(reader.Sequence.Slice(reader.Position));
        ulong result = 0;
        var shift = 0;
        for (var i = 0; i < 10; i++)
        {
            if (!scratch.TryRead(out var b))
                return false;
            result |= (ulong)(b & 0x7F) << shift;
            if ((b & 0x80) == 0)
            {
                reader.Advance(i + 1);
                value = result;
                return true;
            }
            shift += 7;
        }
        throw new FormatException("Varint is too long, file is corrupted");
    }

}
