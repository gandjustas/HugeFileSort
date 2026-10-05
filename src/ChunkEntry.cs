using System.Buffers;
using System.Buffers.Binary;
using System.Globalization;
using System.IO.Hashing;
using System.Runtime.InteropServices;
using System.Text;

/// <summary>
/// Материализованная запись чанка: текст, ключ (culture — sort key, ordinal — срез текста) и
/// развёрнутые значения (по 8 байт на long, по возрастанию) в одном арендованном из пула
/// массиве — раскладка [текст][ключ][значения]. Владеет массивом: после записи группы
/// Return() возвращает его в пул.
/// </summary>
readonly struct ChunkEntry(byte[] rented, int hash, ArraySegment<byte> key, ArraySegment<byte> text, ArraySegment<byte> values, int valuesCount)
{
    private const int StackCharLimit = 4096;

    private readonly byte[] buffer = rented;

    /// <summary>Хэш байтов ключа, посчитан парсером чанка.</summary>
    public int Hash { get; } = hash;

    /// <summary>Байты ключа (culture — sort key, ordinal — текст).</summary>
    public ArraySegment<byte> Key { get; } = key;

    /// <summary>Байты текста.</summary>
    public ArraySegment<byte> Text { get; } = text;

    /// <summary>Развёрнутые значения: ValuesCount long'ов по возрастанию.</summary>
    public ArraySegment<byte> Values { get; } = values;

    public int ValuesCount { get; } = valuesCount;

    public void Return() => ArrayPool<byte>.Shared.Return(buffer);

    public static ChunkEntry Create(ReadOnlySequence<byte> text, ReadOnlySequence<byte> rawValues, long valuesCount,
        bool ordinal, Encoding encoding, CompareInfo compareInfo, CompareOptions compareOptions)
    {
        var textSize = checked((int)text.Length);
        var valuesSize = checked((int)valuesCount) * 8;

        // Ключ: ordinal — срез текста; culture — sort key (длина известна после декодирования чаров).
        var maxChars = ordinal ? 0 : encoding.GetMaxCharCount(textSize);
        char[]? rentedChars = maxChars > StackCharLimit ? ArrayPool<char>.Shared.Rent(maxChars) : null;
        Span<char> chars = maxChars > StackCharLimit ? rentedChars!.AsSpan() : stackalloc char[StackCharLimit];
        int written = 0;
        if (!ordinal)
        {
            written = encoding.GetChars(text, chars);
            chars = chars[..written];
        }
        var keySize = ordinal ? textSize : compareInfo.GetSortKeyLength(chars, compareOptions);

        var buffer = ArrayPool<byte>.Shared.Rent(ordinal ? textSize + valuesSize : textSize + keySize + valuesSize);
        try
        {
            // Ordinal: ключ — срез текста (одна копия); culture — отдельный регион с sort key.
            var key = ordinal
                ? new ArraySegment<byte>(buffer, 0, textSize)
                : new ArraySegment<byte>(buffer, textSize, keySize);
            var textSegment = new ArraySegment<byte>(buffer, 0, textSize);
            var values = new ArraySegment<byte>(buffer, ordinal ? textSize : textSize + keySize, valuesSize);

            text.CopyTo(textSegment);
            if (!ordinal)
                compareInfo.GetSortKey(chars, key, compareOptions);
            Unfold(rawValues, values);
            return new ChunkEntry(buffer, (int)XxHash32.HashToUInt32(key.AsSpan()), key, textSegment, values, (int)valuesCount);
        }
        catch
        {
            ArrayPool<byte>.Shared.Return(buffer);
            throw;
        }
        finally
        {
            if (rentedChars is not null)
                ArrayPool<char>.Shared.Return(rentedChars);
        }
    }

    /// <summary>Разворачивает δ-варинты значений в long'ы (значения записи уже полные).</summary>
    private static void Unfold(ReadOnlySequence<byte> values, ArraySegment<byte> target)
    {
        var targetLongs = MemoryMarshal.Cast<byte, long>(target.AsSpan());
        long previous = 0;
        var i = 0;

        // Быстрый путь: вся цепочка значений в одном сегменте (обычный случай).
        if (values.IsSingleSegment)
        {
            var span = values.FirstSpan;
            var pos = 0;
            while (i < targetLongs.Length)
            {
                if (!Extensions.TryReadVarint(span, ref pos, out var delta))
                    throw new FormatException("Chunk record values are truncated");
                previous += checked((long)delta);
                targetLongs[i++] = previous;
            }
            return;
        }

        ulong result = 0;
        var shift = 0;
        foreach (var segment in values)
        {
            foreach (var b in segment.Span)
            {
                result |= (ulong)(b & 0x7F) << shift;
                if ((b & 0x80) == 0)
                {
                    previous += unchecked((long)result);
                    targetLongs[i++] = previous;
                    result = 0;
                    shift = 0;
                }
                else
                {
                    shift += 7;
                    if (shift > 63)
                        throw new FormatException("Varint is too long, file is corrupted");
                }
            }
        }
        if (i != targetLongs.Length)
            throw new FormatException("Chunk record values are truncated");
    }
}
