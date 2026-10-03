using System.Buffers;
using System.IO.Compression;
using System.Numerics;
using System.Runtime.InteropServices;
/// <summary>
/// Стадия отсортированного чанка: текстовый буфер (принят от Chunk), CSR-массив значений,
/// границы значений и текстов, порядок записи — все массивы арендованы из пулов и
/// возвращаются в Dispose. SerializedSize/Serialize готовят данные к сжатию,
/// Compress выполняет переход в CompressedChunk (Dispose вызывается сразу после Serialize).
/// </summary>
sealed class SortedChunk(
    ChunkPools pools,
    byte[] buffer,
    int keyCount,
    byte[] keyOffsetBuffer,
    byte[] keyLengthBuffer,
    byte[] valuesBuffer,
    byte[] valueOffsetBuffer,
    byte[] valueCountBuffer,
    byte[] orderBuffer) : IDisposable
{
    bool disposed = false;

    /// <summary>
    /// Сериализует чанк: varint(длина текста), текст, varint(количество чисел), varint(разности
    /// чисел) — для каждого ключа в порядке Order, возвращает записанный размер. Sort key в файл
    /// не пишутся — merge пересчитывает их пакетами.
    /// </summary>
    public int Serialize(Span<byte> dst)
    {
        var values = MemoryMarshal.Cast<byte, long>(valuesBuffer.AsSpan());
        var keyOffsets = MemoryMarshal.Cast<byte, int>(keyOffsetBuffer.AsSpan())[..keyCount];
        var keyLengths = MemoryMarshal.Cast<byte, int>(keyLengthBuffer.AsSpan())[..keyCount];
        var valueCounts = MemoryMarshal.Cast<byte, int>(valueCountBuffer.AsSpan())[..keyCount];
        var valueOffsets = MemoryMarshal.Cast<byte, int>(valueOffsetBuffer.AsSpan())[..keyCount];
        var order = MemoryMarshal.Cast<byte, int>(orderBuffer.AsSpan())[..keyCount];

        var pos = 0;
        // Только первые keyCount: арендованный буфер порядка длиннее и забит мусором из пула.
        for (var n = 0; n < order.Length; n++)
        {
            var i = order[n];
            var textLen = keyLengths[i];
            pos = Extensions.WriteVarint(dst, pos, (ulong)textLen);
            buffer.AsSpan(keyOffsets[i], textLen).CopyTo(dst[pos..]);
            pos += textLen;
            var start = valueOffsets[i];
            var count = valueCounts[i];
            pos = Extensions.WriteVarint(dst, pos, (ulong)count);
            long previous = 0;
            for (var j = start; j < start + count; j++)
            {
                pos = Extensions.WriteVarint(dst, pos, (ulong)(values[j] - previous));
                previous = values[j];
            }
        }
        return pos;
    }

    /// <summary>
    /// Переход сортировка → сжатие: сериализация в буфер S точного размера, one-shot zstd S → C.
    /// Имя файла чанку не нужно — его вычисляет блок записи.
    /// </summary>
    public CompressedChunk Compress(int quality, int maxWindowLog2)
    {
        // Сериализованный размер может превышать текстовый буфер: на пару в хвосте приходится
        // 16 Б, а varint-дельта ≤ 10 Б плюс до 10 Б на ключ (длина + счётчик) — арендуем с запасом.
        var s = pools.Scratch.Rent(checked(buffer.Length + 10 * keyCount));
        var size = Serialize(s);
        Dispose(); // буферы и массивы возвращаются в пулы до сжатия

        var cLength = checked((int)ZstandardEncoder.GetMaxCompressedLength(size));
        var c = pools.Scratch.Rent(cLength);
        using ZstandardEncoder encoder = new(new ZstandardCompressionOptions
        {
            Quality = quality,
            WindowLog = Math.Min(maxWindowLog2, Math.Clamp(BitOperations.Log2((uint)size) + 1, 20, 31)),
            EnableLongDistanceMatching = true,
        });
        // Один вызов на весь чанк (isFinalBlock дописывает footer кадра).
        var status = encoder.Compress(s.AsSpan(0, size), c.AsSpan(0, cLength),
            out var consumed, out var written, isFinalBlock: true);
        if (status != OperationStatus.Done || consumed != size)
            throw new InvalidOperationException($"zstd compression incomplete: {status}, {consumed} of {size}");
        pools.Scratch.Return(s);
        // В файл идёт ровно сжатый размер — не size (несжатый).
        return new CompressedChunk(pools, c, written);
    }

    public void Dispose()
    {
        if (disposed) return;

        pools.Scratch.Return(buffer);
        pools.Scratch.Return(keyOffsetBuffer);
        pools.Scratch.Return(keyLengthBuffer);
        pools.Scratch.Return(valuesBuffer);
        pools.Scratch.Return(valueOffsetBuffer);
        pools.Scratch.Return(valueCountBuffer);
        pools.Scratch.Return(orderBuffer);
        disposed = true;
    }
}
