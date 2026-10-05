using System.Buffers;
using System.Buffers.Binary;
using System.Collections;
using Microsoft.Extensions.ObjectPool;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

/// <summary>
/// Группа записей с равным ключом. Единственная запись не копируется — группа держит её
/// ChunkEntry и пишет прямо из его сегментов (штатный случай, ноль копий). Вторая равная
/// запись переводит группу в групповую раскладку: [count:4][ключ][текст₁][значения₁
/// (long'и)][текст₂][значения₂]…, мета записи r (0-based с первой) — с конца буфера:
/// ^(4r+1) start текста, ^(4r+2) длина текста, ^(4r+3) start значений, ^(4r+4) число значений.
/// Данные растут от головы, мета — от хвоста; при встрече буфер растёт (данные и мета
/// переносятся целиком, смещения в мете — абсолютные). Значения записей приходят от парсера
/// уже развёрнутыми и упорядоченными — при добавлении они memcpy'ятся. Не равная запись не
/// трогается (TryAdd = false, вернётся в хип). Экземпляры пулятся: Reset() возвращает массивы
/// записей и буфер группы в пулы. Все обращения — с потока мержа или задачи записи (после
/// SendAsync владение переходит к писателю).
/// </summary>
sealed class MergeGroup : IEnumerable<(Memory<byte>, long)>
{
    private static readonly int u64size = Unsafe.SizeOf<long>();
    private static readonly int u32size = Unsafe.SizeOf<int>();
    private const int MetaPerRecord = 16; // 4 int на запись в хвостовом блоке меты

    private ChunkEntry first;      // не материализованная одиночная запись (count == 1, !materialized)
    private bool materialized;
    private byte[]? buffer;        // групповая раскладка
    private int bufferSize;
    private int dataEnd;           // конец данных от начала буфера
    private int keyOffset;
    private int keySize;
    private int count;

    // Span<int> поверх буфера; пересоздание — дешёвый Cast, поэтому в итераторах не хранится
    // через yield (CS4007), а вычисляется на месте.
    private Span<int> Ints => MemoryMarshal.Cast<byte, int>(buffer!.AsSpan(0, bufferSize));

    /// <summary>Возвращает массивы записей и буфер группы в пулы, сбрасывает состояние.</summary>
    public void Reset()
    {
        if (buffer is not null)
            ArrayPool<byte>.Shared.Return(buffer);
        else if (count != 0)
            first.Return();
        first = default;
        materialized = false;
        buffer = null;
        bufferSize = 0;
        dataEnd = 0;
        keyOffset = 0;
        keySize = 0;
        count = 0;
    }

    public bool TryAdd(in ChunkEntry entry)
    {
        if (count == 0)
        {
            first = entry;
            count = 1;
            return true;
        }

        if (count == 1)
        {
            // Одиночная запись: ключ группы — её ключ; при равенстве материализуем раскладку.
            if (first.Key.AsSpan().SequenceCompareTo(entry.Key.AsSpan()) != 0)
                return false;
            MaterializeFirst(in entry);
            return true;
        }

        if (buffer!.AsSpan(keyOffset, keySize).SequenceCompareTo(entry.Key.AsSpan()) != 0)
            return false;
        AddMore(in entry);
        return true;
    }

    /// <summary>Переписывает одиночную запись и новую в групповую раскладку нового буфера.</summary>
    private void MaterializeFirst(in ChunkEntry second)
    {
        var firstText = first.Text;
        var firstValues = first.Values;
        var needed = u32size + first.Key.Count
            + firstText.Count + firstValues.Count + second.Text.Count + second.Values.Count
            + 2 * MetaPerRecord;
        var newBuffer = ArrayPool<byte>.Shared.Rent(Math.Max(1 << 20, needed));
        try
        {
            bufferSize = newBuffer.Length;
            var span = newBuffer.AsSpan(0, bufferSize);
            var ints = MemoryMarshal.Cast<byte, int>(span);
            ints[0] = 1;
            var offset = u32size;
            keyOffset = offset;
            first.Key.AsSpan().CopyTo(span.Slice(offset, first.Key.Count));
            keySize = first.Key.Count;
            offset += keySize;

            firstText.AsSpan().CopyTo(span.Slice(offset, firstText.Count));
            ints[^1] = offset;
            ints[^2] = firstText.Count;
            offset += firstText.Count;
            firstValues.AsSpan().CopyTo(span.Slice(offset, firstValues.Count));
            ints[^3] = offset;
            ints[^4] = firstValues.Count / u64size;
            offset += firstValues.Count;

            dataEnd = offset;
            count = 1;
            buffer = newBuffer;
            first.Return();
            first = default;
            materialized = true;
        }
        catch
        {
            ArrayPool<byte>.Shared.Return(newBuffer);
            throw;
        }
        AddMore(in second);
    }

    private void AddMore(in ChunkEntry entry)
    {
        var textLength = entry.Text.Count;
        var valuesLength = entry.Values.Count;

        // Данные растут от головы, мета — от хвоста: данные и мета новой записи должны
        // помещаться, иначе буфер растёт.
        if (dataEnd + textLength + valuesLength > bufferSize - (count + 1) * MetaPerRecord)
            Grow(textLength + valuesLength);

        var span = buffer!.AsSpan(0, bufferSize);
        var ints = MemoryMarshal.Cast<byte, int>(span);
        var offset = dataEnd;

        entry.Text.AsSpan().CopyTo(span.Slice(offset, textLength));
        ints[^(count * 4 + 1)] = offset;
        ints[^(count * 4 + 2)] = textLength;
        offset += textLength;

        entry.Values.AsSpan().CopyTo(span.Slice(offset, valuesLength));
        ints[^(count * 4 + 3)] = offset;
        ints[^(count * 4 + 4)] = entry.ValuesCount;
        offset += valuesLength;
        count++;
        dataEnd = offset;
        ints[0] = count;
        entry.Return();
    }

    /// <summary>Рост буфера: данные — в голову нового, мета — в хвост; смещения в мете абсолютные.</summary>
    private void Grow(int additional)
    {
        var metaBytes = count * MetaPerRecord;
        var needed = dataEnd + additional + (count + 1) * MetaPerRecord;
        var newBuffer = ArrayPool<byte>.Shared.Rent(Math.Max(bufferSize * 2, needed));
        buffer!.AsSpan(0, dataEnd).CopyTo(newBuffer);
        buffer.AsSpan(bufferSize - metaBytes, metaBytes).CopyTo(newBuffer.AsSpan(newBuffer.Length - metaBytes));
        ArrayPool<byte>.Shared.Return(buffer!);
        buffer = newBuffer;
        bufferSize = newBuffer.Length;
    }

    public IEnumerator<(Memory<byte>, long)> GetEnumerator()
    {
        if (count == 0)
            throw new InvalidOperationException("Empty merge group");

        if (!materialized)
        {
            // Одиночная запись: значения развёрнуты парсером — отдаём из её сегментов без копий.
            var text = first.Text;
            for (var i = 0; i < first.ValuesCount; i++)
                yield return (text.AsMemory(), BinaryPrimitives.ReadInt64LittleEndian(first.Values.AsSpan(i * u64size)));
            yield break;
        }

        // Группа: значения записей упорядочены внутри каждой записи — сливаем по возрастанию.
        PriorityQueue<(int Record, long Value), long> heap = new(count);
        var next = ArrayPool<int>.Shared.Rent(count);
        try
        {
            for (var r = 0; r < count; r++)
            {
                var valuesCount = Ints[^(4 * r + 4)];
                if (valuesCount > 0)
                {
                    var value = BinaryPrimitives.ReadInt64LittleEndian(buffer!.AsSpan(Ints[^(4 * r + 3)], u64size));
                    heap.Enqueue((r, value), value);
                    next[r] = 1;
                }
                else
                {
                    next[r] = 0;
                }
            }
            while (heap.TryDequeue(out var item, out _))
            {
                var (r, value) = item;
                yield return (buffer!.AsMemory(Ints[^(4 * r + 1)], Ints[^(4 * r + 2)]), value);

                var valuesStart = Ints[^(4 * r + 3)];
                var valuesCount = Ints[^(4 * r + 4)];
                if (next[r] < valuesCount)
                {
                    var nextValue = BinaryPrimitives.ReadInt64LittleEndian(buffer!.AsSpan(valuesStart + next[r] * u64size, u64size));
                    heap.Enqueue((r, nextValue), nextValue);
                    next[r]++;
                }
            }
        }
        finally
        {
            ArrayPool<int>.Shared.Return(next);
        }
    }

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
}

/// <summary>Пул групп: после записи группа возвращает все арендованные массивы и очищается.</summary>
sealed class MergeGroupPoolPolicy : IPooledObjectPolicy<MergeGroup>
{
    public MergeGroup Create() => new();

    public bool Return(MergeGroup obj)
    {
        obj.Reset();
        return true;
    }
}
