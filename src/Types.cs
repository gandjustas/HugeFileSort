using System.Buffers;
using System.Collections.Concurrent;
using System.Globalization;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using Microsoft.Extensions.ObjectPool;

/// <summary>Ключ чанка: смещение и длина байт текста в нативном буфере чанка.</summary>
readonly record struct BufferString(int Offset, int Length);

/// <summary>Значение словаря чанка: последовательный номер ключа и количество значений у ключа.</summary>
readonly record struct KeyInfo(int Number, int Count);

/// <summary>
/// Сравнитель ключей Dictionary чанка: при добавлении нового ключа копирует альтернативу (байты текста)
/// в привязанный нативный буфер и возвращает BufferString — ключи не аллоцируют отдельные массивы.
/// Ключи сравниваются как массивы байт (без учёта кодировки). При нехватке места в буфере бросает исключение.
/// </summary>
sealed unsafe class BytesComparer : IEqualityComparer<BufferString>,
    IAlternateEqualityComparer<ReadOnlySpan<byte>, BufferString>,
    IComparer<BufferString>
{
    private byte* buffer;
    private int bufferLength;

    /// <summary>Сколько байт занято в буфере текущего чанка.</summary>
    public int Offset { get; private set; }

    /// <summary>Подключает нативный буфер нового чанка и сбрасывает позицию записи.</summary>
    public void Bind(byte* ptr, int length)
    {
        buffer = ptr;
        bufferLength = length;
        Offset = 0;
    }

    public BufferString Create(ReadOnlySpan<byte> alternate)
    {
        if (Offset + alternate.Length > bufferLength)
            throw new OverflowException(
                $"Not enough space in the chunk buffer: need {alternate.Length} bytes, have {bufferLength - Offset}");
        alternate.CopyTo(new Span<byte>(buffer + Offset, alternate.Length));
        Offset += alternate.Length;
        return new BufferString(Offset - alternate.Length, alternate.Length);
    }

    public bool Equals(ReadOnlySpan<byte> alternate, BufferString existing) =>
        alternate.SequenceEqual(Slice(existing));

    public bool Equals(BufferString x, BufferString y) =>
        x.Offset == y.Offset && x.Length == y.Length || Slice(x).SequenceEqual(Slice(y));

    public int Compare(BufferString x, BufferString y) =>
        Slice(x).SequenceCompareTo(Slice(y));

    public int GetHashCode(ReadOnlySpan<byte> alternate) =>
        (int)System.IO.Hashing.XxHash32.HashToUInt32(alternate);

    public int GetHashCode(BufferString key) =>
        (int)System.IO.Hashing.XxHash32.HashToUInt32(Slice(key));

    private ReadOnlySpan<byte> Slice(BufferString key) => new(buffer + key.Offset, key.Length);
}

/// <summary>Сравнивает BufferString-сегменты (offset/length) в привязанном нативном буфере побайтово.</summary>
sealed unsafe class NativeSegmentComparer(byte* buffer) : IComparer<BufferString>
{
    public int Compare(BufferString x, BufferString y)
    {
        if (x.Offset == y.Offset && x.Length == y.Length)
            return 0;
        ReadOnlySpan<byte> sx = new(buffer + x.Offset, x.Length);
        ReadOnlySpan<byte> sy = new(buffer + y.Offset, y.Length);
        return sx.SequenceCompareTo(sy);
    }
}

/// <summary>
/// Пул нативных текстовых буферов чанка фиксированного размера (64-байтное выравнивание).
/// Все чанки прогона имеют одинаковый размер — буферы переиспользуются между стадиями
/// накопления, сортировки и сериализации вместо реаллокации.
/// </summary>
sealed unsafe class ChunkBufferPool
{
    private readonly ConcurrentBag<nuint> buffers = [];

    public ChunkBufferPool(int bufferSize) => BufferSize = bufferSize;

    public int BufferSize { get; }

    public byte* Rent() => buffers.TryTake(out var buffer)
        ? (byte*)buffer
        : (byte*)NativeMemory.AlignedAlloc((nuint)BufferSize, 64);

    public void Return(byte* buffer) => buffers.Add((nuint)buffer);
}

/// <summary>
/// Запись промежуточного файла при merge: текст и sort key — сегменты буферов пакета (Batch),
/// значения — срез общего массива значений пакета. Dispose уменьшает счётчик пакета: последний
/// возврат возвращает буферы пакета в пулы (MergeSorted делает это после потребления группы).
/// </summary>
sealed class ReadChunkEntry(
    Batch batch,
    ArraySegment<byte> text,
    ArraySegment<byte> sortKey,
    long[] values,
    int valuesStart,
    int valuesCount) : IDisposable
{
    /// <summary>Байты текста в кодировке файла.</summary>
    public ArraySegment<byte> Text { get; } = text;

    /// <summary>Явный sort key текста (ordinal: алиас текста; культура: сегмент буфера пакета).</summary>
    public ArraySegment<byte> SortKey { get; } = sortKey;

    /// <summary>Общий массив значений пакета (срез — по ValuesStart/ValuesCount).</summary>
    public long[] ValuesArray { get; } = values;

    public int ValuesStart { get; } = valuesStart;

    public int ValuesCount { get; } = valuesCount;

    public void Dispose() => batch.Release();
}

/// <summary>
/// Пакет записей одного чанка при merge: арендованный буфер текстов (записи ссылаются в него
/// zero-copy), буфер sort key (в ordinal-режиме не выделяется) и общий массив значений.
/// Пакет живёт, пока merge не потребит все записи: каждая ReadChunkEntry.Dispose уменьшает
/// счётчик, последний возвращает массивы в пулы.
/// </summary>
sealed class Batch(byte[] buffer, byte[]? sortKeyBuffer, int entryCount)
{
    private readonly byte[] buffer = buffer;
    private readonly byte[]? sortKeyBuffer = sortKeyBuffer;
    private int remaining = entryCount;

    /// <summary>Записи пакета; заполняется после создания пакета.</summary>
    public ReadChunkEntry[] Entries { get; set; } = [];

    /// <summary>Вызывается при потреблении очередной записи; последний возврат — буферы в пулы.</summary>
    public void Release()
    {
        if (Interlocked.Decrement(ref remaining) == 0)
        {
            ArrayPool<byte>.Shared.Return(buffer);
            if (sortKeyBuffer is not null)
                ArrayPool<byte>.Shared.Return(sortKeyBuffer);
        }
    }
}

/// <summary>Метаданные записи пакета: смещение/длина текста, диапазон значений в общем массиве.</summary>
readonly record struct BatchEntryMeta(int TextOffset, int TextLength, int ValuesStart, int ValuesCount);

/// <summary>Сжатый чанк в памяти: файл назначения и нативный буфер сжатых данных (освобождает писатель).</summary>
unsafe struct CompressedChunk(FileInfo file, byte* buffer, int length) : IDisposable
{
    public readonly FileInfo File = file;
    public byte* Buffer = buffer;
    public readonly int Length = length;

    public void Dispose()
    {
        if (Buffer is not null)
            NativeMemory.AlignedFree(Buffer);
        Buffer = null;
    }
}

/// <summary>Сравнивает записи промежуточных файлов только по sort key (побайтово).</summary>
sealed class ChunkEntryComparer : IComparer<ReadChunkEntry>
{
    public static readonly ChunkEntryComparer Instance = new();

    public int Compare(ReadChunkEntry? x, ReadChunkEntry? y)
    {
        if (x is null) return y is null ? 0 : -1;
        if (y is null) return 1;
        return x.SortKey.AsSpan().SequenceCompareTo(y.SortKey.AsSpan());
    }
}

/// <summary>Политика пула словарей чанка: каждый словарь создаётся со своим BytesComparer.</summary>
sealed class LookupPoolPolicy : IPooledObjectPolicy<Dictionary<BufferString, KeyInfo>>
{
    public Dictionary<BufferString, KeyInfo> Create() => new(new BytesComparer());

    public bool Return(Dictionary<BufferString, KeyInfo> obj)
    {
        obj.Clear();
        return true;
    }
}

/// <summary>
/// Стадия накопления чанка: нативный буфер текстов (берётся из пула и передаётся дальше по
/// DetachBuffer), словарь из пула (байты текста → номер ключа) и список пар (номер, значение).
/// </summary>
sealed unsafe class Chunk : IDisposable
{
    private byte* buffer;
    private readonly int bufferLength;
    private readonly ObjectPool<Dictionary<BufferString, KeyInfo>> dictionaryPool;
    private readonly ChunkBufferPool bufferPool;
    private readonly Dictionary<BufferString, KeyInfo> lookup;
    private readonly BytesComparer comparer;
    private readonly Dictionary<BufferString, KeyInfo>.AlternateLookup<ReadOnlySpan<byte>> alt;
    private readonly List<(int Number, long Value)> pairs = [];
    private readonly List<int> textLengths = [];
    private bool lookupReleased;

    public Chunk(
        ObjectPool<Dictionary<BufferString, KeyInfo>> dictionaryPool,
        ChunkBufferPool bufferPool)
    {
        this.dictionaryPool = dictionaryPool;
        this.bufferPool = bufferPool;
        // Один dictionaryPool.Get() на все три поля: comparer и alternate lookup обязаны
        // принадлежать тому же словарю, что и lookup.
        lookup = dictionaryPool.Get();
        comparer = (BytesComparer)lookup.Comparer;
        buffer = bufferPool.Rent();
        bufferLength = bufferPool.BufferSize;
        comparer.Bind(buffer, bufferLength);
        alt = lookup.GetAlternateLookup<ReadOnlySpan<byte>>();
    }

    /// <summary>Нативный буфер текстов ключей (лежат подряд в порядке номеров).</summary>
    public Span<byte> BufferSpan => new(buffer, bufferLength);

    public Dictionary<BufferString, KeyInfo> Lookup => lookup;

    /// <summary>Компарер ключей, привязанный к буферу чанка (используется и для ordinal-сортировки).</summary>
    public BytesComparer Comparer => comparer;

    /// <summary>Длины текстов ключей по номерам.</summary>
    public IReadOnlyList<int> TextLengths => textLengths;

    /// <summary>Число уникальных ключей в чанке.</summary>
    public int KeyCount => lookup.Count;

    /// <summary>Байт занято под ключи в буфере чанка.</summary>
    public int Used => comparer.Offset;

    /// <summary>
    /// Строит CSR-массив значений: границы по номерам ключей (префикс-сумма количеств)
    /// и scatter пар в свои диапазоны.
    /// </summary>
    public (long[] Values, int[] CsrOffsets) BuildCsr()
    {
        var k = textLengths.Count;
        var csr = new int[k + 1];
        foreach (var (number, _) in pairs)
            csr[number + 1]++;
        for (var i = 1; i <= k; i++)
            csr[i] += csr[i - 1];
        var values = GC.AllocateUninitializedArray<long>(pairs.Count);
        var fill = new int[k];
        foreach (var (number, value) in pairs)
            values[csr[number] + fill[number]++] = value;
        return (values, csr);
    }

    /// <summary>
    /// Добавляет строку: текст (уже скопирован в буфер чанка) и значение.
    /// Новый текст получает очередной номер; пара (номер, значение) — в общий список.
    /// </summary>
    public void Add(ReadOnlySpan<byte> text, long value)
    {
        ref var info = ref CollectionsMarshal.GetValueRefOrAddDefault(alt, text, out var exist);
        if (!exist)
        {
            info = new KeyInfo(textLengths.Count, 1);
            textLengths.Add(text.Length);
        }
        else
        {
            info = info with { Count = info.Count + 1 };
        }
        pairs.Add((info.Number, value));
    }

    /// <summary>Передаёт владение нативным буфером вызывающему (следующей стадии чанка).</summary>
    public byte* DetachBuffer()
    {
        var detached = buffer;
        buffer = null;
        return detached;
    }

    /// <summary>Возвращает словарь в пул (после finalize словарь не нужен).</summary>
    public void ReleaseLookup()
    {
        if (lookupReleased)
            return;
        lookupReleased = true;
        dictionaryPool.Return(lookup);
    }

    public void Dispose()
    {
        ReleaseLookup();
        if (buffer is not null)
        {
            bufferPool.Return(buffer);
            buffer = null;
        }
    }
}

/// <summary>
/// Стадия отсортированного чанка: нативный буфер текстов (принят от Chunk), CSR-массив значений,
/// границы значений и текстов, порядок записи. SerializedSize/Serialize готовят данные к сжатию;
/// Dispose возвращает текстовый буфер в пул.
/// </summary>
sealed unsafe class SortedChunk : IDisposable
{
    private byte* buffer;
    private readonly ChunkBufferPool bufferPool;
    private readonly long[] values;
    private readonly int[] csrOffsets;
    private readonly int[] textOffsets;
    private readonly int[] order;

    public SortedChunk(
        byte* buffer,
        ChunkBufferPool bufferPool,
        long[] values,
        int[] csrOffsets,
        int[] textOffsets,
        int[] order)
    {
        this.buffer = buffer;
        this.bufferPool = bufferPool;
        this.values = values;
        this.csrOffsets = csrOffsets;
        this.textOffsets = textOffsets;
        this.order = order;
    }

    /// <summary>Срез нативного буфера текстов.</summary>
    public Span<byte> GetTextSlice(int offset, int length) => new(buffer + offset, length);

    /// <summary>Точный несжатый размер сериализованного чанка (все числа — LEB128 varint).</summary>
    public long SerializedSize()
    {
        long total = 0;
        foreach (var i in order)
        {
            var textLen = textOffsets[i + 1] - textOffsets[i];
            var count = csrOffsets[i + 1] - csrOffsets[i];
            total += VarintSize((ulong)textLen) + textLen + VarintSize((ulong)count);
            long previous = 0;
            for (var j = csrOffsets[i]; j < csrOffsets[i + 1]; j++)
            {
                total += VarintSize(ulong.CreateChecked(values[j] - previous));
                previous = values[j];
            }
        }
        return total;
    }

    /// <summary>
    /// Сериализует чанк: varint(длина текста), текст, varint(количество чисел), varint(разности
    /// чисел) — для каждого ключа в порядке Order. Sort key в файл не пишутся — merge
    /// пересчитывает их пакетами.
    /// </summary>
    public void Serialize(Span<byte> dst)
    {
        var pos = 0;
        foreach (var i in order)
        {
            var textLen = textOffsets[i + 1] - textOffsets[i];
            pos = Extensions.WriteVarint(dst, pos, (ulong)textLen);
            GetTextSlice(textOffsets[i], textLen).CopyTo(dst.Slice(pos));
            pos += textLen;
            var start = csrOffsets[i];
            var count = csrOffsets[i + 1] - start;
            pos = Extensions.WriteVarint(dst, pos, (ulong)count);
            long previous = 0;
            for (var j = start; j < start + count; j++)
            {
                pos = Extensions.WriteVarint(dst, pos, ulong.CreateChecked(values[j] - previous));
                previous = values[j];
            }
        }
        if (pos != dst.Length)
            throw new InvalidOperationException($"Chunk serialized size mismatch: {pos} != {dst.Length}");
    }

    public void Dispose()
    {
        if (buffer is not null)
        {
            bufferPool.Return(buffer);
            buffer = null;
        }
    }

    private static int VarintSize(ulong value) => 1 + (int)(BitOperations.Log2(value | 1) / 7);
}
