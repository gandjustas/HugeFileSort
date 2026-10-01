using System.Buffers;
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
        var sx = new ReadOnlySpan<byte>(buffer + x.Offset, x.Length);
        var sy = new ReadOnlySpan<byte>(buffer + y.Offset, y.Length);
        return sx.SequenceCompareTo(sy);
    }
}

/// <summary>
/// Запись промежуточного файла при merge: текст и sort key — сегменты буферов пакета (Batch),
/// значения — срез общего массива значений пакета. Dispose уменьшает счётчик пакета: последний
/// возврат возвращает буферы пакета в пулы (MergeSorted делает это после потребления группы).
/// </summary>
sealed class ReadChunkEntry<T>(
    Batch<T> batch,
    ArraySegment<byte> text,
    ArraySegment<byte> sortKey,
    T[] values,
    int valuesStart,
    int valuesCount) where T : IBinaryInteger<T>
{
    /// <summary>Байты текста в кодировке файла.</summary>
    public ArraySegment<byte> Text { get; } = text;

    /// <summary>Явный sort key текста (ordinal: алиас текста; культура: сегмент буфера пакета).</summary>
    public ArraySegment<byte> SortKey { get; } = sortKey;

    /// <summary>Общий массив значений пакета (срез — по ValuesStart/ValuesCount).</summary>
    public T[] ValuesArray { get; } = values;

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
sealed class Batch<T>(byte[] buffer, byte[]? sortKeyBuffer, T[] values, int entryCount) where T : IBinaryInteger<T>
{
    private readonly byte[] buffer = buffer;
    private readonly byte[]? sortKeyBuffer = sortKeyBuffer;
    private readonly T[] values = values;
    private int remaining = entryCount;

    /// <summary>Записи пакета; заполняется после создания пакета.</summary>
    public ReadChunkEntry<T>[] Entries { get; set; } = [];

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
unsafe struct CompressedChunk : IDisposable
{
    public readonly FileInfo File;
    public byte* Buffer;
    public readonly int Length;

    public CompressedChunk(FileInfo file, byte* buffer, int length)
    {
        File = file;
        Buffer = buffer;
        Length = length;
    }

    public unsafe void Dispose()
    {
        if (Buffer is not null)
            NativeMemory.AlignedFree(Buffer);
        Buffer = null;
    }
}

/// <summary>Сравнивает записи промежуточных файлов только по sort key (побайтово).</summary>
sealed class ChunkEntryComparer<T> : IComparer<ReadChunkEntry<T>> where T : IBinaryInteger<T>
{
    public static readonly ChunkEntryComparer<T> Instance = new();

    public int Compare(ReadChunkEntry<T>? x, ReadChunkEntry<T>? y)
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
/// Чанк: фаза разбиения — нативный буфер текстов (ключи Dictionary ссылаются через BufferString),
/// словарь (байты текста → номер ключа + количество значений) и один список пар (номер, значение);
/// после finalize — нативный буфер sort key, CSR-массив значений и порядок записи.
/// Dispose освобождает нативную память, массивы — GC.
/// </summary>
sealed unsafe class Chunk<T> : IDisposable where T : IBinaryInteger<T>
{
    private readonly byte* buffer;
    private readonly int bufferLength;
    private readonly ObjectPool<Dictionary<BufferString, KeyInfo>> dictionaryPool;

    public Chunk(
        ObjectPool<Dictionary<BufferString, KeyInfo>> dictionaryPool,
        int bufferSize)
    {
        this.dictionaryPool = dictionaryPool;
        buffer = (byte*)NativeMemory.AlignedAlloc((nuint)bufferSize, 64);
        bufferLength = bufferSize;
        // Один dictionaryPool.Get() на все три поля: comparer и alternate lookup обязаны
        // принадлежать тому же словарю, что и lookup.
        lookup = dictionaryPool.Get();
        comparer = (BytesComparer)lookup.Comparer;
        comparer.Bind(buffer, bufferLength);
        alt = lookup.GetAlternateLookup<ReadOnlySpan<byte>>();
    }
    // Один dictionaryPool.Get() на все три поля: comparer и alternate lookup обязаны
    // принадлежать тому же словарю, что и lookup.
    private readonly Dictionary<BufferString, KeyInfo> lookup;
    private readonly BytesComparer comparer;
    private readonly Dictionary<BufferString, KeyInfo>.AlternateLookup<ReadOnlySpan<byte>> alt;

    private readonly List<(int Number, T Value)> pairs = [];
    private readonly List<int> textLengths = [];

    private byte* sortKeyBuffer;
    private int sortKeyLength;
    private bool lookupReleased;

    /// <summary>Нативный буфер текстов ключей (лежат подряд в порядке номеров).</summary>
    public Span<byte> BufferSpan => new(buffer, bufferLength);

    public Dictionary<BufferString, KeyInfo> Lookup => lookup;

    /// <summary>Компарер ключей, привязанный к буферу чанка (используется и для ordinal-сортировки).</summary>
    public BytesComparer Comparer => comparer;

    /// <summary>Ключи, упорядоченные по строковому сравнению; заполняется фазой сортировки.</summary>
    public int[]? Order { get; internal set; }

    /// <summary>Байт занято под ключи в буфере чанка.</summary>
    public int Used => comparer.Offset;

    /// <summary>CSR-массив значений после finalize.</summary>
    public T[]? Values { get; internal set; }

    /// <summary>Границы диапазонов значений по номерам ключей (длина K+1).</summary>
    public int[]? CsrOffsets { get; internal set; }

    /// <summary>Длины sort key по номерам ключей.</summary>
    public int[]? SortKeyLengths { get; internal set; }

    /// <summary>Смещения sort key по номерам ключей.</summary>
    public int[]? SortKeyOffsets { get; internal set; }

    /// <summary>Смещения текстов по номерам ключей.</summary>
    public int[]? TextOffsets { get; internal set; }

    /// <summary>Длины текстов ключей по номерам.</summary>
    public IReadOnlyList<int> TextLengths => textLengths;

    /// <summary>Число уникальных ключей в чанке.</summary>
    public int KeyCount => lookup.Count;

    /// <summary>
    /// Строит CSR-массив значений: границы по номерам ключей (префикс-сумма количеств)
    /// и scatter пар в свои диапазоны.
    /// </summary>
    public T[] BuildCsr()
    {
        var k = textLengths.Count;
        var csr = new int[k + 1];
        foreach (var (number, _) in pairs)
            csr[number + 1]++;
        for (var i = 1; i <= k; i++)
            csr[i] += csr[i - 1];
        var values = GC.AllocateUninitializedArray<T>(pairs.Count);
        var fill = new int[k];
        foreach (var (number, value) in pairs)
            values[csr[number] + fill[number]++] = value;
        Values = values;
        CsrOffsets = csr;
        return values;
    }

    /// <summary>Срез нативного буфера текстов.</summary>
    public unsafe Span<byte> GetTextSlice(int offset, int length) => new(buffer + offset, length);

    /// <summary>Срез нативного буфера sort key.</summary>
    public unsafe Span<byte> GetSortKeySlice(int offset, int length) => new(sortKeyBuffer + offset, length);

    /// <summary>Компарер sort key, привязанный к текущему нативному буферу sort key.</summary>
    public unsafe NativeSegmentComparer CreateSortKeyComparer() => new(sortKeyBuffer);

    /// <summary>Выделяет нативный буфер sort key точного размера.</summary>
    public unsafe void AllocateSortKeys(int totalLength)
    {
        sortKeyBuffer = (byte*)NativeMemory.AlignedAlloc((nuint)Math.Max(16, totalLength), 64);
        sortKeyLength = totalLength;
    }

    public unsafe Span<byte> SortKeySpan => new(sortKeyBuffer, sortKeyLength);

    public unsafe Span<byte> SortKeySpanFrom(int offset) => new(sortKeyBuffer + offset, sortKeyLength - offset);

    /// <summary>
    /// Добавляет строку: текст (уже скопирован в буфер чанка) и значение.
    /// Новый текст получает очередной номер; пара (номер, значение) — в общий список.
    /// </summary>
    public void Add(ReadOnlySpan<byte> text, T value)
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

    /// <summary>Возвращает словарь в пул до завершения жизни чанка (после finalize словарь не нужен).</summary>
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
        ReleaseSortKeys();
        NativeMemory.AlignedFree(buffer);
    }

    /// <summary>Освобождает нативный буфер sort key после записи чанка.</summary>
    public void ReleaseSortKeys()
    {
        if (sortKeyBuffer is null)
            return;
        NativeMemory.AlignedFree(sortKeyBuffer);
        sortKeyBuffer = null;
    }
}
