using System.Buffers;
using System.Globalization;
using System.Numerics;
using System.Runtime.InteropServices;
using System.Text;
using Microsoft.Extensions.ObjectPool;

/// <summary>Общие настройки конвейера сортировки.</summary>
internal static class SortConstants
{
    /// <summary>Порог необработанных байт для сброса PipeWriter в файл (Writer сам не пишет до FlushAsync).</summary>
    public const int FlushThreshold = 1024 * 1024;
}

/// <summary>
/// Сравнитель ключей Dictionary чанка: при добавлении нового ключа копирует альтернативу (байты текста)
/// в буфер чанка и возвращает сегмент этого буфера, поэтому ключи не аллоцируют отдельные массивы.
/// Ключи сравниваются как массивы байт (без учёта кодировки). При нехватке места в буфере бросает исключение.
/// </summary>
sealed class BytesComparer : IEqualityComparer<ArraySegment<byte>>,
    IAlternateEqualityComparer<ReadOnlySpan<byte>, ArraySegment<byte>>
{
    private byte[] buffer = [];

    /// <summary>Сколько байт занято в буфере текущего чанка.</summary>
    public int Offset { get; private set; }

    /// <summary>Подключает буфер нового чанка и сбрасывает позицию записи.</summary>
    public void Bind(byte[] buffer)
    {
        this.buffer = buffer;
        Offset = 0;
    }

    public ArraySegment<byte> Create(ReadOnlySpan<byte> alternate)
    {
        if (Offset + alternate.Length > buffer.Length)
            throw new OverflowException(
                $"Not enough space in the chunk buffer: need {alternate.Length} bytes, have {buffer.Length - Offset}");
        var segment = new ArraySegment<byte>(buffer, Offset, alternate.Length);
        alternate.CopyTo(segment);
        Offset += alternate.Length;
        return segment;
    }

    public bool Equals(ReadOnlySpan<byte> alternate, ArraySegment<byte> existing) =>
        alternate.SequenceEqual(existing);

    public bool Equals(ArraySegment<byte> x, ArraySegment<byte> y) =>
        x.AsSpan().SequenceEqual(y.AsSpan());

    public int GetHashCode(ReadOnlySpan<byte> alternate) =>
        (int)System.IO.Hashing.XxHash32.HashToUInt32(alternate);

    public int GetHashCode(ArraySegment<byte> segment) =>
        (int)System.IO.Hashing.XxHash32.HashToUInt32(segment.AsSpan());
}

/// <summary>Сравнивает явные sort key (байтовые сегменты в общем массиве) побайтово: порядок идентичен CompareInfo.Compare.</summary>
sealed class SortKeyComparer : IComparer<ArraySegment<byte>>
{
    public static readonly SortKeyComparer Instance = new();

    public int Compare(ArraySegment<byte> x, ArraySegment<byte> y) =>
        x.AsSpan().SequenceCompareTo(y.AsSpan());
}

/// <summary>
/// Запись чанка: ключ (байты текста в буфере чанка), его sort key (байтовый сегмент в общем массиве чанка;
/// default при ordinal-режиме — ключом служат сами байты текста) и числа.
/// </summary>
readonly record struct ChunkEntry<T>(ArraySegment<byte> Key, ArraySegment<byte> SortKey, List<T> Values);

/// <summary>
/// Запись промежуточного файла при чтении: текст и sort key — сегменты арендованных из ArrayPool массивов,
/// значения — список из ObjectPool. Владелец обязан вызвать Dispose, когда запись больше не нужна
/// (MergeSorted делает это сразу после продвижения потока мимо записи).
/// </summary>
sealed class ReadChunkEntry<T> : IDisposable where T : IBinaryInteger<T>
{
    private readonly ObjectPool<List<T>> listPool;

    /// <summary>Байты текста в кодировке файла.</summary>
    public ArraySegment<byte> Text { get; }

    /// <summary>Явный sort key текста (вычисляется один раз, сравнения в хипе — побайтовые).</summary>
    public ArraySegment<byte> SortKey { get; }

    public List<T> Values { get; }

    public ReadChunkEntry(ArraySegment<byte> text, ArraySegment<byte> sortKey, List<T> values, ObjectPool<List<T>> listPool)
    {
        Text = text;
        SortKey = sortKey;
        Values = values;
        this.listPool = listPool;
    }

    public void Dispose()
    {
        ArrayPool<byte>.Shared.Return(Text.Array!);
        // В ordinal-режиме SortKey указывает на тот же массив, что и Text, — вернуть один раз.
        if (!ReferenceEquals(SortKey.Array, Text.Array))
            ArrayPool<byte>.Shared.Return(SortKey.Array!);
        listPool.Return(Values);
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

/// <summary>Политика пула списков чисел: Return очищает список, сохраняя ёмкость для повторного использования.</summary>
sealed class ListPoolPolicy<T> : IPooledObjectPolicy<List<T>>
{
    public List<T> Create() => [];

    public bool Return(List<T> obj)
    {
        obj.Clear();
        return true;
    }
}

/// <summary>Политика пула словарей чанка: каждый словарь создаётся со своим Utf8BytesComparer.</summary>
sealed class LookupPoolPolicy<T> : IPooledObjectPolicy<Dictionary<ArraySegment<byte>, List<T>>>
{
    public Dictionary<ArraySegment<byte>, List<T>> Create() => new(new BytesComparer());

    public bool Return(Dictionary<ArraySegment<byte>, List<T>> obj)
    {
        obj.Clear();
        return true;
    }
}

/// <summary>
/// Чанк: буфер, на который ссылаются ключи Dictionary через ArraySegment, сам Dictionary (байты текста → List&lt;long&gt;)
/// и ссылки на пулы; Dispose возвращает всё в пулы.
/// </summary>
sealed class Chunk<T>: IDisposable where T:INumber<T>  
{
    readonly byte[] buffer;
    readonly Dictionary<ArraySegment<byte>, List<T>> lookup;
    readonly BytesComparer comparer;

    readonly ArrayPool<byte> arrayPool;
    readonly ObjectPool<List<T>> listPool;
    readonly ObjectPool<Dictionary<ArraySegment<byte>, List<T>>> dictionaryPool;
    readonly Dictionary<ArraySegment<byte>, List<T>>.AlternateLookup<ReadOnlySpan<byte>> alt;
    
    public Chunk(
        ArrayPool<byte> arrayPool,
        ObjectPool<List<T>> listPool,
        ObjectPool<Dictionary<ArraySegment<byte>, List<T>>> dictionaryPool,
        int bufferSize)
    {
        this.arrayPool = arrayPool;
        this.listPool = listPool;
        this.dictionaryPool = dictionaryPool;
        buffer = arrayPool.Rent(bufferSize);
        lookup = dictionaryPool.Get();
        comparer = (BytesComparer)lookup.Comparer;
        comparer.Bind(buffer);
        alt = lookup.GetAlternateLookup<ReadOnlySpan<byte>>();
    }

    public byte[] Buffer => buffer;
    public Dictionary<ArraySegment<byte>, List<T>> Lookup => lookup;

    public List<ChunkEntry<T>>? Sorted {get; set; }
    
    public void Dispose()
    {
        foreach (var list in lookup.Values) listPool.Return(list);
        dictionaryPool.Return(lookup);
        arrayPool.Return(buffer);
    }

    internal void AddItemToLookup(Span<byte> buffer, T x)
    {
        ref var list = ref CollectionsMarshal.GetValueRefOrAddDefault(alt, buffer, out bool exist);
        if (!exist)
        {
            list = listPool.Get();
        }
        list!.Add(x);
    }


}
