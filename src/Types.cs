using System.Buffers;
using System.Globalization;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using Microsoft.Extensions.ObjectPool;

/// <summary>Ключ чанка: смещение и длина байт текста в нативном буфере чанка.</summary>
readonly record struct BufferString(int Offset, int Length);

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

/// <summary>Запись чанка: ключ и sort key (BufferString в нативных буферах чанка) и его числа.</summary>
readonly record struct ChunkEntry<T>(BufferString Key, BufferString SortKey, List<T> Values);

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

    /// <summary>Явный sort key текста (вычисляется при split, сравнения в хипе — побайтовые).</summary>
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

/// <summary>Политика пула словарей чанка: каждый словарь создаётся со своим BytesComparer.</summary>
sealed class LookupPoolPolicy<T> : IPooledObjectPolicy<Dictionary<BufferString, List<T>>>
{
    public Dictionary<BufferString, List<T>> Create() => new(new BytesComparer());

    public bool Return(Dictionary<BufferString, List<T>> obj)
    {
        obj.Clear();
        return true;
    }
}

/// <summary>
/// Чанк: нативный буфер, на который ссылаются ключи Dictionary через BufferString, сам Dictionary
/// (байты текста → список чисел), нативный буфер sort key и ссылки на пулы; Dispose освобождает всё.
/// </summary>
sealed unsafe class Chunk<T> : IDisposable where T : INumber<T>
{
    private readonly byte* buffer;
    private readonly int bufferLength;
    private readonly ObjectPool<List<T>> listPool;
    private readonly ObjectPool<Dictionary<BufferString, List<T>>> dictionaryPool;
    // Один dictionaryPool.Get() на все три поля: comparer и alternate lookup обязаны
    // принадлежать тому же словарю, что и lookup.
    private readonly Dictionary<BufferString, List<T>> lookup = null!;
    private readonly BytesComparer comparer = null!;
    private readonly Dictionary<BufferString, List<T>>.AlternateLookup<ReadOnlySpan<byte>> alt;

    private byte* sortKeyBuffer;
    public int Id { get; set; }
    private int sortKeyLength;
    private bool lookupReleased;
    private List<List<T>>? valueLists;

    public Chunk(
        ObjectPool<List<T>> listPool,
        ObjectPool<Dictionary<BufferString, List<T>>> dictionaryPool,
        int bufferSize)
    {
        buffer = (byte*)NativeMemory.AlignedAlloc((nuint)bufferSize, 64);
        bufferLength = bufferSize;
        this.listPool = listPool;
        this.dictionaryPool = dictionaryPool;
        // Один dictionaryPool.Get() на все три поля: comparer и alternate lookup обязаны
        // принадлежать тому же словарю, что и lookup.
        lookup = dictionaryPool.Get();
        comparer = (BytesComparer)lookup.Comparer;
        comparer.Bind(buffer, bufferLength);
        alt = lookup.GetAlternateLookup<ReadOnlySpan<byte>>();
    }

    /// <summary>Нативный буфер ключей чанка.</summary>
    public Span<byte> BufferSpan => new(buffer, bufferLength);

    public Dictionary<BufferString, List<T>> Lookup => lookup;

    /// <summary>Компарер ключей, привязанный к буферу чанка (используется и для ordinal-сортировки).</summary>
    public BytesComparer Comparer => comparer;

    /// <summary>Указатель на нативный буфер sort key.</summary>
    public byte* SortKeyPtr => sortKeyBuffer;

    /// <summary>Текущая ёмкость буфера sort key.</summary>
    public int SortKeyLength => sortKeyLength;

    /// <summary>Ключи, упорядоченные по строковому сравнению; заполняется фазой сортировки.</summary>
    public List<ChunkEntry<T>>? Sorted { get; set; }

    /// <summary>Байт занято под ключи в буфере чанка.</summary>
    public int Used => comparer.Offset;

    public void Dispose()
    {
        ReleaseLookup();
        ReleaseSortKeys();
        // Списки значений нужны до конца записи чанка — возвращаем только здесь.
        if (valueLists is not null)
        {
            foreach (var list in valueLists)
            {
                list.Clear();
                listPool.Return(list);
            }
            valueLists = null;
        }
        Sorted = null;
    }

    /// <summary>Возвращает словарь в пул до завершения жизни чанка (после сортировки словарь не нужен —
    /// остаются только буфер с ключами, Sorted и списки значений до конца записи).</summary>
    public void ReleaseLookup()
    {
        if (lookupReleased)
            return;
        lookupReleased = true;
        valueLists = [.. lookup.Values];
        dictionaryPool.Return(lookup);
    }

    /// <summary>Выделяет нативный буфер под sort keys чанка.</summary>
    public void AllocateSortKeys(int length)
    {
        sortKeyBuffer = (byte*)NativeMemory.AlignedAlloc((nuint)Math.Max(16, length), 64);
        sortKeyLength = (int)Math.Max(16, length);
    }

    /// <summary>Перевыделает буфер sort key вдвое большим; сегменты уже записанных ключей
    /// становятся невалидны — вызывающий обязан перезаписать ключи заново.</summary>
    public void GrowSortKeys()
    {
        var old = sortKeyBuffer;
        sortKeyLength *= 2;
        sortKeyBuffer = (byte*)NativeMemory.AlignedAlloc((nuint)sortKeyLength, 64);
        if (old is not null)
            NativeMemory.AlignedFree(old);
    }

    public Span<byte> SortKeySpan => new(sortKeyBuffer, sortKeyLength);

    public Span<byte> SortKeySpanFrom(int offset) => new(sortKeyBuffer + offset, sortKeyLength - offset);

    /// <summary>Компарер sort key, привязанный к текущему нативному буферу чанка.</summary>
    public unsafe NativeSegmentComparer CreateSortKeyComparer() => new(sortKeyBuffer);

    /// <summary>Срез нативного буфера sort key.</summary>
    public unsafe Span<byte> GetSortKeySlice(int offset, int length) => new(sortKeyBuffer + offset, length);

    /// <summary>Освобождает нативный буфер sort key после записи чанка.</summary>
    public void ReleaseSortKeys()
    {
        if (sortKeyBuffer is null)
            return;
        NativeMemory.AlignedFree(sortKeyBuffer);
        sortKeyBuffer = null;
    }

    internal void AddItemToLookup(ReadOnlySpan<byte> key, T x)
    {
        ref var list = ref CollectionsMarshal.GetValueRefOrAddDefault(alt, key, out var exist);
        if (!exist)
        {
            list = listPool.Get();
        }
        list!.Add(x);
    }
}
