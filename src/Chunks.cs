using System.Buffers;
using System.Globalization;
using System.Numerics;
using System.Numerics.Tensors;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using Microsoft.Extensions.ObjectPool;
using NV = (int Number, long Value);

/// <summary>Ключ чанка: смещение и длина байт текста в буфере чанка.</summary>
readonly record struct BufferString(int Offset, int Length);

/// <summary>Значение словаря чанка: последовательный номер ключа и количество значений у ключа.</summary>
readonly record struct KeyInfo(int Number, int Count);

/// <summary>Политика словарей чанка: каждый словарь создаётся со своим BytesComparer.</summary>
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
/// Сравнитель ключей Dictionary чанка: при добавлении нового ключа копирует альтернативу (байты текста)
/// в привязанный буфер и возвращает BufferString — ключи не аллоцируют отдельные массивы.
/// Ключи сравниваются как массивы байт (без учёта кодировки). При нехватке места в буфере бросает исключение.
/// </summary>
sealed class BytesComparer : IEqualityComparer<BufferString>,
    IAlternateEqualityComparer<ReadOnlySpan<byte>, BufferString>,
    IComparer<BufferString>
{
    private byte[] buffer = [];
    private int bufferLength;

    /// <summary>Сколько байт занято под тексты в буфере текущего чанка.</summary>
    public int Offset { get; private set; }

    /// <summary>Сколько байт с конца буфера занято парами (номер, значение).</summary>
    public int TailReserved { get; private set; }

    /// <summary>Резервирует место под очередную пару (номер, значение) в конце буфера.</summary>
    public void ReservePair() => TailReserved += 16;

    /// <summary>Свободно байт между текстами (от начала) и парами (с конца).</summary>
    public int Free => bufferLength - Offset - TailReserved;

    /// <summary>Подключает буфер нового чанка и сбрасывает позиции записи.</summary>
    public void Bind(byte[] buffer, int length)
    {
        this.buffer = buffer;
        bufferLength = length;
        Offset = 0;
        TailReserved = 0;
    }
    public BufferString Create(ReadOnlySpan<byte> alternate)
    {
        if (alternate.Length > Free)
            throw new OverflowException(
                $"Not enough space in the chunk buffer: need {alternate.Length} bytes, have {Free - alternate.Length}");
        alternate.CopyTo(buffer.AsSpan(Offset));
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

    private ReadOnlySpan<byte> Slice(BufferString key) => buffer.AsSpan(key.Offset, key.Length);
}

/// <summary>
/// Порядок ключей для culture-режима: сравнивает номера ключей по срезам их sort key
/// (лежат в sortKeys подряд, границы — offsets/lengths).
/// </summary>
sealed class SortKeyComparer(byte[] sortKeys, byte[] offsets, byte[] lengths) : IComparer<int>
{
    public int Compare(int x, int y)
    {
        if (x == y)
            return 0;

        var o = MemoryMarshal.Cast<byte, int>(offsets);
        var l = MemoryMarshal.Cast<byte, int>(lengths);
        return sortKeys.AsSpan(o[x], l[x])
            .SequenceCompareTo(sortKeys.AsSpan(o[y], l[y]));
    }
}

/// <summary>
/// MemoryPool поверх ArrayPool: сегменты Pipe берутся из общего Scratch-пула. Аренда больше
/// максимального размера пула (рост буферов групп и кэша sort key) — обычный массив мимо пула.
/// </summary>
sealed class ScratchMemoryPool(ArrayPool<byte> pool, int maxPooled) : MemoryPool<byte>
{
    public override int MaxBufferSize => int.MaxValue;

    public override IMemoryOwner<byte> Rent(int minBufferSize = -1)
    {
        if (minBufferSize <= maxPooled)
            return new Owner(pool.Rent(minBufferSize), pool, skipReturn: false);
        return new Owner(new byte[minBufferSize], pool, skipReturn: true);
    }

    protected override void Dispose(bool disposing) { }

    private sealed class Owner(byte[] array, ArrayPool<byte> pool, bool skipReturn) : IMemoryOwner<byte>
    {
        public Memory<byte> Memory => array;

        public void Dispose()
        {
            if (!skipReturn)
                pool.Return(array);
        }
    }
}

/// <summary>
/// Все пулы чанков: текстовые буферы и словари (долгоживущие, по одному на чанк) и общий
/// Scratch (sort key, буферы сериализации/сжатия, сегменты Pipe чтения). Создаётся в Split
/// с размерами от параметров и передаётся чанкам.
/// </summary>
sealed class ChunkPools
{
    public ChunkPools(int bufferSize, int maxConcurrent)
    {
        BufferSize = bufferSize;
        // Собственный Scratch: maxArrayLength = степень двойки >= bufferSize, чтобы Rent(BufferSize)
        // попадал в верхний bucket. maxArraysPerBucket с запасом покрывает сегменты Pipe
        // (pause 16МБ / 1МБ сегмент = ~16 шт только на чтение входного файла) + sort key + S/C.
        // Запас x2: culture-режим держит в Scratch конкатенированные чары всех ключей (~2x текстовых байт).
        var scratchMaxLength = (int)Math.Min(int.MaxValue, 2L * BitOperations.RoundUpToPowerOf2((uint)bufferSize));
        Scratch = ArrayPool<byte>.Create(
            scratchMaxLength,
            Math.Max(32, maxConcurrent * 8));
        Memory = new ScratchMemoryPool(Scratch, scratchMaxLength);
        Lookups = new DefaultObjectPool<Dictionary<BufferString, KeyInfo>>(new LookupPoolPolicy());
    }

    public int BufferSize { get; }

    // Собственный Scratch: sort key, буферы сериализации/сжатия и текстовые буферы чанков.
    public ArrayPool<byte> Scratch { get; }

    // MemoryPool поверх Scratch: сегменты Pipe чтения (>1МБ) тоже из Scratch.
    public MemoryPool<byte> Memory { get; }

    public ObjectPool<Dictionary<BufferString, KeyInfo>> Lookups { get; }
}

/// <summary>
/// Стадия накопления чанка: один буфер — тексты ключей пишутся с начала (ключи Dictionary ссылаются
/// в него через BufferString), пары (номер ключа, значение) — с конца; словарь из пула.
/// Переход в SortedChunk — методом Sort.
/// </summary>
sealed class Chunk : IDisposable
{
    // Один Lookups.Get() на все три поля: comparer и alternate lookup обязаны
    // принадлежать тому же словарю, что и lookup.
    private byte[] buffer;
    private readonly ChunkPools pools;
    private readonly Dictionary<BufferString, KeyInfo> lookup;
    private readonly BytesComparer comparer;
    private readonly Dictionary<BufferString, KeyInfo>.AlternateLookup<ReadOnlySpan<byte>> alt;
    private int valueCount;
    private bool lookupReleased;

    public Chunk(ChunkPools pools)
    {
        this.pools = pools;
        lookup = pools.Lookups.Get();
        comparer = (BytesComparer)lookup.Comparer;
        buffer = pools.Scratch.Rent(pools.BufferSize);
        comparer.Bind(buffer, pools.BufferSize);
        alt = lookup.GetAlternateLookup<ReadOnlySpan<byte>>();
    }

    public Dictionary<BufferString, KeyInfo> Lookup => lookup;


    /// <summary>
    /// Добавляет строку: текст (пишется с начала буфера) и значение (пара (номер, значение) —
    /// с конца). Новый текст получает очередной номер.
    /// </summary>
    public void Add(ReadOnlySpan<byte> text, long value)
    {
        comparer.ReservePair();
        // Create проверяет место только под новый текст: чанк из одних дубликатов уже собранных
        // ключей иначе растит хвост пар до текстов и затирает их. Исключение — обычный сигнал
        // переполнения: SplitToChunks отдаст чанк и повторит строку в новом.
        if (comparer.Free < 0)
            throw new OverflowException("Not enough space in the chunk buffer for a value pair");
        var keyCount = lookup.Count;
        ref var info = ref CollectionsMarshal.GetValueRefOrAddDefault(alt, text, out var exist);
        if (!exist)
        {
            // Номер ключа — 0-based (до вставки он равен числу уже находящихся в словаре ключей).
            info = new KeyInfo(keyCount, 1);
        }
        else
        {
            info = info with { Count = info.Count + 1 };
        }

        valueCount++;
        var slots = MemoryMarshal.Cast<byte, NV>(buffer.AsSpan());
        slots[^valueCount] = (info.Number, value);
    }

    /// <summary>
    /// Переход накопление → сортировка: CSR значений из пар, декодирование ключей и sort key
    /// (culture), порядок записи. Все вспомогательные буферы берутся из Scratch — аллокаций нет.
    /// Словарь возвращается в пул, текстовый буфер передаётся в SortedChunk.
    /// Ordinal-режим: чары не декодируются и sort key не вычисляются, порядок — по байтам текста.
    /// </summary>
    public SortedChunk Sort(
        Encoding encoding,
        CompareInfo compareInfo,
        CompareOptions compareOptions,
        bool ordinal,
        CancellationToken ct)
    {
        var keyCount = lookup.Count;
        var parallelOptions = new ParallelOptions { CancellationToken = ct };

        var valueCountBuffer = pools.Scratch.Rent(keyCount * Unsafe.SizeOf<int>());
        var keyOffsetBuffer = pools.Scratch.Rent(keyCount * Unsafe.SizeOf<int>());
        var keyLengthBuffer = pools.Scratch.Rent(keyCount * Unsafe.SizeOf<int>());
        var valueCounts = MemoryMarshal.Cast<byte, int>(valueCountBuffer.AsSpan())[..keyCount];
        var keyOffsets = MemoryMarshal.Cast<byte, int>(keyOffsetBuffer.AsSpan())[..keyCount];
        var keyLengths = MemoryMarshal.Cast<byte, int>(keyLengthBuffer.AsSpan())[..keyCount];

        // Чары и их границы по номерам ключей — только для culture-режима.
        Span<char> chars = [];
        Span<int> charOffsets = [], charLengths = [];
        byte[]? charBuffer = null, charOffsetBuffer = null, charLengthBuffer = null;
        if (!ordinal)
        {
            // Верхняя граница чаров — сумма по ключам: у ряда кодировок GetMaxCharCount
            // даёт надбавку за вызов, и максимум по всему текстовому буферу её не покрывает.
            var maxChars = 0;
            foreach (var (key, _) in lookup)
                maxChars += encoding.GetMaxCharCount(key.Length);
            charBuffer = pools.Scratch.Rent(checked(maxChars * Unsafe.SizeOf<char>()));
            charOffsetBuffer = pools.Scratch.Rent(keyCount * Unsafe.SizeOf<int>());
            charLengthBuffer = pools.Scratch.Rent(keyCount * Unsafe.SizeOf<int>());
            chars = MemoryMarshal.Cast<byte, char>(charBuffer.AsSpan())[..maxChars];
            charOffsets = MemoryMarshal.Cast<byte, int>(charOffsetBuffer.AsSpan())[..keyCount];
            charLengths = MemoryMarshal.Cast<byte, int>(charLengthBuffer.AsSpan())[..keyCount];
        }

        var currentOffset = 0;
        var buffer = DetachBuffer();
        foreach (var (key, info) in lookup)
        {
            valueCounts[info.Number] = info.Count;
            keyOffsets[info.Number] = key.Offset;
            keyLengths[info.Number] = key.Length;
            if (!ordinal)
            {
                charOffsets[info.Number] = currentOffset;
                var len = encoding.GetChars(buffer.AsSpan(key.Offset, key.Length), chars[currentOffset..]);
                charLengths[info.Number] = len;
                currentOffset += len;
            }
        }
        // Словарь возвращается в пул, текстовый буфер передаётся следующей стадии.
        ReleaseLookup();

        // CSR значений: стартовые смещения из префиксных сумм счётчиков, scatter пар из хвоста
        // буфера (движением стартов, без отдельного массива курсоров), затем вычитание счётчиков
        // возвращает смещения к стартам диапазонов.
        var valueOffsetBuffer = pools.Scratch.Rent(keyCount * Unsafe.SizeOf<int>());
        var valueOffsets = MemoryMarshal.Cast<byte, int>(valueOffsetBuffer.AsSpan())[..keyCount];
        valueOffsets[0] = 0;
        for (var i = 1; i < keyCount; i++)
            valueOffsets[i] = valueOffsets[i - 1] + valueCounts[i - 1];

        var valuesBuffer = pools.Scratch.Rent(checked(valueCount * Unsafe.SizeOf<long>()));
        var values = MemoryMarshal.Cast<byte, long>(valuesBuffer.AsSpan())[..valueCount];
        var source = MemoryMarshal.Cast<byte, NV>(buffer);
        for (int i = 1; i <= valueCount; i++)
        {
            var (number, value) = source[^i];
            values[valueOffsets[number]++] = value;
        }
        TensorPrimitives.Subtract(valueOffsets, valueCounts, valueOffsets);

        byte[]? sortKeyBuffer = null, sortKeyOffsetBuffer = null, sortKeyLengthBuffer = null;
        Span<int> sortKeyOffsets, sortKeyLengths;
        if (!ordinal)
        {
            // Пасс A: длина sort key каждого ключа — параллельно, вход — уже декодированные чары.
            // Спаны в теле нарезаются заново: ref struct'ы нельзя захватить в делегат.
            sortKeyLengthBuffer = pools.Scratch.Rent(keyCount * Unsafe.SizeOf<int>());
            sortKeyLengths = MemoryMarshal.Cast<byte, int>(sortKeyLengthBuffer.AsSpan())[..keyCount];
            Parallel.For(0, keyCount, parallelOptions, i =>
            {
                var lens = MemoryMarshal.Cast<byte, int>(sortKeyLengthBuffer.AsSpan());
                var ch = MemoryMarshal.Cast<byte, char>(charBuffer.AsSpan());
                var co = MemoryMarshal.Cast<byte, int>(charOffsetBuffer.AsSpan());
                var cl = MemoryMarshal.Cast<byte, int>(charLengthBuffer.AsSpan());
                lens[i] = compareInfo.GetSortKeyLength(ch.Slice(co[i], cl[i]), compareOptions);
            });

            // Точный размер под все sort key — сумма длин по ключам (длина конкатенации не годится).
            var totalSortKey = 0;
            for (int i = 0; i < keyCount; i++)
                totalSortKey += sortKeyLengths[i];
            sortKeyBuffer = pools.Scratch.Rent(Math.Max(16, totalSortKey));
            sortKeyOffsetBuffer = pools.Scratch.Rent(keyCount * Unsafe.SizeOf<int>());
            sortKeyOffsets = MemoryMarshal.Cast<byte, int>(sortKeyOffsetBuffer.AsSpan())[..keyCount];
            sortKeyOffsets[0] = 0;
            for (int i = 1; i < keyCount; i++)
                sortKeyOffsets[i] = sortKeyOffsets[i - 1] + sortKeyLengths[i - 1];
        }

        // Параллельно: sort key каждого ключа пишется в свой непересекающийся диапазон буфера
        // (culture), значения внутри диапазона ключа сортируются по возрастанию (оба режима).
        Parallel.For(0, keyCount, parallelOptions, i =>
        {
            if (!ordinal)
            {
                var ch = MemoryMarshal.Cast<byte, char>(charBuffer.AsSpan());
                var co = MemoryMarshal.Cast<byte, int>(charOffsetBuffer.AsSpan());
                var cl = MemoryMarshal.Cast<byte, int>(charLengthBuffer.AsSpan());
                var st = MemoryMarshal.Cast<byte, int>(sortKeyOffsetBuffer.AsSpan());
                var ln = MemoryMarshal.Cast<byte, int>(sortKeyLengthBuffer.AsSpan());
                compareInfo.GetSortKey(ch.Slice(co[i], cl[i]), sortKeyBuffer.AsSpan(st[i], ln[i]), compareOptions);
            }
            var values = MemoryMarshal.Cast<byte, long>(valuesBuffer.AsSpan());
            var starts = MemoryMarshal.Cast<byte, int>(valueOffsetBuffer.AsSpan());
            var counts = MemoryMarshal.Cast<byte, int>(valueCountBuffer.AsSpan());
            values.Slice(starts[i], counts[i]).Sort();
        });

        // Чары нужны только до вычисления sort key — возвращаются в Scratch здесь же.
        if (!ordinal)
        {
            pools.Scratch.Return(charBuffer!);
            pools.Scratch.Return(charOffsetBuffer!);
            pools.Scratch.Return(charLengthBuffer!);
        }

        // Порядок записи: номера ключей по возрастанию sort key (ordinal — по байтам текста);
        // компарер сравнивает срезы по номеру ключа. Сортируем срез [0..keyCount):
        // арендованный буфер длиннее и забит мусором из пула.
        var orderBuffer = pools.Scratch.Rent(keyCount * Unsafe.SizeOf<int>());
        var order = MemoryMarshal.Cast<byte, int>(orderBuffer.AsSpan())[..keyCount];
        for (var i = 0; i < keyCount; i++)
            order[i] = i;
        if (ordinal)
            order.Sort(new SortKeyComparer(buffer, keyOffsetBuffer, keyLengthBuffer));
        else
            order.Sort(new SortKeyComparer(sortKeyBuffer!, sortKeyOffsetBuffer!, sortKeyLengthBuffer!));

        // Sort key-массивы нужны только для сортировки порядка — возвращаются в пулы здесь же.
        if (sortKeyBuffer is not null)
        {
            pools.Scratch.Return(sortKeyBuffer);
            pools.Scratch.Return(sortKeyOffsetBuffer!);
            pools.Scratch.Return(sortKeyLengthBuffer!);
        }

        return new SortedChunk(pools,
            buffer, keyCount, keyOffsetBuffer, keyLengthBuffer,
            valuesBuffer, valueOffsetBuffer, valueCountBuffer,
            orderBuffer);
    }

    /// <summary>Передаёт владение текстовым буфером вызывающему (следующей стадии чанка).</summary>
    public byte[] DetachBuffer()
    {
        var detached = buffer;
        buffer = [];
        return detached;
    }

    /// <summary>Возвращает словарь в пул (после finalize словарь не нужен).</summary>
    public void ReleaseLookup()
    {
        if (lookupReleased)
            return;
        lookupReleased = true;
        pools.Lookups.Return(lookup);
    }

    public void Dispose()
    {
        ReleaseLookup();
        if (buffer.Length > 0)
            pools.Scratch.Return(buffer);
        buffer = [];
    }
}
