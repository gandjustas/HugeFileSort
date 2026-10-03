using System.Buffers;
using System.Globalization;
using System.IO.Compression;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using Microsoft.Extensions.ObjectPool;

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

/// <summary>Сравнивает BufferString-сегменты (offset/length) в привязанном буфере побайтово.</summary>
sealed class NativeSegmentComparer(byte[] buffer) : IComparer<BufferString>
{
    public int Compare(BufferString x, BufferString y)
    {
        if (x.Offset == y.Offset && x.Length == y.Length)
            return 0;
        return buffer.AsSpan(x.Offset, x.Length).SequenceCompareTo(buffer.AsSpan(y.Offset, y.Length));
    }
}

/// <summary>
/// MemoryPool поверх ArrayPool: сегменты Pipe берутся из общего Scratch-пула.
/// </summary>
sealed class ScratchMemoryPool(ArrayPool<byte> pool) : MemoryPool<byte>
{
    public override int MaxBufferSize => int.MaxValue;

    public override IMemoryOwner<byte> Rent(int minBufferSize = -1) => new Owner(pool.Rent(minBufferSize), pool);

    protected override void Dispose(bool disposing) { }

    private sealed class Owner(byte[] array, ArrayPool<byte> pool) : IMemoryOwner<byte>
    {
        public Memory<byte> Memory => array;

        public void Dispose() => pool.Return(array);
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
        Scratch = ArrayPool<byte>.Create(checked((int)BitOperations.RoundUpToPowerOf2((uint)bufferSize)), Math.Max(32, maxConcurrent * 8));
        Memory = new ScratchMemoryPool(Scratch);
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
    private byte[] buffer = [];
    private readonly int bufferLength;
    private readonly ChunkPools pools;
    private readonly Dictionary<BufferString, KeyInfo> lookup;
    private readonly BytesComparer comparer;
    private readonly Dictionary<BufferString, KeyInfo>.AlternateLookup<ReadOnlySpan<byte>> alt;
    private readonly List<int> textLengths = [];
    private int pairsCount;
    private bool lookupReleased;

    public Chunk(ChunkPools pools)
    {
        this.pools = pools;
        lookup = pools.Lookups.Get();
        comparer = (BytesComparer)lookup.Comparer;
        buffer = pools.Scratch.Rent(pools.BufferSize);
        bufferLength = pools.BufferSize;
        comparer.Bind(buffer, bufferLength);
        alt = lookup.GetAlternateLookup<ReadOnlySpan<byte>>();
    }

    /// <summary>Буфер текстов ключей (лежат подряд в порядке номеров, от начала буфера).</summary>
    public Span<byte> BufferSpan => buffer.AsSpan(0, comparer.Offset);

    public Dictionary<BufferString, KeyInfo> Lookup => lookup;

    /// <summary>Компарер ключей, привязанный к буферу чанка (используется и для ordinal-сортировки).</summary>
    public BytesComparer Comparer => comparer;

    /// <summary>Длины текстов ключей по номерам.</summary>
    public IReadOnlyList<int> TextLengths => textLengths;

    /// <summary>Число уникальных ключей в чанке.</summary>
    public int KeyCount => lookup.Count;

    /// <summary>
    /// Строит CSR-массив значений: границы по номерам ключей (счётчики — из KeyInfo.Count)
    /// и scatter пар, читая хвост буфера в обратном порядке (порядок в диапазоне ключа
    /// не важен — CSR-диапазон сортируется).
    /// </summary>
    public (long[] Values, int[] CsrOffsets) BuildCsr()
    {
        var k = textLengths.Count;
        var csr = new int[k + 1];
        foreach (var info in lookup.Values)
            csr[info.Number + 1] = info.Count;
        for (var i = 1; i <= k; i++)
            csr[i] += csr[i - 1];
        var values = GC.AllocateUninitializedArray<long>(pairsCount);
        var fill = new int[k];
        var slots = MemoryMarshal.Cast<byte, (uint Number, long Value)>(buffer);
        for (var i = 1; i <= pairsCount; i++)
        {
            var (number, value) = slots[^i];
            values[csr[number] + fill[number]++] = value;
        }
        return (values, csr);
    }

    /// <summary>
    /// Добавляет строку: текст (пишется с начала буфера) и значение (пара (номер, значение) —
    /// с конца). Новый текст получает очередной номер.
    /// </summary>
    public void Add(ReadOnlySpan<byte> text, long value)
    {
        comparer.ReservePair();
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

        pairsCount++;
        var slots = MemoryMarshal.Cast<byte, (uint Number, long Value)>(buffer.AsSpan());
        slots[^pairsCount] = ((uint)info.Number, value);
    }

    /// <summary>
    /// Переход накопление → сортировка: CSR значений из пар, параллельные пассы sort key
    /// (буфер из Scratch, возвращается сюда же), порядок записи. Словарь возвращается в пул,
    /// текстовый буфер передаётся в SortedChunk. Ordinal-режим: sort key не вычисляются,
    /// порядок — по байтам текста.
    /// </summary>
    public SortedChunk Sort(
        Encoding encoding,
        CompareInfo compareInfo,
        CompareOptions compareOptions,
        bool ordinal,
        CancellationToken ct)
    {
        var keyCount = lookup.Count;
        var maxKeyBytes = textLengths.Count > 0 ? textLengths.Max() : 0;
        var maxChars = encoding.GetMaxCharCount(maxKeyBytes);
        // Смещения текстов: тексты лежат в буфере подряд в порядке номеров.
        var textOffsets = new int[keyCount + 1];
        for (var i = 0; i < keyCount; i++)
            textOffsets[i + 1] = textOffsets[i] + textLengths[i];

        int[] sortKeyOffsets = [];
        int[] sortKeyLengths = [];
        byte[]? sortKeyBuffer = null;
        ParallelOptions parallelOptions = new() { CancellationToken = ct };

        if (!ordinal)
        {
            // Пасс A (параллельно): точная длина sort key каждого ключа.
            var sortKeyLengthsByNumber = new int[keyCount];
            Parallel.For(0, keyCount, parallelOptions,
                () => new char[maxChars],
                (i, _, buf) =>
                {
                    var w = encoding.GetChars(BufferSpan.Slice(textOffsets[i], textLengths[i]), buf);
                    sortKeyLengthsByNumber[i] = compareInfo.GetSortKeyLength(buf.AsSpan(0, w), compareOptions);
                    return buf;
                },
                _ => { });

            var totalSortKey = 0;
            for (var i = 0; i < keyCount; i++)
                totalSortKey += sortKeyLengthsByNumber[i];
            sortKeyBuffer = pools.Scratch.Rent(Math.Max(16, totalSortKey));
            sortKeyOffsets = new int[keyCount + 1];
            for (var i = 0; i < keyCount; i++)
                sortKeyOffsets[i + 1] = sortKeyOffsets[i] + sortKeyLengthsByNumber[i];
            sortKeyLengths = sortKeyLengthsByNumber;
        }

        (var values, var csr) = BuildCsr();

        // Параллельно: sort key каждого ключа (culture) и сортировка его CSR-диапазона.
        Parallel.For(0, keyCount, parallelOptions,
            () => new char[maxChars],
            (i, _, buf) =>
            {
                if (!ordinal)
                {
                    var w = encoding.GetChars(BufferSpan[textOffsets[i]..textOffsets[i + 1]], buf);
                    compareInfo.GetSortKey(buf.AsSpan(0, w), sortKeyBuffer.AsSpan(sortKeyOffsets[i], sortKeyLengths[i]), compareOptions);
                }
                Array.Sort(values, csr[i], csr[i + 1] - csr[i]);
                return buf;
            },
            _ => { });

        // Порядок записи: номера ключей по возрастанию sort key (ordinal — по байтам текста).
        var order = Enumerable.Range(0, keyCount).ToArray();
        var indices = new BufferString[keyCount];
        for (var i = 0; i < keyCount; i++)
            indices[i] = ordinal
                ? new BufferString(textOffsets[i], textOffsets[i + 1] - textOffsets[i])
                : new BufferString(sortKeyOffsets[i], sortKeyLengths[i]);
        if (ordinal)
            Array.Sort(indices, order, comparer);
        else
            Array.Sort(indices, order, new NativeSegmentComparer(sortKeyBuffer!));

        // Sort key нужен только для сортировки порядка — возвращается в Scratch здесь же.
        if (sortKeyBuffer is not null)
            pools.Scratch.Return(sortKeyBuffer);

        // Словарь возвращается в пул, текстовый буфер передаётся следующей стадии.
        ReleaseLookup();
        return new SortedChunk(pools, DetachBuffer(), values, csr, textOffsets, order);
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

/// <summary>
/// Стадия отсортированного чанка: текстовый буфер (принят от Chunk), CSR-массив значений,
/// границы значений и текстов, порядок записи. SerializedSize/Serialize готовят данные к сжатию,
/// Compress выполняет переход в CompressedChunk; Dispose возвращает текстовый буфер в Scratch.
/// </summary>
sealed class SortedChunk : IDisposable
{
    private byte[] buffer = [];
    private readonly ChunkPools pools;
    private readonly long[] values;
    private readonly int[] csrOffsets;
    private readonly int[] textOffsets;
    private readonly int[] order;

    public SortedChunk(
        ChunkPools pools,
        byte[] buffer,
        long[] values,
        int[] csrOffsets,
        int[] textOffsets,
        int[] order)
    {
        this.pools = pools;
        this.buffer = buffer;
        this.values = values;
        this.csrOffsets = csrOffsets;
        this.textOffsets = textOffsets;
        this.order = order;
    }

    /// <summary>Срез буфера текстов.</summary>
    public Span<byte> GetTextSlice(int offset, int length) => buffer.AsSpan(offset, length);

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

    /// <summary>
    /// Переход сортировка → сжатие: сериализация в буфер S точного размера (текстовый буфер
    /// возвращается в Scratch сразу после неё), one-shot zstd S → C.
    /// </summary>
    public CompressedChunk Compress(FileInfo file, int quality, int maxWindowLog2)
    {
        var size = SerializedSize();
        var s = pools.Scratch.Rent(checked((int)size));
        Serialize(s.AsSpan(0, (int)size));
        Dispose(); // текстовый буфер возвращается в Scratch до сжатия

        var cLength = checked((int)ZstandardEncoder.GetMaxCompressedLength(size));
        var c = pools.Scratch.Rent(cLength);
        using ZstandardEncoder encoder = new(new ZstandardCompressionOptions
        {
            Quality = quality,
            WindowLog = Math.Min(maxWindowLog2, Math.Clamp(BitOperations.Log2((uint)size) + 1, 20, 31)),
            EnableLongDistanceMatching = true,
        });
        // Один вызов на весь чанк (isFinalBlock дописывает footer кадра).
        var status = encoder.Compress(s.AsSpan(0, (int)size), c.AsSpan(0, cLength),
            out var consumed, out var written, isFinalBlock: true);
        if (status != OperationStatus.Done || consumed != size)
            throw new InvalidOperationException($"zstd compression incomplete: {status}, {consumed} of {size}");
        pools.Scratch.Return(s);
        return new CompressedChunk(pools, file, c, written);
    }

    public void Dispose()
    {
        if (buffer.Length > 0)
            pools.Scratch.Return(buffer);
        buffer = [];
    }

    private static int VarintSize(ulong value) => 1 + (int)(BitOperations.Log2(value | 1) / 7);
}

/// <summary>Сжатый чанк в памяти: файл назначения и сжатые данные (буфер возвращает писатель через Dispose).</summary>
sealed class CompressedChunk(ChunkPools pools, FileInfo file, byte[] buffer, int length) : IDisposable
{
    public FileInfo File { get; } = file;

    public byte[] Buffer { get; } = buffer;

    public int Length { get; } = length;

    public void Dispose() => pools.Scratch.Return(Buffer);
}
