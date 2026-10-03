using System.Buffers;
using System.Collections.Concurrent;
using System.Globalization;
using System.IO.Compression;
using System.IO.Pipelines;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks.Dataflow;
using DotMake.CommandLine;
using Microsoft.Extensions.ObjectPool;
using System.Threading.Channels;

/// <summary>
/// Команда split и конвейер разбиения: читает исходник, собирает строки в чанки (текст — ключ
/// словаря, числа — списки), сортирует чанки и пишет отсортированные промежуточные файлы со
/// сжатием zstd. Конвейер со стадиями наперёд (backpressure приходит только от записи на диск):
/// чтение выделенным потоком (sync FileStream → Pipe) → разбор строк → сортировка чанков
/// (TransformBlock) → сериализация + one-shot zstd (TransformBlock) → запись файла (ActionBlock).
/// </summary>
[CliCommand(Parent = typeof(RootCliCommand), Description = "Split a huge file into sorted chunk files, printing their paths to stdout")]
internal partial class SplitCliCommand
{
    // Граница stackalloc для копий текста строки; больше — аренда из пула, чтобы не переполнить стек.
    private const int StackallocByteThreshold = 16 * 1024;

    // Граница stackalloc для декодирующего char-буфера числа.
    private const int StackallocCharThreshold = 512;
    private const int ReadBufferSize = 1 << 20;

    [CliArgument(Description = "Source file to split", ValidationRules = CliValidationRules.ExistingFile)]
    public FileInfo Source { get; set; } = null!;

    [CliOption(Name = "parallel", Description = "Number of chunks sorted in parallel (default: 1)")]
    public int Parallelism { get; set; } = 1;

    [CliOption(Name = "compare-options", Description = "Text comparison: None (default), Ordinal, IgnoreCase, IgnoreNonSpace, IgnoreSymbols, or a combination")]
    public string CompareOptionsName { get; set; } = "None";

    [CliOption(Name = "min-chunk-mb", Description = "Lower bound of the adaptive chunk buffer, MB (default: 32)")]
    public int MinChunkMb { get; set; } = 32;

    [CliOption(Name = "max-chunk-mb", Description = "Upper bound of the adaptive chunk buffer, MB (default: 500)")]
    public int MaxChunkMb { get; set; } = 500;

    [CliOption(Name = "zstd-level", Description = "Zstd compression level for chunk files, 1-22 (default: 9)")]
    public int ZstdLevel { get; set; } = 9;

    // ——— Состояние конвейера; инициализируется Initialize перед прогоном ———
    private Encoding encoding = null!;
    private CompareInfo compareInfo = null!;
    private CompareOptions compareOptions;
    private bool isUtf8;
    private bool isOrdinal;
    private byte[] newLine = null!;
    private byte[] dotSpace = null!;
    private int bufferSize;
    private int windowLog2;
    private int savedChunkCount;
    private ChunkBufferPool bufferPool = null!;
    private readonly ArrayPool<byte> arrayPool = ArrayPool<byte>.Shared;
    private readonly ObjectPool<Dictionary<BufferString, KeyInfo>> dictionaryPool = new DefaultObjectPool<Dictionary<BufferString, KeyInfo>>(new LookupPoolPolicy());

    public Task<int> RunAsync(CliContext context) =>
        RootCliCommand.RunWithSettingsAsync(CompareOptionsName, ExecuteAsync, context);

    private async Task<int> ExecuteAsync(Encoding encoding, CompareOptions compareOptions, CancellationToken ct)
    {
        Channel<FileInfo> files = Channel.CreateUnbounded<FileInfo>();
        var t = SplitAsync(encoding, compareOptions, files.Writer, ct);
        await foreach (var file in files.Reader.ReadAllAsync(ct))
            Console.WriteLine(file.FullName);
        await t;
        return 0;
    }

    /// <summary>Запускает конвейер разбиения с готовыми настройками; используется и командой sort.</summary>
    internal async Task SplitAsync(
        Encoding encoding,
        CompareOptions compareOptions,
        ChannelWriter<FileInfo> files,
        CancellationToken ct)
    {
        this.encoding = encoding;
        this.compareOptions = compareOptions;
        compareInfo = CultureInfo.CurrentCulture.CompareInfo;
        isUtf8 = encoding.WebName == "utf-8";
        isOrdinal = compareOptions == CompareOptions.Ordinal;
        newLine = encoding.GetBytes(Environment.NewLine);
        dotSpace = encoding.GetBytes(". ");
        bufferSize = ComputeBufferSize(Source.Length, MinChunkMb, MaxChunkMb);
        windowLog2 = ComputeWindowLog2(Source.Length, bufferSize);
        bufferPool = new ChunkBufferPool(bufferSize);

        TransformBlock<Chunk, SortedChunk> sorter = new(
            chunk => SortChunk(chunk, ct),
            new ()
            {
                MaxDegreeOfParallelism = Parallelism,
                BoundedCapacity = Parallelism + 2,
                EnsureOrdered = false,
                CancellationToken = ct,
            });
        TransformBlock<SortedChunk, CompressedChunk> compressor = new(
            CompressChunk,
            new ()
            {
                MaxDegreeOfParallelism = 2,
                BoundedCapacity = 2,
                EnsureOrdered = false,
                CancellationToken = ct,
            });
        ActionBlock<CompressedChunk> saver = new(
            async chunk =>
            {
                WriteChunk(in chunk);
                chunk.Dispose();
                await files.WriteAsync(chunk.File, ct);
            },
            new ()
            {
                MaxDegreeOfParallelism = 1,
                BoundedCapacity = 1,
                EnsureOrdered = false,
                CancellationToken = ct,
            });
        using var sorterToCompressor = sorter.LinkTo(compressor, new DataflowLinkOptions { PropagateCompletion = true });
        using var compressorToSaver = compressor.LinkTo(saver, new DataflowLinkOptions { PropagateCompletion = true });

        // Чтение исходника выделенным потоком: sync FileStream → Pipe (опережение разбора на 8-16 МБ).
        Pipe pipe = new(new PipeOptions(
            minimumSegmentSize: ReadBufferSize,
            resumeWriterThreshold: ReadBufferSize * 8,
            pauseWriterThreshold: ReadBufferSize * 16,
            useSynchronizationContext: false));
        var fill = Task.Factory.StartNew(() => FillPipe(pipe.Writer, ct), ct, TaskCreationOptions.LongRunning, TaskScheduler.Default);

        try
        {
        var lines = pipe.Reader.Split(newLine, ct);
        var completed = await SplitToChunks(lines, ct)
            .AllAsync((c, ct) => new ValueTask<bool>(sorter.SendAsync(c, ct)), ct)
            .ConfigureAwait(false);
            if (!completed)
                throw new InvalidOperationException("Sorter stopped accepting chunks (pipeline faulted)");
            sorter.Complete();
            await Task.WhenAll(fill, sorter.Completion, compressor.Completion, saver.Completion);
        }
        finally
        {
            // Любая ошибка конвейера доводится до верхнего уровня: потребители канала
            // дочитывают уже записанные файлы и выходят, исключение уходит через await задачи.
            files.TryComplete();
        }
    }

    /// <summary>Адаптивный буфер чанка: большой файл — крупные чанки (меньше накладных расходов merge), маленький — мелкие (больше параллелизма).</summary>
    private static int ComputeBufferSize(long fileLength, int minChunkMb, int maxChunkMb) =>
        (int)Math.Clamp(fileLength / (Environment.ProcessorCount * 2L), minChunkMb * 1024L * 1024L, maxChunkMb * 1024L * 1024L);

    /// <summary>Логарифм окна zstd: не больше размера чанка и в бюджете ~2ГБ на все параллельные декодеры merge.</summary>
    private static int ComputeWindowLog2(long fileLength, int chunkBufferBytes)
    {
        var chunkCount = Math.Max(1, fileLength / chunkBufferBytes);
        var perStream = 2L * 1024 * 1024 * 1024 / chunkCount;
        return Math.Clamp(BitOperations.Log2((uint)Math.Max(1, perStream)), 20, 31);
    }

    /// <summary>Продюсер чтения файла: sync-чтение блоками readSize в пайп (выделенный поток).</summary>
    private void FillPipe(PipeWriter writer, CancellationToken ct)
    {
        try
        {
            using FileStream fileStream = new(
                Source.FullName, 
                FileMode.Open, 
                FileAccess.Read, 
                FileShare.Read,
                bufferSize: 0, 
                FileOptions.SequentialScan);
            while (true)
            {
                ct.ThrowIfCancellationRequested();
                var span = writer.GetSpan(ReadBufferSize);
                var n = fileStream.Read(span);
                if (n == 0) break;
                writer.Advance(n);
                // Синхронное завершение — забираем результат без Task; иначе блокируем выделенный поток.
                var vt = writer.FlushAsync(ct);
                FlushResult r = vt.IsCompleted ? vt.Result : vt.AsTask().GetAwaiter().GetResult();
                if (r.IsCompleted) break;
            }
            writer.Complete();
        }
        catch (Exception ex)
        {
            writer.Complete(ex);
        }
    }

    /// <summary>
    /// Собирает строки в чанки: байты текста строки — ключ Dictionary (ArraySegment в буфере чанка,
    /// сравнение по байтам), число — элемент списка. Повторный текст добавляет число в список существующего ключа.
    /// </summary>
    internal async IAsyncEnumerable<Chunk> SplitToChunks(IAsyncEnumerable<ReadOnlySequence<byte>> lines, [EnumeratorCancellation] CancellationToken ct = default)
    {
        Chunk chunk = new(dictionaryPool, bufferPool);
        await foreach (var line in lines.WithCancellation(ct).ConfigureAwait(false))
        {
            SequenceReader<byte> reader = new(line);
            if (!reader.TryReadTo(out ReadOnlySpan<byte> number, dotSpace)) throw new FormatException($"Line does not contain '{Encoding.UTF8.GetString(dotSpace)}' delimiter");
            var x = ParseNumber(number);

            // До конца строки — текст; пытаемся добавить его в словарь чанка.
            var text = reader.Sequence.Slice(reader.Position);
            try
            {
                AddToChunk(chunk, x, text);
                continue;
            }
            catch (OverflowException)
            {
            }
            // Предыдущий словарь переполнился, выдаем словарь наружу, создаем новый и добавляем текст в него.

            yield return chunk;
            chunk = new(dictionaryPool, bufferPool);
            AddToChunk(chunk, x, text);
        }
        if (chunk is { Lookup.Count: 0 }) chunk.Dispose();
        else yield return chunk;
    }

    /// <summary>
    /// Преобразует байты числа в long. Для utf-8 — парсинг байт напрямую,
    /// для остальных кодировок — сперва декодирование в char.
    /// </summary>
    private long ParseNumber(ReadOnlySpan<byte> bytes) =>
        isUtf8 ? long.Parse(bytes, null) : DecodeAndParse(bytes);

    private long DecodeAndParse(ReadOnlySpan<byte> bytes)
    {
        Span<char> chars = bytes.Length <= StackallocCharThreshold
            ? stackalloc char[StackallocCharThreshold]
            : new char[encoding.GetMaxCharCount(bytes.Length)];
        var written = encoding.GetChars(bytes, chars);
        return long.Parse(chars[..written], null);
    }

    private void AddToChunk(Chunk chunk, long x, ReadOnlySequence<byte> text)
    {
        byte[]? rented = null;
        try
        {
            Span<byte> buffer = text.Length <= StackallocByteThreshold
                ? stackalloc byte[StackallocByteThreshold]
                : (rented = arrayPool.Rent((int)text.Length));
            text.CopyTo(buffer);
            chunk.Add(buffer[..(int)text.Length], x);
        }
        finally
        {
            if (rented is not null) arrayPool.Return(rented);
        }
    }

    /// <summary>
    /// Переход накопление → сортировка: CSR значений из пар, параллельные пассы sort key
    /// (локальный нативный буфер, освобождается здесь же), порядок записи. Словарь возвращается
    /// в пул, текстовый буфер передаётся в SortedChunk.
    /// </summary>
    private unsafe SortedChunk SortChunk(Chunk chunk, CancellationToken ct)
    {
        var keyCount = chunk.KeyCount;
        var maxKeyBytes = chunk.TextLengths.Count > 0 ? chunk.TextLengths.Max() : 0;
        var maxChars = encoding.GetMaxCharCount(maxKeyBytes);
        // Смещения текстов: тексты лежат в буфере подряд в порядке номеров.
        var textOffsets = new int[keyCount + 1];
        for (var i = 0; i < keyCount; i++)
            textOffsets[i + 1] = textOffsets[i] + chunk.TextLengths[i];

        int[] sortKeyOffsets = [];
        int[] sortKeyLengths = [];
        byte* sortKeyBuffer = null;
        ParallelOptions parallelOptions = new() { CancellationToken = ct };

        if (!isOrdinal)
        {
            // Пасс A (параллельно): точная длина sort key каждого ключа.
            var sortKeyLengthsByNumber = new int[keyCount];
            Parallel.For(0, keyCount, parallelOptions,
                () => new char[maxChars],
                (i, _, buf) =>
                {
                    var w = encoding.GetChars(chunk.BufferSpan.Slice(textOffsets[i], chunk.TextLengths[i]), buf);
                    sortKeyLengthsByNumber[i] = compareInfo.GetSortKeyLength(buf.AsSpan(0, w), compareOptions);
                    return buf;
                },
                _ => { });

            var totalSortKey = 0;
            for (var i = 0; i < keyCount; i++)
                totalSortKey += sortKeyLengthsByNumber[i];
            sortKeyBuffer = (byte*)NativeMemory.AlignedAlloc((nuint)Math.Max(16, totalSortKey), 64);
            sortKeyOffsets = new int[keyCount + 1];
            for (var i = 0; i < keyCount; i++)
                sortKeyOffsets[i + 1] = sortKeyOffsets[i] + sortKeyLengthsByNumber[i];
            sortKeyLengths = sortKeyLengthsByNumber;
        }

        (var values, var csr) = chunk.BuildCsr();

        // Параллельно: sort key каждого ключа (culture) и сортировка его CSR-диапазона.
        Parallel.For(0, keyCount, parallelOptions,
            () => new char[maxChars],
            (i, _, buf) =>
            {
                if (!isOrdinal)
                {
                    var w = encoding.GetChars(chunk.BufferSpan[textOffsets[i]..textOffsets[i + 1]], buf);
                    compareInfo.GetSortKey(buf.AsSpan(0, w), new Span<byte>(sortKeyBuffer + sortKeyOffsets[i], sortKeyLengths[i]), compareOptions);
                }
                Array.Sort(values, csr[i], csr[i + 1] - csr[i]);
                return buf;
            },
            _ => { });

        // Порядок записи: номера ключей по возрастанию sort key (ordinal — по байтам текста).
        var order = Enumerable.Range(0, keyCount).ToArray();
        var indices = new BufferString[keyCount];
        for (var i = 0; i < keyCount; i++)
            indices[i] = isOrdinal
                ? new BufferString(textOffsets[i], textOffsets[i + 1] - textOffsets[i])
                : new BufferString(sortKeyOffsets[i], sortKeyLengths[i]);
        if (isOrdinal)
            Array.Sort(indices, order, chunk.Comparer);
        else
            Array.Sort(indices, order, new NativeSegmentComparer(sortKeyBuffer));

        // Sort key нужен только для сортировки порядка — освобождается здесь же.
        if (sortKeyBuffer is not null)
            NativeMemory.AlignedFree(sortKeyBuffer);

        // Словарь возвращается в пул, текстовый буфер передаётся следующей стадии.
        chunk.ReleaseLookup();
        return new SortedChunk(chunk.DetachBuffer(), bufferPool, values, csr, textOffsets, order);
    }

    /// <summary>
    /// Переход сортировка → сжатие: сериализация в буфер S точного размера (текстовый буфер
    /// возвращается в пул сразу после неё), one-shot zstd S → C.
    /// </summary>
    private unsafe CompressedChunk CompressChunk(SortedChunk sorted)
    {
        var index = Interlocked.Increment(ref savedChunkCount) - 1;
        FileInfo chunkFile = new(Path.ChangeExtension(Source.FullName, $".chunk-{index:D3}{Path.GetExtension(Source.FullName)}"));
        byte* s = null;
        byte* c = null;
        try
        {
            var size = sorted.SerializedSize();
            s = (byte*)NativeMemory.AlignedAlloc((nuint)Math.Max(16, size), 64);
            sorted.Serialize(new Span<byte>(s, checked((int)size)));
            sorted.Dispose(); // текстовый буфер возвращается в пул до сжатия

            var cLength = checked((int)ZstandardEncoder.GetMaxCompressedLength(size));
            c = (byte*)NativeMemory.AlignedAlloc((nuint)cLength, 64);
            using ZstandardEncoder encoder = new(new ZstandardCompressionOptions
            {
                Quality = ZstdLevel,
                WindowLog = Math.Min(windowLog2, Math.Clamp(BitOperations.Log2((uint)size) + 1, 20, 31)),
                EnableLongDistanceMatching = true,
            });
            // Один вызов на весь чанк (isFinalBlock дописывает footer кадра).
            var status = encoder.Compress(new ReadOnlySpan<byte>(s, checked((int)size)), new Span<byte>(c, cLength),
                out var consumed, out var written, isFinalBlock: true);
            if (status != OperationStatus.Done || consumed != size)
                throw new InvalidOperationException($"zstd compression incomplete: {status}, {consumed} of {size}");
            NativeMemory.AlignedFree(s);
            s = null;
            return new CompressedChunk(chunkFile, c, written);
        }
        catch
        {
            if (s is not null) NativeMemory.AlignedFree(s);
            if (c is not null) NativeMemory.AlignedFree(c);
            throw;
        }
    }

    /// <summary>Пишет сжатый чанк на диск одним Write (преаллокация точного размера — без расширения по кусочкам).</summary>
    private static unsafe void WriteChunk(in CompressedChunk chunk)
    {
        using FileStream fileStream = new(
            chunk.File.FullName, 
            FileMode.Create, FileAccess.Write, FileShare.None,
            bufferSize: 0, 
            FileOptions.SequentialScan
            );
        fileStream.SetLength(chunk.Length);
        fileStream.Write(new ReadOnlySpan<byte>(chunk.Buffer, chunk.Length));
    }
}
