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
    private readonly ArrayPool<byte> arrayPool = ArrayPool<byte>.Shared;
    private readonly ObjectPool<Dictionary<BufferString, KeyInfo>> dictionaryPool = new DefaultObjectPool<Dictionary<BufferString, KeyInfo>>(new LookupPoolPolicy());

    public Task<int> RunAsync(CliContext context) =>
        RootCliCommand.RunWithSettingsAsync(CompareOptionsName, ExecuteAsync, context);

    private async Task<int> ExecuteAsync(Encoding encoding, CompareOptions compareOptions, CancellationToken ct)
    {
        var files = await SplitAsync(encoding, compareOptions, ct);
        foreach (var file in files)
            Console.WriteLine(file.FullName);
        return 0;
    }

    /// <summary>Запускает конвейер разбиения с готовыми настройками; используется и командой sort.</summary>
    internal async Task<List<FileInfo>> SplitAsync(Encoding encoding, CompareOptions compareOptions, CancellationToken ct)
    {
        Initialize(encoding, compareOptions);
        var files = new ConcurrentQueue<FileInfo>();

        var sorter = new TransformBlock<Chunk, Chunk>(
            chunk =>
            {
                SortChunk(chunk, ct);
                return chunk;
            },
            new ExecutionDataflowBlockOptions
            {
                MaxDegreeOfParallelism = Parallelism,
                BoundedCapacity = Parallelism + 2,
                EnsureOrdered = false,
                CancellationToken = ct,
            });
        var compressor = new TransformBlock<Chunk, CompressedChunk>(
            CompressChunk,
            new ExecutionDataflowBlockOptions
            {
                MaxDegreeOfParallelism = 2,
                BoundedCapacity = 2,
                EnsureOrdered = false,
                CancellationToken = ct,
            });
        // Последняя стадия — ActionBlock: у него нет выходной очереди, поэтому BoundedCapacity
        // считает только вход (у TransformBlock непотреблённый выход тоже занимает лимит и
        // без связанного получателя блокирует стадию после первого чанка).
        var saver = new ActionBlock<CompressedChunk>(
            chunk =>
            {
                WriteChunk(in chunk);
                chunk.Dispose();
                files.Enqueue(chunk.File);
            },
            new ExecutionDataflowBlockOptions
            {
                MaxDegreeOfParallelism = 1,
                BoundedCapacity = 1,
                EnsureOrdered = false,
                CancellationToken = ct,
            });
        var sorterToCompressor = sorter.LinkTo(compressor, new DataflowLinkOptions { PropagateCompletion = true });
        var compressorToSaver = compressor.LinkTo(saver, new DataflowLinkOptions { PropagateCompletion = true });

        // Чтение исходника выделенным потоком: sync FileStream → Pipe (опережение разбора на 8-16 МБ).
        var pipe = new Pipe(new PipeOptions(
            minimumSegmentSize: 1 << 20,
            pauseWriterThreshold: 16 << 20,
            resumeWriterThreshold: 8 << 20,
            useSynchronizationContext: false));
        var fill = Task.Factory.StartNew(() => FillPipe(pipe.Writer, ct), ct, TaskCreationOptions.LongRunning, TaskScheduler.Default);

        Exception? loopError = null;
        try
        {
            await foreach (var chunk in SplitToChunks(pipe.Reader.Split(newLine, ct), bufferSize, ct).ConfigureAwait(false))
            {
                if (!await sorter.SendAsync(chunk, ct).ConfigureAwait(false))
                {
                    // Конвейер остановился (сбой стадии) — буфер чанка больше никому не нужен.
                    chunk.Dispose();
                    break;
                }
            }
        }
        catch (Exception ex)
        {
            loopError = ex;
        }
        sorter.Complete();

        try
        {
            await sorter.Completion.ConfigureAwait(false);
            await compressor.Completion.ConfigureAwait(false);
            await saver.Completion.ConfigureAwait(false);
        }
        finally
        {
            sorterToCompressor.Dispose();
            compressorToSaver.Dispose();
        }

        try
        {
            await fill.ConfigureAwait(false);
        }
        catch (Exception ex) when (loopError is not null)
        {
            // Первая ошибка — из цикла разбора; исключение fill теряется намеренно
            _ = ex;
        }

        if (loopError is not null)
            throw new InvalidOperationException("Failed to split the source file", loopError);

        List<FileInfo> result = [];
        while (files.TryDequeue(out var file))
            result.Add(file);
        return result;
    }

    /// <summary>
    /// Инициализирует состояние конвейера из опций команды и настроек кодировки/сравнения.
    /// Вызывается перед каждым прогоном (и командой probe-split — без запуска конвейера).
    /// </summary>
    internal void Initialize(Encoding encoding, CompareOptions compareOptions)
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
            using var fileStream = new FileStream(Source.FullName, FileMode.Open, FileAccess.Read, FileShare.Read,
                bufferSize: 0, FileOptions.SequentialScan);
            while (true)
            {
                ct.ThrowIfCancellationRequested();
                var span = writer.GetSpan(1 << 20);
                var n = fileStream.Read(span);
                if (n == 0) break;
                writer.Advance(n);
                var r = writer.FlushAsync(ct).AsTask().GetAwaiter().GetResult();
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
    internal async IAsyncEnumerable<Chunk> SplitToChunks(IAsyncEnumerable<ReadOnlySequence<byte>> lines, int bufferSize, [EnumeratorCancellation] CancellationToken ct = default)
    {
        Chunk chunk = new(dictionaryPool, bufferSize);
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
            chunk = new(dictionaryPool, bufferSize);
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
    /// Finalize чанка: точные длины sort key по словарю, CSR-массив значений (scatter пар),
    /// параллельно — sort key по ключам и сортировка CSR-диапазонов; порядок записи — по sort key.
    /// Ordinal-режим: sort key не вычисляются, порядок — по байтам текста.
    /// </summary>
    private void SortChunk(Chunk chunk, CancellationToken ct)
    {
        var keyCount = chunk.KeyCount;
        var maxKeyBytes = chunk.TextLengths.Count > 0 ? chunk.TextLengths.Max() : 0;
        var maxChars = encoding.GetMaxCharCount(maxKeyBytes);
        // Смещения текстов: тексты лежат в буфере подряд в порядке номеров.
        var textOffsets = new int[keyCount + 1];
        for (var i = 0; i < keyCount; i++)
            textOffsets[i + 1] = textOffsets[i] + chunk.TextLengths[i];
        chunk.TextOffsets = textOffsets;

        int[] sortKeyOffsets = [];
        int[] sortKeyLengths = [];
        var parallelOptions = new ParallelOptions { CancellationToken = ct };

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
            chunk.AllocateSortKeys(totalSortKey);
            sortKeyOffsets = new int[keyCount + 1];
            for (var i = 0; i < keyCount; i++)
                sortKeyOffsets[i + 1] = sortKeyOffsets[i] + sortKeyLengthsByNumber[i];
            sortKeyLengths = sortKeyLengthsByNumber;
            chunk.SortKeyOffsets = sortKeyOffsets;
            chunk.SortKeyLengths = sortKeyLengths;
        }

        // CSR значений: границы по номерам ключей и scatter пар.
        var values = chunk.BuildCsr();
        var csr = chunk.CsrOffsets!;

        // Параллельно: sort key каждого ключа (culture) и сортировка его CSR-диапазона.
        var csrValues = chunk.Values!;
        Parallel.For(0, keyCount, parallelOptions,
            () => new char[maxChars],
            (i, _, buf) =>
            {
                if (!isOrdinal)
                {
                    var w = encoding.GetChars(chunk.BufferSpan.Slice(textOffsets[i], textOffsets[i + 1] - textOffsets[i]), buf);
                    compareInfo.GetSortKey(buf.AsSpan(0, w), chunk.GetSortKeySlice(sortKeyOffsets[i], sortKeyLengths[i]), compareOptions);
                }
                Array.Sort(csrValues, csr[i], csr[i + 1] - csr[i]);
                return buf;
            },
            _ => { });

        // Порядок записи: номера ключей по возрастанию sort key (ordinal — по байтам текста).
        var order = Enumerable.Range(0, keyCount).ToArray();
        if (isOrdinal)
        {
            var texts = new BufferString[keyCount];
            for (var i = 0; i < keyCount; i++)
                texts[i] = new BufferString(textOffsets[i], textOffsets[i + 1] - textOffsets[i]);
            Array.Sort(texts, order, chunk.Comparer);
        }
        else
        {
            var sortKeys = new BufferString[keyCount];
            for (var i = 0; i < keyCount; i++)
                sortKeys[i] = new BufferString(sortKeyOffsets[i], sortKeyLengths[i]);
            Array.Sort(sortKeys, order, chunk.CreateSortKeyComparer());
        }
        chunk.Order = order;

        // Словарь и sort key больше не нужны — освобождаем до сериализации чанка.
        chunk.ReleaseLookup();
        chunk.ReleaseSortKeys();
    }

    /// <summary>
    /// Сериализует и сжимает чанк: точный размер по ComputeChunkSize → буфер S, один проход по
    /// Order (memcpy текстов + varint-заголовки), one-shot zstd S → C. Буфер текстов чанка
    /// освобождается сразу после сериализации, до сжатия.
    /// </summary>
    private unsafe CompressedChunk CompressChunk(Chunk chunk)
    {
        var index = Interlocked.Increment(ref savedChunkCount) - 1;
        var chunkFile = new FileInfo(Path.ChangeExtension(Source.FullName, $".chunk-{index:D3}{Path.GetExtension(Source.FullName)}"));
        byte* s = null;
        byte* c = null;
        try
        {
            var size = ComputeChunkSize(chunk);
            s = (byte*)NativeMemory.AlignedAlloc((nuint)Math.Max(16, size), 64);
            SerializeChunk(chunk, new Span<byte>(s, checked((int)size)));
            chunk.Dispose(); // тексты больше не нужны — освобождаем до сжатия

            var cLength = checked((int)ZstandardEncoder.GetMaxCompressedLength(size));
            c = (byte*)NativeMemory.AlignedAlloc((nuint)cLength, 64);
            using var encoder = new ZstandardEncoder(new ZstandardCompressionOptions
            {
                Quality = ZstdLevel,
#if NET10_0
                // Backport .NET 11.0.4: свойство переименовано из WindowLog2 в WindowLog.
                WindowLog = Math.Min(windowLog2, Math.Clamp(BitOperations.Log2((uint)size) + 1, 20, 31)),
#else
                WindowLog2 = Math.Min(windowLog2, Math.Clamp(BitOperations.Log2((uint)size) + 1, 20, 31)),
#endif
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

    /// <summary>
    /// Сериализует отсортированный чанк (формат v3, все числа — LEB128 varint):
    /// varint(длина текста), текст, varint(количество чисел), varint(разности чисел) — для каждого
    /// ключа в порядке Order. Sort key в файл не пишутся — merge пересчитывает их пакетами.
    /// </summary>
    private static void SerializeChunk(Chunk chunk, Span<byte> dst)
    {
        var textOffsets = chunk.TextOffsets!;
        var values = chunk.Values!;
        var csr = chunk.CsrOffsets!;
        var buffer = chunk.BufferSpan;
        var pos = 0;
        foreach (var i in chunk.Order!)
        {
            var textLen = textOffsets[i + 1] - textOffsets[i];
            pos = Extensions.WriteVarint(dst, pos, (ulong)textLen);
            buffer.Slice(textOffsets[i], textLen).CopyTo(dst.Slice(pos));
            pos += textLen;
            var start = csr[i];
            var count = csr[i + 1] - start;
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

    /// <summary>Пишет сжатый чанк на диск одним Write (преаллокация точного размера — без расширения по кусочкам).</summary>
    private static unsafe void WriteChunk(in CompressedChunk chunk)
    {
        using var fileStream = new FileStream(chunk.File.FullName, FileMode.Create, FileAccess.Write, FileShare.None,
            bufferSize: 0, FileOptions.SequentialScan);
        fileStream.SetLength(chunk.Length);
        fileStream.Write(new ReadOnlySpan<byte>(chunk.Buffer, chunk.Length));
    }

    /// <summary>Точный несжатый размер файла чанка до сериализации — для аллокации буфера S.</summary>
    private static long ComputeChunkSize(Chunk chunk)
    {
        var textOffsets = chunk.TextOffsets!;
        var csr = chunk.CsrOffsets!;
        var values = chunk.Values!;
        long total = 0;
        foreach (var i in chunk.Order!)
        {
            var textLen = textOffsets[i + 1] - textOffsets[i];
            var count = csr[i + 1] - csr[i];
            total += VarintSize((ulong)textLen) + textLen + VarintSize((ulong)count);
            long previous = 0;
            for (var j = csr[i]; j < csr[i + 1]; j++)
            {
                var delta = ulong.CreateChecked(values[j] - previous);
                total += VarintSize(delta);
                previous = values[j];
            }
        }
        return total;
    }

    private static int VarintSize(ulong value) => 1 + (int)(BitOperations.Log2(value | 1) / 7);
}
