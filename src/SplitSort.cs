using System.Buffers;
using System.Globalization;
using System.IO.Pipelines;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks.Dataflow;
using Microsoft.Extensions.ObjectPool;
using ZstdSharp;

/// <summary>
/// Фаза разбиения: читает исходный файл, собирает строки в чанки (текст — ключ словаря, числа — списки),
/// сортирует чанки параллельно (TPL Dataflow) и пишет отсортированные промежуточные файлы с компрессией zstd.
/// </summary>
class SplitSort<T>(
    FileInfo source,
    Encoding encoding,
    CompareOptions compareOptions,
    int sortParallelism,
    int minChunkMb,
    int maxChunkMb,
    int flushMb,
    int zstdLevel) where T : IBinaryInteger<T>
{
    // Граница stackalloc для копий текста строки; больше — аренда из пула, чтобы не переполнить стек.
    private const int StackallocByteThreshold = 16 * 1024;

    // Граница stackalloc для декодирующего char-буфера числа.
    private const int StackallocCharThreshold = 512;

    readonly FileInfo source = source;
    readonly Encoding encoding = encoding;
    readonly byte[] newLine = encoding.GetBytes(Environment.NewLine);
    readonly byte[] dotSpace = encoding.GetBytes(". ");
    readonly CompareInfo compareInfo = CultureInfo.CurrentCulture.CompareInfo;
    readonly CompareOptions compareOptions = compareOptions;
    readonly bool isUtf8 = encoding.WebName == "utf-8";
    readonly bool isOrdinal = compareOptions == CompareOptions.Ordinal;
    readonly int bufferSize = ComputeBufferSize(source.Length, minChunkMb, maxChunkMb);
    readonly int flushThreshold = flushMb * 1024 * 1024;
    // Буфер файлового потока записи — чуть меньше порога сброса PipeWriter,
    // чтобы данные уходили в файл сквозь двойную буферизацию большими блоками.
    readonly int writeStreamBufferSize = Math.Clamp(flushMb * 1024 * 1024 - 64 * 1024, 64 * 1024, 1024 * 1024);
    readonly int zstdLevel = zstdLevel;
    readonly ArrayPool<byte> arrayPool = ArrayPool<byte>.Shared;
    readonly ObjectPool<Dictionary<BufferString, KeyInfo>> dictionaryPool = new DefaultObjectPool<Dictionary<BufferString, KeyInfo>>(new LookupPoolPolicy());

    /// <summary>Адаптивный буфер чанка: большой файл — крупные чанки (меньше накладных расходов merge), маленький — мелкие (больше параллелизма).</summary>
    private static int ComputeBufferSize(long fileLength, int minChunkMb, int maxChunkMb) =>
        (int)Math.Clamp(fileLength / (Environment.ProcessorCount * 2L), minChunkMb * 1024L * 1024L, maxChunkMb * 1024L * 1024L);

    /// <summary>
    /// Прогоняет конвейер разбиения и возвращает пути созданных промежуточных файлов (порядок не важен).
    /// Конвейер: цикл строк (продюсер) → TransformBlock сортировки чанков (параллельно)
    /// → TransformBlock записи на диск (zstd) → BufferBlock имён файлов.
    /// </summary>
    public async Task<List<FileInfo>> RunAsync(CancellationToken ct)
    {
        var sorter = new TransformBlock<Chunk<T>, Chunk<T>>(
            async chunk =>
            {
                await SortChunk(chunk, ct).ConfigureAwait(false);
                return chunk;
            },
            new ExecutionDataflowBlockOptions
            {
                MaxDegreeOfParallelism = sortParallelism,
                BoundedCapacity = sortParallelism * 2,
                CancellationToken = ct,
            });
        var saver = new TransformBlock<Chunk<T>, FileInfo>(
            async chunk =>
            {
                var file = await SaveChunk(chunk, ct).ConfigureAwait(false);
                chunk.Dispose();
                return file;
            },
            new ExecutionDataflowBlockOptions
            {
                MaxDegreeOfParallelism = 2,
                BoundedCapacity = 4,
                CancellationToken = ct,
            });
        var fileNames = new BufferBlock<FileInfo>(new DataflowBlockOptions { CancellationToken = ct });
        var sorterToSaver = sorter.LinkTo(saver, new DataflowLinkOptions { PropagateCompletion = true });
        var saverToNames = saver.LinkTo(fileNames, new DataflowLinkOptions { PropagateCompletion = true });

        // Чтение файла выделенным продюсером через Pipe: FileStream без внутренней буферизации
        // и с SequentialScan, большие пороги позволяют читателю уходить вперёд на 8-16 МБ.
        var pipe = new Pipe(new PipeOptions(
            minimumSegmentSize: 1 << 20,
            pauseWriterThreshold: 16 << 20,
            resumeWriterThreshold: 8 << 20,
            useSynchronizationContext: false));

        using var fileStream = new FileStream(source.FullName, FileMode.Open, FileAccess.Read, FileShare.Read,
            bufferSize: 0, FileOptions.SequentialScan | FileOptions.Asynchronous);
        var fill = FillAsync(fileStream, pipe.Writer, readSize: 1 << 20);

        Exception? loopError = null;
        try
        {
            await foreach (var chunk in SplitToChunks(pipe.Reader.Split(newLine, ct), bufferSize, ct).ConfigureAwait(false))
            {
                await sorter.SendAsync(chunk, ct).ConfigureAwait(false);
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
            await saver.Completion.ConfigureAwait(false);
        }
        finally
        {
            sorterToSaver.Dispose();
            saverToNames.Dispose();
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

        List<FileInfo> files = [];
        while (fileNames.TryReceive(out var file))
            files.Add(file);
        return files;
    }

    /// <summary>Продюсер чтения файла: блоками readSize в пайп.</summary>
    private static async Task FillAsync(Stream source, PipeWriter writer, int readSize)
    {
        try
        {
            while (true)
            {
                var mem = writer.GetMemory(readSize);
                int n = await source.ReadAsync(mem).ConfigureAwait(false);
                if (n == 0) break;
                writer.Advance(n);
                var r = await writer.FlushAsync().ConfigureAwait(false);
                if (r.IsCompleted) break;
            }
            await writer.CompleteAsync().ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            await writer.CompleteAsync(ex).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Собирает строки в чанки: байты текста строки — ключ Dictionary (ArraySegment в буфере чанка,
    /// сравнение по байтам), число — элемент списка. Повторный текст добавляет число в список существующего ключа.
    /// </summary>
    private async IAsyncEnumerable<Chunk<T>> SplitToChunks(IAsyncEnumerable<ReadOnlySequence<byte>> lines, int bufferSize, [EnumeratorCancellation] CancellationToken ct = default)
    {
        Chunk<T> chunk = new(dictionaryPool, bufferSize);
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
    /// Преобразует байты числа в T. Для utf-8 — парсинг байт напрямую,
    /// для остальных кодировок — сперва декодирование в char.
    /// </summary>
    private T ParseNumber(ReadOnlySpan<byte> bytes) =>
        isUtf8 ? T.Parse(bytes, null) : DecodeAndParse(bytes);

    private T DecodeAndParse(ReadOnlySpan<byte> bytes)
    {
        Span<char> chars = bytes.Length <= StackallocCharThreshold
            ? stackalloc char[StackallocCharThreshold]
            : new char[encoding.GetMaxCharCount(bytes.Length)];
        var written = encoding.GetChars(bytes, chars);
        return T.Parse(chars[..written], null);
    }

    private void AddToChunk(Chunk<T> chunk, T x, ReadOnlySequence<byte> text)
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
    private async Task SortChunk(Chunk<T> chunk, CancellationToken ct)
    {
        var keyCount = chunk.KeyCount;
        var maxKeyBytes = 0;
        foreach (var key in chunk.Lookup.Keys)
            maxKeyBytes = Math.Max(maxKeyBytes, key.Length);
        var decodedBuffer = new char[encoding.GetMaxCharCount(maxKeyBytes)];

        // Смещения текстов: тексты лежат в буфере подряд в порядке номеров.
        var textOffsets = new int[keyCount + 1];
        for (var i = 0; i < keyCount; i++)
            textOffsets[i + 1] = textOffsets[i] + chunk.TextLengths[i];
        chunk.TextOffsets = textOffsets;

        int[] sortKeyOffsets = [];
        int[] sortKeyLengths = [];

        if (!isOrdinal)
        {
            // Пасс 1: точная длина sort key каждого ключа.
            var sortKeyLengthsByNumber = new int[keyCount];
            var totalSortKey = 0;
            foreach (var kvp in chunk.Lookup)
            {
                var written = encoding.GetChars(chunk.BufferSpan.Slice(kvp.Key.Offset, kvp.Key.Length), decodedBuffer);
                totalSortKey += sortKeyLengthsByNumber[kvp.Value.Number] =
                    compareInfo.GetSortKeyLength(decodedBuffer.AsSpan(0, written), compareOptions);
            }
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
        var maxChars = encoding.GetMaxCharCount(chunk.TextLengths.Count > 0 ? chunk.TextLengths.Max() : 0);
        var parallelOptions = new ParallelOptions { CancellationToken = ct };
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

        // Словарь больше не нужен — возвращаем в пул до записи файла.
        chunk.ReleaseLookup();
    }

    /// <summary>
    /// Пишет отсортированный чанк в промежуточный файл с компрессией zstd (fast). Формат v2,
    /// все числа — LEB128 varint: varint(длина текста), текст, varint(длина sort key), [sort key],
    /// varint(количество чисел), varint(первое число), varint(разности)... keyLen == 0 — ordinal-режим.
    /// </summary>
    private async Task<FileInfo> SaveChunk(Chunk<T> chunk, CancellationToken ct)
    {
        var index = Interlocked.Increment(ref savedChunkCount) - 1;
        var chunkFile = new FileInfo(Path.ChangeExtension(source.FullName, $".chunk-{index:D3}{Path.GetExtension(source.FullName)}"));
        using var fileStream = new FileStream(chunkFile.FullName, FileMode.Create, FileAccess.Write, FileShare.None,
            bufferSize: writeStreamBufferSize, FileOptions.SequentialScan);
        // Преаллокация точного несжатого размера — файловая система не расширяет файл по кусочкам.
        fileStream.SetLength(ComputeChunkSize(chunk));
        fileStream.Seek(0, SeekOrigin.Begin);
        using var zstdStream = new CompressionStream(fileStream, zstdLevel);
        var writer = PipeWriter.Create(zstdStream, new StreamPipeWriterOptions(leaveOpen: true));
        var textOffsets = chunk.TextOffsets!;
        var sortKeyOffsets = chunk.SortKeyOffsets;
        var sortKeyLengths = chunk.SortKeyLengths;
        var values = chunk.Values!;
        var csr = chunk.CsrOffsets!;
        var ordinal = sortKeyOffsets is null; // ordinal-режим: sort key не вычислялись, keyLen = 0
        foreach (var i in chunk.Order!)
        {
            var keySpan = chunk.BufferSpan.Slice(textOffsets[i], textOffsets[i + 1] - textOffsets[i]);
            writer.WriteVarint((ulong)keySpan.Length);
            writer.Write(keySpan);
            writer.WriteVarint(ordinal ? 0UL : (ulong)sortKeyLengths![i]);
            if (!ordinal)
                writer.Write(chunk.GetSortKeySlice(sortKeyOffsets![i], sortKeyLengths![i]));
            var start = csr[i];
            var count = csr[i + 1] - start;
            writer.WriteVarint((ulong)count);
            T previous = T.Zero;
            for (var j = start; j < start + count; j++)
            {
                writer.WriteVarint(ulong.CreateChecked(values[j] - previous));
                previous = values[j];
            }
            if (writer.UnflushedBytes >= flushThreshold)
                await writer.FlushAsync(ct).ConfigureAwait(false);
        }
        await writer.FlushAsync(ct).ConfigureAwait(false);
        await writer.CompleteAsync().ConfigureAwait(false);
        chunk.ReleaseSortKeys();
        // Dispose zstd-потока дописывает footer кадра; затем обрезаем преаллокацию по факту.
        zstdStream.Dispose();
        fileStream.SetLength(fileStream.Position);
        return chunkFile;
    }
    private int savedChunkCount;

    /// <summary>Точный несжатый размер файла чанка до записи — для преаллокации.</summary>
    private static long ComputeChunkSize(Chunk<T> chunk)
    {
        var textOffsets = chunk.TextOffsets!;
        var sortKeyOffsets = chunk.SortKeyOffsets; // null в ordinal-режиме
        var sortKeyLengths = chunk.SortKeyLengths;
        var csr = chunk.CsrOffsets!;
        var values = chunk.Values!;
        long total = 0;
        foreach (var i in chunk.Order!)
        {
            var textLen = textOffsets[i + 1] - textOffsets[i];
            var count = csr[i + 1] - csr[i];
            total += VarintSize((ulong)textLen) + textLen + VarintSize((ulong)count);
            if (sortKeyLengths is not null)
            {
                var keyLen = sortKeyLengths[i];
                total += VarintSize((ulong)keyLen) + keyLen;
            }
            T previous = T.Zero;
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
