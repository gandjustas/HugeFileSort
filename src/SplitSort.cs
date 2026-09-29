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
class SplitSort<T> where T : IBinaryInteger<T>
{
    // Адаптивный размер чанка ограничен снизу (больше параллелизма на маленьких файлах)
    // и сверху (меньше накладных расходов merge на больших). Пул буферов округляет
    // запрошенный размер вверх до степени двойки.
    private const long MinBufferSize = 32 * 1024 * 1024;
    private const long MaxBufferSize = 100 * 1024 * 1024;

    // Граница stackalloc для копий текста строки; больше — аренда из пула, чтобы не переполнить стек.
    private const int StackallocByteThreshold = 16 * 1024;

    // Граница stackalloc для декодирующего char-буфера числа.
    private const int StackallocCharThreshold = 512;

    // Порог необработанных байт для сброса PipeWriter в файл: StreamPipeWriter сам
    // не пишет в Stream до FlushAsync/CompleteAsync.
    private const int FlushThreshold = 1024 * 1024;

    // zstd "быстрый" уровень компрессии промежуточных файлов.
    private const int ZstdFastLevel = 1;

    // Параллелизм сортировки чанков: чанк в сортировке держит буфер + sort keys (~3.5x Used)
    // + словарь, поэтому верхняя граница ограничена памятью.
    private static readonly int SortParallelism = Math.Min(Environment.ProcessorCount, 4);

    readonly FileInfo source;
    readonly Encoding encoding;
    readonly byte[] newLine;
    readonly byte[] dotSpace;
    readonly CompareInfo compareInfo = CultureInfo.CurrentCulture.CompareInfo;
    readonly CompareOptions compareOptions;
    readonly bool isUtf8;
    readonly bool isOrdinal;
    readonly int bufferSize;
    readonly ArrayPool<byte> arrayPool = ArrayPool<byte>.Shared;
    readonly ObjectPool<List<T>> listPool = new DefaultObjectPool<List<T>>(new ListPoolPolicy<T>());
    readonly ObjectPool<Dictionary<ArraySegment<byte>, List<T>>> dictionaryPool = new DefaultObjectPool<Dictionary<ArraySegment<byte>, List<T>>>(new LookupPoolPolicy<T>());

    public SplitSort(FileInfo source, Encoding encoding, CompareOptions compareOptions = CompareOptions.None)
    {
        this.source = source;
        this.encoding = encoding;
        this.compareOptions = compareOptions;
        isUtf8 = encoding.WebName == "utf-8";
        isOrdinal = compareOptions == CompareOptions.Ordinal;
        newLine = encoding.GetBytes(Environment.NewLine);
        dotSpace = encoding.GetBytes(". ");
        bufferSize = ComputeBufferSize(source.Length);
    }

    /// <summary>Адаптивный буфер чанка: большой файл — крупные чанки (меньше накладных расходов merge), маленький — мелкие (больше параллелизма).</summary>
    private static int ComputeBufferSize(long fileLength) =>
        (int)Math.Clamp(fileLength / (Environment.ProcessorCount * 2L), MinBufferSize, MaxBufferSize);

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
                MaxDegreeOfParallelism = SortParallelism,
                BoundedCapacity = SortParallelism * 2,
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

        var reader = PipeReader.Create(source.OpenRead());
        Exception? loopError = null;
        try
        {
            await foreach (var chunk in SplitToChunks(reader.Split(newLine, ct), bufferSize, ct).ConfigureAwait(false))
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
        catch when (loopError is null)
        {
            throw;
        }
        finally
        {
            sorterToSaver.Dispose();
            saverToNames.Dispose();
        }

        if (loopError is not null)
            throw new InvalidOperationException("Failed to split the source file", loopError);

        var files = new List<FileInfo>();
        while (fileNames.TryReceive(out var file))
            files.Add(file);
        return files;
    }

    /// <summary>
    /// Собирает строки в чанки: байты текста строки — ключ Dictionary (ArraySegment в буфере чанка,
    /// сравнение по байтам), число — элемент списка. Повторный текст добавляет число в список существующего ключа.
    /// </summary>
    private async IAsyncEnumerable<Chunk<T>> SplitToChunks(IAsyncEnumerable<ReadOnlySequence<byte>> lines, int bufferSize, [EnumeratorCancellation] CancellationToken ct = default)
    {
        Chunk<T> chunk = new(arrayPool, listPool, dictionaryPool, bufferSize);
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
            chunk = new(arrayPool, listPool, dictionaryPool, bufferSize);
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
            chunk.AddItemToLookup(buffer[..(int)text.Length], x);
        }
        finally
        {
            if (rented is not null) arrayPool.Return(rented);
        }
    }

    /// <summary>
    /// Сортирует списки чисел параллельно и упорядочивает ключи чанка.
    /// Ordinal-режим: ключи не вычисляются — сравниваются байты текста, SortKey записей пустой.
    /// Культурный режим: явные sort key в общий короткоживущий массив, сортировка по ним.
    /// </summary>
    private async Task SortChunk(Chunk<T> chunk, CancellationToken ct)
    {
        var valuesTask = Parallel.ForEachAsync(chunk.Lookup.Values, ct, (values, _) =>
        {
            values.Sort();
            return ValueTask.CompletedTask;
        });

        if (isOrdinal)
        {
            chunk.Sorted = chunk.Lookup
                .AsParallel()
                .OrderBy(kvp => kvp.Key, SortKeyComparer.Instance)
                .Select(kvp => new ChunkEntry<T>(kvp.Key, default, kvp.Value))
                .ToList();
        }
        else
        {
            var maxKeyBytes = 0;
            foreach (var key in chunk.Lookup.Keys)
                maxKeyBytes = Math.Max(maxKeyBytes, key.Count);
            var decodedBuffer = new char[encoding.GetMaxCharCount(maxKeyBytes)];

            // Пасс 1: точный суммарный размер sort key всех ключей.
            var sortKeySize = 0;
            foreach (var kvp in chunk.Lookup)
            {
                var written = encoding.GetChars(kvp.Key, decodedBuffer);
                sortKeySize += compareInfo.GetSortKeyLength(decodedBuffer.AsSpan(0, written), compareOptions);
            }

            // Пасс 2: CompareInfo.GetSortKey → байты в общий короткоживущий массив.
            var sortKeys = new byte[sortKeySize];
            var entries = new List<(ArraySegment<byte> Key, ArraySegment<byte> SortKey, List<T> Values)>(chunk.Lookup.Count);
            var offset = 0;
            foreach (var kvp in chunk.Lookup)
            {
                var written = encoding.GetChars(kvp.Key, decodedBuffer);
                var length = compareInfo.GetSortKey(decodedBuffer.AsSpan(0, written), sortKeys.AsSpan(offset), compareOptions);
                entries.Add((kvp.Key, new ArraySegment<byte>(sortKeys, offset, length), kvp.Value));
                offset += length;
            }

            chunk.Sorted = entries
                .AsParallel()
                .OrderBy(e => e.SortKey, SortKeyComparer.Instance)
                .Select(e => new ChunkEntry<T>(e.Key, e.SortKey, e.Values))
                .ToList();
        }

        await valuesTask.ConfigureAwait(false);
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
        await using var fileStream = chunkFile.Create();
        await using var zstdStream = new CompressionStream(fileStream, ZstdFastLevel);
        var writer = PipeWriter.Create(zstdStream);
        foreach (var entry in chunk.Sorted!)
        {
            writer.WriteVarint((ulong)entry.Key.Count);
            writer.Write(entry.Key.AsSpan());
            writer.WriteVarint((ulong)entry.SortKey.Count);
            if (entry.SortKey.Count > 0)
                writer.Write(entry.SortKey.AsSpan());
            writer.WriteVarint((ulong)entry.Values.Count);
            T previous = T.Zero;
            foreach (var value in entry.Values)
            {
                writer.WriteVarint(ulong.CreateChecked(value - previous));
                previous = value;
            }
            if (writer.UnflushedBytes >= FlushThreshold)
                await writer.FlushAsync(ct).ConfigureAwait(false);
        }
        await writer.FlushAsync(ct).ConfigureAwait(false);
        await writer.CompleteAsync().ConfigureAwait(false);
        // Dispose zstd-потока дописывает footer кадра; await using закрывает его и файл.
        return chunkFile;
    }

    private int savedChunkCount;
}
