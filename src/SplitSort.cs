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
    readonly int zstdLevel = zstdLevel;
    readonly ArrayPool<byte> arrayPool = ArrayPool<byte>.Shared;
    readonly ObjectPool<List<T>> listPool = new DefaultObjectPool<List<T>>(new ListPoolPolicy<T>());
    readonly ObjectPool<Dictionary<BufferString, List<T>>> dictionaryPool = new DefaultObjectPool<Dictionary<BufferString, List<T>>>(new LookupPoolPolicy<T>());

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
        finally
        {
            sorterToSaver.Dispose();
            saverToNames.Dispose();
        }

        if (loopError is not null)
            throw new InvalidOperationException("Failed to split the source file", loopError);

        List<FileInfo> files = [];
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
        Chunk<T> chunk = new(listPool, dictionaryPool, bufferSize) { Id = Interlocked.Increment(ref chunkId) };
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
            chunk = new(listPool, dictionaryPool, bufferSize) { Id = Interlocked.Increment(ref chunkId) };
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
        var valuesTask = Parallel.ForEachAsync(chunk.Lookup.Values, new ParallelOptions { CancellationToken = ct }, (values, _) =>
        {
            values.Sort();
            return ValueTask.CompletedTask;
        });

        if (isOrdinal)
        {
            chunk.Sorted = chunk.Lookup
                .AsParallel()
                .WithCancellation(ct)
                .OrderBy(kvp => kvp.Key, chunk.Comparer)
                .Select(kvp => new ChunkEntry<T>(kvp.Key, default, kvp.Value))
                .ToList();
        }
        else
        {
            var maxKeyBytes = 0;
            foreach (var key in chunk.Lookup.Keys)
                maxKeyBytes = Math.Max(maxKeyBytes, key.Length);
            var decodedBuffer = new char[encoding.GetMaxCharCount(maxKeyBytes)];

            // Один нативный буфер на все sort keys чанка. Бюджет — суммарные байты ключей × 1.5
            // (без пасса GetSortKeyLength); при нехватке буфер удваивается и ключи пишутся заново.
            chunk.AllocateSortKeys(Math.Max(16, chunk.Used * 3 / 2));
            List<ChunkEntry<T>> entries;
            while (true)
            {
                entries = new List<ChunkEntry<T>>(chunk.Lookup.Count);
                var offset = 0;
                var overflow = false;
                foreach (var kvp in chunk.Lookup)
                {
                    var written = encoding.GetChars(chunk.BufferSpan.Slice(kvp.Key.Offset, kvp.Key.Length), decodedBuffer);
                    var keyChars = decodedBuffer.AsSpan(0, written);
                    int length;
                    try
                    {
                        length = compareInfo.GetSortKey(keyChars, chunk.SortKeySpanFrom(offset), compareOptions);
                    }
                    catch (ArgumentException)
                    {
                        chunk.GrowSortKeys();
                        overflow = true;
                        break;
                    }
                    entries.Add(new ChunkEntry<T>(kvp.Key, new BufferString(offset, length), kvp.Value));
                    offset += length;
                }
                if (!overflow)
                    break;
            }

            chunk.Sorted = entries
                .AsParallel()
                .WithCancellation(ct)
                .OrderBy(e => e.SortKey, chunk.CreateSortKeyComparer())
                .Select(e => new ChunkEntry<T>(e.Key, e.SortKey, e.Values))
                .ToList();
        }

        await valuesTask.ConfigureAwait(false);
        // Словарь и списки значений больше не нужны — возвращаем в пулы до записи файла.
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
        await using var fileStream = chunkFile.Create();
        await using var zstdStream = new CompressionStream(fileStream, zstdLevel);
        var writer = PipeWriter.Create(zstdStream);
        foreach (var entry in chunk.Sorted!)
        {
            var keySpan = chunk.BufferSpan.Slice(entry.Key.Offset, entry.Key.Length);
            writer.WriteVarint((ulong)keySpan.Length);
            writer.Write(keySpan);
            writer.WriteVarint((ulong)entry.SortKey.Length);
            if (entry.SortKey.Length > 0)
                writer.Write(chunk.GetSortKeySlice(entry.SortKey.Offset, entry.SortKey.Length));
            writer.WriteVarint((ulong)entry.Values.Count);
            T previous = T.Zero;
            foreach (var value in entry.Values)
            {
                writer.WriteVarint(ulong.CreateChecked(value - previous));
                previous = value;
            }
            if (writer.UnflushedBytes >= flushThreshold)
                await writer.FlushAsync(ct).ConfigureAwait(false);
        }
        await writer.FlushAsync(ct).ConfigureAwait(false);
        await writer.CompleteAsync().ConfigureAwait(false);
        chunk.ReleaseSortKeys();
        // Dispose zstd-потока дописывает footer кадра; await using закрывает его и файл.
        return chunkFile;
    }
    private int savedChunkCount;
    private int chunkId;
}
