using System.Buffers;
using System.Globalization;
using System.IO.Pipelines;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Text;
using Microsoft.Extensions.ObjectPool;

/// <summary>
/// Фаза разбиения: читает исходный файл, собирает строки в чанки (текст — ключ словаря, числа — списки),
/// сортирует чанки и пишет отсортированные промежуточные файлы &lt;.chunk-NNN.*&gt;.
/// </summary>
class SplitSort<T> where T : IBinaryInteger<T>
{
    // Бюджет буфера строк одного чанка; определяет количество промежуточных файлов для merge.
    private const int BufferSize = 64 * 1024 * 1024;

    // Граница stackalloc для копий текста строки; больше — аренда из пула, чтобы не переполнить стек.
    private const int StackallocByteThreshold = 16 * 1024;

    // Граница stackalloc для декодирующего char-буфера числа.
    private const int StackallocCharThreshold = 512;

    readonly FileInfo source;
    readonly Encoding encoding;
    readonly byte[] newLine;
    readonly byte[] dotSpace;
    readonly CompareInfo compareInfo = CultureInfo.CurrentCulture.CompareInfo;
    readonly CompareOptions compareOptions;
    readonly bool isUtf8;
    readonly ArrayPool<byte> arrayPool = ArrayPool<byte>.Shared;
    readonly ObjectPool<List<T>> listPool = new DefaultObjectPool<List<T>>(new ListPoolPolicy<T>());
    readonly ObjectPool<Dictionary<ArraySegment<byte>, List<T>>> dictionaryPool = new DefaultObjectPool<Dictionary<ArraySegment<byte>, List<T>>>(new LookupPoolPolicy<T>());

    public SplitSort(FileInfo source, Encoding encoding, CompareOptions compareOptions = CompareOptions.None)
    {
        this.source = source;
        this.encoding = encoding;
        this.compareOptions = compareOptions;
        isUtf8 = encoding.WebName == "utf-8";
        newLine = encoding.GetBytes(Environment.NewLine);
        dotSpace = encoding.GetBytes(". ");
    }

    /// <summary>Прогоняет конвейер разбиения и возвращает пути созданных промежуточных файлов.</summary>
    public async Task<List<FileInfo>> RunAsync(CancellationToken ct)
    {
        var reader = PipeReader.Create(source.OpenRead());
        var files = new List<FileInfo>();
        await foreach (var file in SaveChunks(SortChunks(SplitToChunks(reader.Split(newLine, ct), BufferSize, ct), ct), ct).ConfigureAwait(false))
        {
            files.Add(file);
        }
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


    private async IAsyncEnumerable<Chunk<T>> SortChunks(IAsyncEnumerable<Chunk<T>> chunks, [EnumeratorCancellation] CancellationToken ct = default)
    {
        await foreach (var chunk in chunks.WithCancellation(ct).ConfigureAwait(false))
        {
            await SortChunk(chunk, ct).ConfigureAwait(false);
            yield return chunk;
        }
    }

    /// <summary>
    /// Сортирует списки чисел параллельно и упорядочивает ключи чанка по явным sort key.
    /// Все буферы здесь короткоживущие: декодирующий — по самому длинному ключу, sort key —
    /// нужен только на время сортировки и после упорядочивания отбрасывается.
    /// </summary>
    private async Task SortChunk(Chunk<T> chunk, CancellationToken ct)
    {
        var valuesTask = Parallel.ForEachAsync(chunk.Lookup.Values, ct, (values, _) =>
        {
            values.Sort();
            return ValueTask.CompletedTask;
        });

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
            .Select(e => new ChunkEntry<T>(e.Key, e.Values))
            .ToList();

        await valuesTask.ConfigureAwait(false);
    }

    /// <summary>
    /// Пишет отсортированный чанк в промежуточный файл. Формат записи, все числа — LEB128 varint:
    /// varint(длина текста), текст (байты в кодировке файла), varint(количество чисел), varint(первое число),
    /// varint(разность второго и первого), varint(разность третьего и второго)...
    /// Списки отсортированы по возрастанию, поэтому разности неотрицательны.
    /// </summary>
    private async IAsyncEnumerable<FileInfo> SaveChunks(IAsyncEnumerable<Chunk<T>> chunks, [EnumeratorCancellation] CancellationToken ct = default)
    {
        var extension = Path.GetExtension(source.FullName);
        var index = 0;
        await foreach (var chunk in chunks.WithCancellation(ct).ConfigureAwait(false))
        {
            var chunkFile = new FileInfo(Path.ChangeExtension(source.FullName, $".chunk-{index++:D3}{extension}"));
            var writer = PipeWriter.Create(chunkFile.Create());
            foreach (var entry in chunk.Sorted!)
            {
                writer.WriteVarint((ulong)entry.Key.Count);
                writer.Write(entry.Key.AsSpan());
                writer.WriteVarint((ulong)entry.Values.Count);
                T previous = T.Zero;
                foreach (var value in entry.Values)
                {
                    writer.WriteVarint(ulong.CreateChecked(value - previous));
                    previous = value;
                }
                if (writer.UnflushedBytes >= SortConstants.FlushThreshold)
                    await writer.FlushAsync(ct).ConfigureAwait(false);
            }
            await writer.FlushAsync(ct).ConfigureAwait(false);
            await writer.CompleteAsync().ConfigureAwait(false);
            chunk.Dispose();
            yield return chunkFile;
        }
    }
}
