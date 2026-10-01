using System.Buffers;
using System.Globalization;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Channels;

static class MergeExtensions
{
    // Граница stackalloc для декодирующего char-буфера записи (8K char = 16КБ); больше — аллокация.
    private const int StackallocCharThreshold = 8 * 1024;

    /// <summary>
    /// Продюсер чанка при merge: читает сжатый файл через декомпрессионный поток, собирает записи
    /// в пакеты (тексты — zero-copy в общем буфере пакета, значения — общий массив), вычисляет
    /// sort key всех записей пакета под глобальным CPU-слотом и кладёт пакеты в канал.
    /// Владеет stream: закрывает его при любом исходе. Владение буфером переходит к пакету.
    /// </summary>
    public static async Task ProduceBatches<T>(
        this Stream stream,
        ChannelWriter<Batch<T>> channel,
        Encoding encoding,
        CompareInfo compareInfo,
        CompareOptions compareOptions,
        SemaphoreSlim cpuSlots,
        int batchBytes,
        string label,
        CancellationToken ct) where T : IBinaryInteger<T>
    {
        var buffer = ArrayPool<byte>.Shared.Rent(batchBytes);
        var start = 0; // начало неразобранных данных (хвост незавершённой записи)
        var end = 0;   // конец данных в буфере
        long entriesProduced = 0;
        try
        {
            while (true)
            {
                // Дозаполняем буфер после хвоста незавершённой записи.
                var eof = false;
                while (end < buffer.Length)
                {
                    var n = await stream.ReadAsync(buffer.AsMemory(end), ct).ConfigureAwait(false);
                    if (n == 0)
                    {
                        eof = true;
                        break;
                    }
                    end += n;
                }
                if (end == start)
                {
                    ArrayPool<byte>.Shared.Return(buffer);
                    break; // EOF и хвоста нет
                }

                int consumed;
                Batch<T> batch;
                await cpuSlots.WaitAsync(ct).ConfigureAwait(false);
                try
                {
                    batch = ParseBatch<T>(buffer, start, end, encoding, compareInfo, compareOptions, eof, out consumed);
                }
                catch
                {
                    ArrayPool<byte>.Shared.Return(buffer);
                    throw;
                }
                finally
                {
                    cpuSlots.Release();
                }
                start = consumed;
                entriesProduced += batch.Entries.Length;

                // Старый буфер (с текстами записей) уходит во владение пакету; продолжаем со свежим.
                if (start != end)
                {
                    // Хвост незавершённой записи — в начало нового буфера.
                    var fresh = ArrayPool<byte>.Shared.Rent(batchBytes);
                    Buffer.BlockCopy(buffer, start, fresh, 0, end - start);
                    end -= start;
                    start = 0;
                    buffer = fresh;
                }
                else
                {
                    start = 0;
                    end = 0;
                    buffer = ArrayPool<byte>.Shared.Rent(batchBytes);
                }
                await channel.WriteAsync(batch, ct).ConfigureAwait(false);
                if (eof)
                    break; // при eof ParseBatch гарантирует start == end
            }
            Extensions.Trace($"[{label}] produced {entriesProduced} entries");
            channel.TryComplete();
        }
        catch (Exception ex)
        {
            channel.TryComplete(ex);
            throw;
        }
        finally
        {
            await stream.DisposeAsync().ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Разбирает записи из среза буфера [start, end): тексты остаются в буфере (zero-copy),
    /// значения δ-варинтов разворачиваются в общий массив, sort key вычисляются двумя пассами
    /// (длины → запись). ordinal-режим: ключ не вычисляется, SortKey алиасит текст.
    /// consumed — граница последней полной записи; на atEof хвост за ней — усечённая запись.
    /// </summary>
    private static Batch<T> ParseBatch<T>(
        byte[] buffer, int start, int end,
        Encoding encoding,
        CompareInfo compareInfo,
        CompareOptions compareOptions,
        bool atEof,
        out int consumed) where T : IBinaryInteger<T>
    {
        var metas = new List<BatchEntryMeta>();
        var values = new List<T>();
        // Срез от начала буфера: TryReadVarint индексирует pos относительно начала среза,
        // а pos здесь — абсолютное смещение в буфере.
        var span = buffer.AsSpan(0, end);
        var pos = start;
        while (pos < end)
        {
            var entryStart = pos;
            if (!Extensions.TryReadVarint(span, ref pos, out var textLength)
                || textLength > (ulong)(end - pos))
            {
                pos = entryStart;
                break;
            }
            var textOffset = pos;
            pos += (int)textLength;
            if (!Extensions.TryReadVarint(span, ref pos, out var count))
            {
                pos = entryStart;
                break;
            }
            var valuesStart = values.Count;
            T previous = T.Zero;
            var complete = true;
            for (ulong i = 0; i < count; i++)
            {
                if (!Extensions.TryReadVarint(span, ref pos, out var delta))
                {
                    complete = false;
                    break;
                }
                previous += T.CreateChecked(delta);
                values.Add(previous);
            }
            if (!complete)
            {
                pos = entryStart;
                break;
            }
            metas.Add(new(textOffset, (int)textLength, valuesStart, checked((int)count)));
        }
        consumed = pos;

        // Отрезаем значения неполной записи, если она не влезла.
        var totalValues = metas.Count == 0 ? 0 : metas[^1].ValuesStart + metas[^1].ValuesCount;
        if (values.Count != totalValues)
            values.RemoveRange(totalValues, values.Count - totalValues);

        if (atEof && pos != end)
            throw new FormatException("Truncated entry at end of chunk file");
        if (metas.Count == 0)
            throw new FormatException("Entry does not fit into the batch buffer");

        var valuesArray = values.ToArray();
        var entryCount = metas.Count;

        Batch<T> batch;
        var entries = new ReadChunkEntry<T>[entryCount];

        if (compareOptions == CompareOptions.Ordinal)
        {
            batch = new Batch<T>(buffer, null, valuesArray, entryCount);
            // Ключ — байты текста: сегмент SortKey алиасит сегмент Text.
            for (var i = 0; i < entryCount; i++)
            {
                var m = metas[i];
                var text = new ArraySegment<byte>(buffer, m.TextOffset, m.TextLength);
                entries[i] = new(batch, text, text, valuesArray, m.ValuesStart, m.ValuesCount);
            }
        }
        else
        {
            // Пасс A: точная длина sort key каждой записи.
            var maxText = 0;
            foreach (var m in metas)
                maxText = Math.Max(maxText, m.TextLength);
            var maxChars = encoding.GetMaxCharCount(maxText);
            Span<char> chars = maxChars <= StackallocCharThreshold
                ? stackalloc char[StackallocCharThreshold]
                : new char[maxChars];
            var keyLengths = new int[entryCount];
            long totalKeys = 0;
            for (var i = 0; i < entryCount; i++)
            {
                var m = metas[i];
                var w = encoding.GetChars(buffer.AsSpan(m.TextOffset, m.TextLength), chars);
                totalKeys += keyLengths[i] = compareInfo.GetSortKeyLength(chars[..w], compareOptions);
            }
            var keyOffsets = new int[entryCount + 1];
            for (var i = 0; i < entryCount; i++)
                keyOffsets[i + 1] = keyOffsets[i] + keyLengths[i];
            var keyBuffer = ArrayPool<byte>.Shared.Rent(checked((int)totalKeys));

            batch = new Batch<T>(buffer, keyBuffer, valuesArray, entryCount);

            // Пасс B: sort key каждой записи в общий буфер пакета.
            for (var i = 0; i < entryCount; i++)
            {
                var m = metas[i];
                var w = encoding.GetChars(buffer.AsSpan(m.TextOffset, m.TextLength), chars);
                compareInfo.GetSortKey(chars[..w], keyBuffer.AsSpan(keyOffsets[i], keyLengths[i]), compareOptions);
                entries[i] = new(batch,
                    new ArraySegment<byte>(buffer, m.TextOffset, m.TextLength),
                    new ArraySegment<byte>(keyBuffer, keyOffsets[i], keyLengths[i]),
                    valuesArray, m.ValuesStart, m.ValuesCount);
            }
        }

        batch.Entries = entries;
        return batch;
    }

    /// <summary>Адаптер канала пакетов к потоку записей для MergeSorted (Dispose каждой записи освобождает пакет).</summary>
    internal static async IAsyncEnumerable<ReadChunkEntry<T>> ReadEntries<T>(
        ChannelReader<Batch<T>> reader,
        string label,
        [EnumeratorCancellation] CancellationToken ct = default) where T : IBinaryInteger<T>
    {
        long consumed = 0;
        while (await reader.WaitToReadAsync(ct).ConfigureAwait(false))
        {
            while (reader.TryRead(out var batch))
            {
                foreach (var entry in batch.Entries)
                {
                    consumed++;
                    yield return entry;
                }
            }
        }
        Extensions.Trace($"[{label}] consumed {consumed} entries");
    }

    public static async IAsyncEnumerable<V> MergeSorted<T,V>(
        this IEnumerable<IAsyncEnumerable<T>> sources,
        IComparer<T> comparer,
        Func<IReadOnlyList<T>, V> merge,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        var enumerators = sources.Select(s => s.GetAsyncEnumerator(ct)).ToList();
        try
        {
            await foreach (var x in MergeSortedWithPriorityQueue(enumerators, comparer, merge, ct).ConfigureAwait(false))
            {
                yield return x;
            }
        }
        finally
        {

            foreach (var e in enumerators)
            {
                await e.DisposeAsync();
            }
        }
    }

    static async IAsyncEnumerable<V> MergeSortedWithPriorityQueue<T,V>(
        List<IAsyncEnumerator<T>> enumerators,
        IComparer<T> comparer,
        Func<IReadOnlyList<T>, V> merge,
        [EnumeratorCancellation] CancellationToken ct)
    {
        PriorityQueue<IAsyncEnumerator<T>, T> pq = new(enumerators.Count, comparer);
        List<T> group = new(enumerators.Count);
        List<IAsyncEnumerator<T>> groupSources = new(enumerators.Count);
        foreach (var e in enumerators)
        {
            if (await e.MoveNextAsync().ConfigureAwait(false))
            {
                pq.Enqueue(e, e.Current);
            }
        }
        while (pq.TryDequeue(out var current, out var x))
        {
            // Собираем группу равных (по компаратору) элементов: записи — в group,
            // их потоки — в groupSources (индексы совпадают).
            group.Add(current.Current);
            groupSources.Add(current);
            while (pq.TryPeek(out var e, out var y) && comparer.Compare(x, y) == 0)
            {
                group.Add(e.Current);
                groupSources.Add(e);
                pq.Dequeue();
            }

            // Lookahead до отдачи группы: поток может выдать ещё одну равную запись —
            // она присоединяется к той же группе (и будет проверена на своей итерации),
            // иначе значения культурно равных ключей одного потока не сольются и
            // глобальный порядок значений нарушится.
            for (var i = 0; i < groupSources.Count; i++)
            {
                var enumerator = groupSources[i];
                if (await enumerator.MoveNextAsync().ConfigureAwait(false))
                {
                    if (comparer.Compare(enumerator.Current, x) == 0)
                    {
                        group.Add(enumerator.Current);
                        groupSources.Add(enumerator);
                    }
                    else
                    {
                        pq.Enqueue(enumerator, enumerator.Current);
                    }
                }
            }

            yield return merge(group);

            // Группа потреблена — возвращаем буферы записей в пулы.
            foreach (var entry in group)
                (entry as IDisposable)?.Dispose();
            group.Clear();
            groupSources.Clear();
        }
    }

    public static IEnumerable<T> MergeSorted<T>(
        this IEnumerable<IEnumerable<T>> sources,
        IComparer<T> comparer)
    {
        var enumerators = sources.Select(s => s.GetEnumerator()).ToList();
        try
        {
            if (enumerators.Count == 1)
            {
                var e = enumerators[0];
                while (e.MoveNext())
                {
                    yield return e.Current;
                }
            }
            else
            {
                foreach (var x in MergeSortedWithPriorityQueue<T>(enumerators, comparer))
                {
                    yield return x;
                }
            }
        }
        finally
        {

            foreach (var e in enumerators)
            {
                e.Dispose();
            }
        }
    }

    static IEnumerable<T> MergeSortedWithPriorityQueue<T>(
        List<IEnumerator<T>> enumerators,
        IComparer<T> comparer)
    {
        PriorityQueue<IEnumerator<T>, T> pq = new(enumerators.Count, comparer);
        List<IEnumerator<T>> l = [];
        foreach (var e in enumerators)
        {
            if (e.MoveNext())
            {
                pq.Enqueue(e, e.Current);
            }
        }
        while (pq.TryDequeue(out var current, out var x))
        {
            while (pq.TryPeek(out var e, out var y) && comparer.Compare(x, y) == 0)
            {
                l.Add(current);
                current = pq.Dequeue();
            }
            if (l.Count == 0)
            {
                yield return current.Current;
                if (current.MoveNext())
                {
                    pq.Enqueue(current, current.Current);
                }
            }
            else
            {
                l.Add(current);
                foreach (var e in l)
                {
                    yield return e.Current;
                    if (e.MoveNext())
                    {
                        pq.Enqueue(e, e.Current);
                    }
                }
                l.Clear();
            }
        }
    }

    public static async IAsyncEnumerable<T> Concat<T>(
        this IEnumerable<IAsyncEnumerable<T>> sources,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        foreach (var source in sources)
        {
            await foreach (var item in source.WithCancellation(ct).ConfigureAwait(false))
            {
                yield return item;
            }
        }
    }


}
