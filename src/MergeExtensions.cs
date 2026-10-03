using System.Buffers;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Channels;

static class MergeExtensions
{
    // Граница stackalloc для декодирующего char-буфера записи (8K char = 16КБ); больше — аллокация.
    private const int StackallocCharThreshold = 8 * 1024;

    /// <summary>
    /// Продюсер чанка при merge: читатель и вычислитель работают параллельно — читатель наполняет
    /// буферы из декомпрессионного потока, не дожидаясь CPU, вычислитель считает sort key пакета
    /// под глобальным CPU-слотом. Буфер с текстами записей переходит во владение пакету.
    /// </summary>
    public static Task ProduceBatches(
        this Stream stream,
        ChannelWriter<Batch> channel,
        Encoding encoding,
        CompareInfo compareInfo,
        CompareOptions compareOptions,
        SemaphoreSlim cpuSlots,
        int batchBytes,
        CancellationToken ct)
    {
        Channel<(byte[] Buffer, int Filled, bool Eof)> buffers = Channel.CreateBounded<(byte[] Buffer, int Filled, bool Eof)>(
            new BoundedChannelOptions(1) { SingleWriter = true, SingleReader = true });
        return Task.WhenAll(
            ReadChunks(stream, buffers.Writer, batchBytes, ct),
            ComputeBatches(buffers.Reader, channel, encoding, compareInfo, compareOptions, cpuSlots, ct));
    }

    /// <summary>Читатель: наполняет буферы до конца (или eof) и передаёт вычислителю.</summary>
    private static async Task ReadChunks(
        Stream stream,
        ChannelWriter<(byte[] Buffer, int Filled, bool Eof)> buffers,
        int batchBytes,
        CancellationToken ct)
    {
        try
        {
            while (true)
            {
                var buffer = ArrayPool<byte>.Shared.Rent(batchBytes);
                int filled = 0;
                var eof = false;
                while (filled < buffer.Length)
                {
                    var n = await stream.ReadAsync(buffer.AsMemory(filled), ct).ConfigureAwait(false);
                    if (n == 0)
                    {
                        eof = true;
                        break;
                    }
                    filled += n;
                }
                await buffers.WriteAsync((buffer, filled, eof), ct).ConfigureAwait(false);
                if (eof)
                {
                    buffers.TryComplete();
                    return;
                }
            }
        }
        catch (Exception ex)
        {
            buffers.TryComplete(ex);
            throw;
        }
        finally
        {
            await stream.DisposeAsync().ConfigureAwait(false);
        }
    }

    /// <summary>Вычислитель: разбирает порции в пакеты под CPU-слотом и кладёт в выходной канал.</summary>
    private static async Task ComputeBatches(
        ChannelReader<(byte[] Buffer, int Filled, bool Eof)> buffers,
        ChannelWriter<Batch> channel,
        Encoding encoding,
        CompareInfo compareInfo,
        CompareOptions compareOptions,
        SemaphoreSlim cpuSlots,
        CancellationToken ct)
    {
        byte[]? pending = null; // хвост незавершённой записи прошлой порции
        var pendingLen = 0;
        try
        {
            while (true)
            {
                var (buffer, filled, eof) = await buffers.ReadAsync(ct).ConfigureAwait(false);
                if (filled == 0)
                {
                    ArrayPool<byte>.Shared.Return(buffer);
                    break; // конец потока
                }

                byte[] batchBuffer;
                if (pending is null)
                {
                    batchBuffer = buffer; // разбор на месте, буфер уходит пакету
                }
                else
                {
                    // Хвост прошлой порции + новые данные в свежий буфер — записи ссылаются на него.
                    batchBuffer = ArrayPool<byte>.Shared.Rent(pendingLen + filled);
                    Buffer.BlockCopy(pending, 0, batchBuffer, 0, pendingLen);
                    Buffer.BlockCopy(buffer, 0, batchBuffer, pendingLen, filled);
                    ArrayPool<byte>.Shared.Return(buffer);
                }
                var end = pendingLen + filled;

                Batch batch;
                int consumed;
                await cpuSlots.WaitAsync(ct).ConfigureAwait(false);
                try
                {
                    batch = ParseBatch(batchBuffer, end, encoding, compareInfo, compareOptions, eof, out consumed);
                }
                catch
                {
                    ArrayPool<byte>.Shared.Return(batchBuffer);
                    throw;
                }
                finally
                {
                    cpuSlots.Release();
                }

                // Хвост незавершённой записи — в отдельный буфер; batchBuffer уходит пакету.
                var tail = end - consumed;
                var oldPending = pending;
                if (tail > 0)
                {
                    pending = ArrayPool<byte>.Shared.Rent(tail);
                    Buffer.BlockCopy(batchBuffer, consumed, pending, 0, tail);
                    pendingLen = tail;
                }
                else
                {
                    pending = null;
                    pendingLen = 0;
                }
                if (oldPending is not null)
                    ArrayPool<byte>.Shared.Return(oldPending);

                await channel.WriteAsync(batch, ct).ConfigureAwait(false);
                if (eof)
                    break; // eof-порция обработана — данных больше не будет
            }
            channel.TryComplete();
        }
        catch (Exception ex)
        {
            channel.TryComplete(ex);
            throw;
        }
    }

    /// <summary>
    /// Разбирает записи из среза буфера [0, end): тексты остаются в буфере (zero-copy),
    /// значения δ-варинтов разворачиваются в общий массив, sort key вычисляются двумя пассами
    /// (длины → запись). ordinal-режим: ключ не вычисляется, SortKey алиасит текст.
    /// consumed — граница последней полной записи; на atEof хвост за ней — усечённая запись.
    /// </summary>
    private static Batch ParseBatch(
        byte[] buffer, int end,
        Encoding encoding,
        CompareInfo compareInfo,
        CompareOptions compareOptions,
        bool atEof,
        out int consumed)
    {
        List<BatchEntryMeta> metas = [];
        List<long> values = [];
        // Срез от начала буфера: TryReadVarint индексирует pos относительно начала среза,
        // а pos здесь — абсолютное смещение в буфере.
        var span = buffer.AsSpan(0, end);
        var pos = 0;
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
            long previous = 0;
            var complete = true;
            for (ulong i = 0; i < count; i++)
            {
                if (!Extensions.TryReadVarint(span, ref pos, out var delta))
                {
                    complete = false;
                    break;
                }
                previous += checked((long)delta);
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

        Batch batch;
        ReadChunkEntry[] entries = new ReadChunkEntry[entryCount];

        if (compareOptions == CompareOptions.Ordinal)
        {
            batch = new Batch(buffer, null, entryCount);
            // Ключ — байты текста: сегмент SortKey алиасит сегмент Text.
            for (var i = 0; i < entryCount; i++)
            {
                var m = metas[i];
                ArraySegment<byte> text = new(buffer, m.TextOffset, m.TextLength);
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

            batch = new Batch(buffer, keyBuffer, entryCount);

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
    internal static async IAsyncEnumerable<ReadChunkEntry> ReadEntries(
        ChannelReader<Batch> reader,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        while (await reader.WaitToReadAsync(ct).ConfigureAwait(false))
        {
            while (reader.TryRead(out var batch))
            {
                foreach (var entry in batch.Entries)
                    yield return entry;
            }
        }
    }

    public static async IAsyncEnumerable<V> MergeSorted<T,V>(
        this IEnumerable<IAsyncEnumerable<T>> sources,
        IComparer<T> comparer,
        Func<IReadOnlyList<T>, V> merge,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        List<IAsyncEnumerator<T>> enumerators = [.. sources.Select(s => s.GetAsyncEnumerator(ct))];
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
            ct.ThrowIfCancellationRequested();
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
        List<IEnumerator<T>> enumerators = [.. sources.Select(s => s.GetEnumerator())];
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
            while (pq.TryPeek(out var _, out var y) && comparer.Compare(x, y) == 0)
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
}
