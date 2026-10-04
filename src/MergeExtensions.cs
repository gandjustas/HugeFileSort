using System.Buffers;
using System.IO.Pipelines;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

static class MergeExtensions
{
    /// <summary>
    /// Разбирает буфер пайпа чанка в перечисление записей. Формат записи в пайпе:
    /// [varint textLen][text][varint keyLen][key][varint count][count×long].
    /// consumed двигается только до последней обработанной записи: ChunkLine.Dispose двигает
    /// окно, итератор делает AdvanceTo не дальше него, поэтому мерж может держать записи групп
    /// (в том числе по две у одного источника при lookahead), а пайп не переиспользует их байты.
    /// Позиции ведутся в абсолютных байтах потока чанка: каждая AdvanceTo строится через
    /// GetPosition текущего буфера, объекты SequencePosition через границы ReadAsync не хранятся.
    /// </summary>
    public static async IAsyncEnumerable<ChunkLine> ReadLines(
        this PipeReader reader,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        ChunkWindow window = new();
        var start = 0L;     // абсолютная позиция начала буфера (= байт уже отдано consumed)
        var parseAbs = 0L;  // абсолютная позиция следующей неразобранной записи
        try
        {
            while (true)
            {
                var result = await reader.ReadAsync(ct).ConfigureAwait(false);
                var buffer = result.Buffer;
                while (TryReadLine(buffer, start, ref parseAbs, window, result.IsCompleted, out var line))
                    yield return line;

                if (result.IsCompleted)
                    break; // усечённый хвост TryReadLine бросил бы исключением

                // consumed — только до обработанных записей: окно двигает Dispose в мерже.
                var consumedRel = window.HasReleased ? checked(window.ReleasedEnd - start) : 0;
                reader.AdvanceTo(buffer.GetPosition(consumedRel), buffer.End);
                start += consumedRel;
            }
        }
        finally
        {
            await reader.CompleteAsync().ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Читает запись с абсолютной позиции parseAbs. Запись пишется продюсером одним GetSpan —
    /// целиком в одном сегменте. false — запись ещё не дописана в пайп; усечённый хвост при
    /// atEof — FormatException.
    /// </summary>
    private static bool TryReadLine(ReadOnlySequence<byte> buffer, long bufferStart, ref long parseAbs, ChunkWindow window, bool atEof, out ChunkLine line)
    {
        line = default;
        var rel = checked(parseAbs - bufferStart);
        if (rel >= buffer.Length)
            return false;

        var rest = buffer.Slice(buffer.GetPosition(rel));
        SequenceReader<byte> reader = new(rest);
        if (!TryReadVarint(ref reader, out var textLength)
            || reader.Remaining < checked((long)textLength))
            return Incomplete(atEof);
        var textOffset = checked((int)reader.Consumed);
        reader.Advance(checked((long)textLength));
        if (!TryReadVarint(ref reader, out var keyLength)
            || reader.Remaining < checked((long)keyLength))
            return Incomplete(atEof);
        var keyOffset = checked((int)reader.Consumed);
        reader.Advance(checked((long)keyLength));
        if (!TryReadVarint(ref reader, out var count)
            || reader.Remaining < checked((long)count * 8))
            return Incomplete(atEof);
        var valuesOffset = checked((int)reader.Consumed);

        var recordLength = reader.Consumed + checked((long)count * 8);
        var record = buffer.Slice(buffer.GetPosition(rel), recordLength);
        if (!record.IsSingleSegment)
            throw new InvalidOperationException("Record is not contiguous in the chunk pipe");
        if (!MemoryMarshal.TryGetArray(record.First, out var segment) || segment.Array is not { } array)
            throw new InvalidOperationException("Chunk pipe segment is not array-backed");
        parseAbs += recordLength;
        line = new ChunkLine(window, array,
            segment.Offset + textOffset, checked((int)textLength),
            segment.Offset + keyOffset, checked((int)keyLength),
            segment.Offset + valuesOffset, checked((int)count),
            parseAbs);
        return true;

        static bool Incomplete(bool atEof)
        {
            if (atEof)
                throw new FormatException("Truncated entry at end of chunk file");
            return false;
        }
    }

    private static bool TryReadVarint(ref SequenceReader<byte> reader, out ulong value)
    {
        value = 0;
        ulong result = 0;
        var shift = 0;
        for (var i = 0; i < 10; i++)
        {
            if (!reader.TryRead(out var b))
                return false;
            result |= (ulong)(b & 0x7F) << shift;
            if ((b & 0x80) == 0)
            {
                value = result;
                return true;
            }
            shift += 7;
        }
        throw new FormatException("Varint is too long, file is corrupted");
    }

    public static async IAsyncEnumerable<V> MergeSorted<T,V>(
        this IEnumerable<IAsyncEnumerable<T>> sources,
        IComparer<T> comparer,
        Func<IReadOnlyList<T>, V> merge,
        [EnumeratorCancellation] CancellationToken ct = default) where T : IDisposable
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
        [EnumeratorCancellation] CancellationToken ct) where T : IDisposable
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

            // Группа потреблена — записи отдают свои байты пайпам.
            foreach (var entry in group)
                entry.Dispose();
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
