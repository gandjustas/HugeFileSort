using System.Buffers;
using System.Globalization;
using System.IO.Pipelines;
using System.Runtime.CompilerServices;
using System.Text;
using System.Numerics;
using Microsoft.Extensions.ObjectPool;

static class MergeExtensions
{
    // Граница stackalloc для короткоживущего декодирующего char-буфера записи (8K char = 16КБ);
    // больше — аренда из пула.
    private const int StackallocCharThreshold = 8 * 1024;

    /// <summary>
    /// Читает промежуточный файл (формат чанка: varint(длина) текст varint(кол-во) varint(δ0) varint(δ1)...)
    /// в поток записей. Буферы записей (текст, sort key, список значений) берутся из пулов;
    /// обязанность вернуть их в пулы — у потребителя записи (ReadChunkEntry.Dispose).
    /// Reader'ом владеет итератор: CompleteAsync в finally.
    /// </summary>
    public static async IAsyncEnumerable<ReadChunkEntry<T>> ReadChunkEntries<T>(
        this PipeReader reader,
        Encoding encoding,
        CompareInfo compareInfo,
        CompareOptions compareOptions,
        [EnumeratorCancellation] CancellationToken ct = default) where T : IBinaryInteger<T>
    {
        var listPool = new DefaultObjectPool<List<T>>(new ListPoolPolicy<T>());
        try
        {
            while (true)
            {
                var result = await reader.ReadAsync(ct).ConfigureAwait(false);
                var buffer = result.Buffer;

                SequenceReader<byte> entryReader;
                while (true)
                {
                    // Пытаемся разобрать одну запись; на недостатке данных — откат и дозагрузка.
                    entryReader = new SequenceReader<byte>(buffer);
                    if (!TryReadEntry(ref entryReader, out var entry, listPool, encoding, compareInfo, compareOptions))
                        break;
                    buffer = buffer.Slice(entryReader.Position);
                    yield return entry;
                }

                if (result.IsCompleted)
                {
                    if (!buffer.IsEmpty)
                        throw new FormatException("Truncated entry at end of chunk file");
                    reader.AdvanceTo(buffer.End, buffer.End);
                    yield break;
                }

                reader.AdvanceTo(buffer.Start, buffer.End);
            }
        }
        finally
        {
            await reader.CompleteAsync().ConfigureAwait(false);
        }

        static bool TryReadEntry(
            ref SequenceReader<byte> entryReader,
            out ReadChunkEntry<T> entry,
            ObjectPool<List<T>> listPool,
            Encoding encoding,
            CompareInfo compareInfo,
            CompareOptions compareOptions)
        {
            entry = null!;
            if (!Extensions.TryReadVarint(ref entryReader, out var textLength))
                return false;
            if (entryReader.Remaining < (long)textLength)
                return false;

            byte[]? textBuffer = null;
            byte[]? sortKeyBuffer = null;
            List<T>? values = null;
            char[]? rentedChars = null;
            try
            {
                // Текст: аренда из пула, сегмент хранит точную длину.
                textBuffer = ArrayPool<byte>.Shared.Rent((int)textLength);
                entryReader.TryCopyTo(textBuffer.AsSpan(0, (int)textLength));
                entryReader.Advance((long)textLength);

                if (!Extensions.TryReadVarint(ref entryReader, out var count))
                    return false;
                values = listPool.Get();
                T previous = T.Zero;
                for (ulong i = 0; i < count; i++)
                {
                    if (!Extensions.TryReadVarint(ref entryReader, out var delta))
                        return false;
                    previous += T.CreateChecked(delta);
                    values.Add(previous);
                }

                // Явный sort key: декодируем текст (кодировка файла) и вычисляем один раз.
                // Декодирующий буфер короткоживущий: stackalloc до 16КБ, больше — аренда.
                var maxChars = encoding.GetMaxCharCount((int)textLength);
                Span<char> chars = maxChars <= StackallocCharThreshold
                    ? stackalloc char[StackallocCharThreshold]
                    : (rentedChars = ArrayPool<char>.Shared.Rent(maxChars));
                var written = encoding.GetChars(textBuffer.AsSpan(0, (int)textLength), chars);
                var keyChars = chars[..written];
                sortKeyBuffer = ArrayPool<byte>.Shared.Rent(compareInfo.GetSortKeyLength(keyChars, compareOptions));
                var keyLength = compareInfo.GetSortKey(keyChars, sortKeyBuffer, compareOptions);

                entry = new ReadChunkEntry<T>(
                    new ArraySegment<byte>(textBuffer, 0, (int)textLength),
                    new ArraySegment<byte>(sortKeyBuffer, 0, keyLength),
                    values, listPool);

                // Владение перешло к записи.
                textBuffer = null;
                sortKeyBuffer = null;
                values = null;
                return true;
            }
            finally
            {
                if (textBuffer is not null) ArrayPool<byte>.Shared.Return(textBuffer);
                if (sortKeyBuffer is not null) ArrayPool<byte>.Shared.Return(sortKeyBuffer);
                if (values is not null) listPool.Return(values);
                if (rentedChars is not null) ArrayPool<char>.Shared.Return(rentedChars);
            }
        }
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