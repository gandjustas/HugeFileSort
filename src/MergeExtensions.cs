using System.Runtime.CompilerServices;

static class MergeExtensions
{
    /// <summary>
    /// N-way merge по сортированным источникам: группы равных ключей собираются через
    /// tryAdd (группа сама сравнивает с ключом группы и материализует запись в свой буфер);
    /// merge возвращает результат группы, после чего группа диспо́зится.
    /// </summary>
    public static async IAsyncEnumerable<V> MergeSorted<T, V>(
        this IEnumerable<IAsyncEnumerable<T>> sources,
        IComparer<T> comparer,
        Func<V> newMergeGroup,
        Func<V, T, bool> tryAddToMergeGroup,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        List<IAsyncEnumerator<T>> enumerators = [.. sources.Select(s => s.GetAsyncEnumerator(ct))];
        // Полёт: MoveNext-задачи источников, чья текущая запись отдана в группу/хип.
        // Живёт между группами: незавершённые задачи дожидаются в следующих группах.
        List<(IAsyncEnumerator<T> Enumerator, Task<bool> Task)> flight = [];
        try
        {
            await foreach (var x in MergeSortedWithPriorityQueue(enumerators, comparer, newMergeGroup, tryAddToMergeGroup, flight, ct).ConfigureAwait(false))
            {
                yield return x;
            }
        }
        finally
        {
            // Досрочно выходим (ошибка/отмена/конец по ct): дожидаемся полёт, затем энумераторы.
            foreach (var (_, task) in flight)
            {
                try { await task.ConfigureAwait(false); }
                catch { /* источник уже упал — ошибка и так увидена мержем */ }
            }
            foreach (var e in enumerators)
            {
                await e.DisposeAsync().ConfigureAwait(false);
            }
        }
    }

    static async IAsyncEnumerable<V> MergeSortedWithPriorityQueue<T, V>(
        List<IAsyncEnumerator<T>> enumerators,
        IComparer<T> comparer,
        Func<V> newMergeGroup,
        Func<V, T, bool> tryAddToMergeGroup,
        List<(IAsyncEnumerator<T> Enumerator, Task<bool> Task)> flight,
        [EnumeratorCancellation] CancellationToken ct)
    {
        // Параллельный первичный наполн: каждый источник ждёт свой пайп независимо.
        var initial = new Task<bool>[enumerators.Count];
        for (var i = 0; i < enumerators.Count; i++)
            initial[i] = enumerators[i].MoveNextAsync().AsTask();
        await Task.WhenAll(initial).ConfigureAwait(false);

        PriorityQueue<IAsyncEnumerator<T>, T> pq = new(enumerators.Count, comparer);
        for (var i = 0; i < enumerators.Count; i++)
        {
            if (initial[i].Result)
                pq.Enqueue(enumerators[i], enumerators[i].Current);
        }

        while (pq.TryDequeue(out var current, out _))
        {
            ct.ThrowIfCancellationRequested();
            var group = newMergeGroup();
            if (!tryAddToMergeGroup(group, current.Current))
                throw new InvalidOperationException("New group rejected the first entry");
            Advance(current);

            var more = true;
            while (more)
            {
                more = false;
                // Сбор равных из хипа: группа сравнивает с ключом группы; не равная остаётся в хипе.
                while (pq.TryPeek(out var e, out _) && tryAddToMergeGroup(group, e.Current))
                {
                    pq.Dequeue();
                    Advance(e);
                    more = true;
                }
                if (flight.Count > 0)
                {
                    // Ждём завершившиеся MoveNext; результат true — запись в хип (может быть равной).
                    var snapshot = flight.Select(pair => pair.Task).ToArray();
                    await foreach (var task in Task.WhenEach(snapshot).WithCancellation(ct).ConfigureAwait(false))
                    {
                        IAsyncEnumerator<T>? enumerator = null;
                        flight.RemoveAll(pair =>
                        {
                            if (pair.Task != task) return false;
                            enumerator ??= pair.Enumerator;
                            return true;
                        });
                        if (enumerator is not null && await task.ConfigureAwait(false))
                            pq.Enqueue(enumerator, enumerator.Current);
                    }
                    more = true;
                }
            }
            yield return group;
        }

        yield break;

        // MoveNext источника. Синхронное завершение разбирается сразу (в pq или в сброс) —
        // в полёт уходит только отложенный MoveNext: у async-итераторов синхронно завершённые
        // MoveNextAsync().AsTask() возвращают один общий кэшированный Task (FromResult),
        // тождество которого нельзя использовать как ключ.
        void Advance(IAsyncEnumerator<T> enumerator)
        {
            ValueTask<bool> vt = enumerator.MoveNextAsync();
            if (vt.IsCompletedSuccessfully)
            {
                if (vt.Result)
                    pq.Enqueue(enumerator, enumerator.Current);
            }
            else
            {
                flight.Add((enumerator, vt.AsTask()));
            }
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
