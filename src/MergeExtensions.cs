using System.Runtime.CompilerServices;
using Microsoft.Extensions.ObjectPool;

static class MergeExtensions
{
    /// <summary>
    /// N-way merge сортированных источников записей: равные ключи собираются в MergeGroup
    /// (группа сама сравнивает пришедшую запись с ключом группы и материализует её в свой
    /// буфер), готовая группа отдаётся наружу — после записи вызывающий возвращает её в пул.
    /// </summary>
    public static async IAsyncEnumerable<MergeGroup> MergeSorted(
        this IEnumerable<IAsyncEnumerable<ChunkEntry>> sources,
        IComparer<ChunkEntry> comparer,
        ObjectPool<MergeGroup> groupPool,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        List<IAsyncEnumerator<ChunkEntry>> enumerators = [.. sources.Select(s => s.GetAsyncEnumerator(ct))];
        // Полёт: MoveNext-задачи источников, чья текущая запись отдана в группу/хип.
        // Живёт между группами: незавершённые задачи дожидаются в следующих группах.
        List<(IAsyncEnumerator<ChunkEntry> Enumerator, Task<bool> Task)> flight = [];
        try
        {
            // Параллельный первичный наполн: каждый источник ждёт свой пайп независимо.
            var initial = new Task<bool>[enumerators.Count];
            for (var i = 0; i < enumerators.Count; i++)
                initial[i] = enumerators[i].MoveNextAsync().AsTask();
            await Task.WhenAll(initial).ConfigureAwait(false);

            PriorityQueue<IAsyncEnumerator<ChunkEntry>, ChunkEntry> pq = new(enumerators.Count, comparer);
            for (var i = 0; i < enumerators.Count; i++)
            {
                if (initial[i].Result)
                    pq.Enqueue(enumerators[i], enumerators[i].Current);
            }

            while (pq.TryDequeue(out var current, out _))
            {
                ct.ThrowIfCancellationRequested();
                var group = groupPool.Get();
                group.TryAdd(current.Current);
                Advance(current);

                var more = true;
                while (more)
                {
                    more = false;
                    // Сбор равных из хипа: группа сравнивает с ключом группы; не равная остаётся в хипе.
                    while (pq.TryPeek(out var e, out _) && group.TryAdd(e.Current))
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
                            var index = flight.FindIndex(pair => pair.Task == task);
                            var enumerator = flight[index].Enumerator;
                            if (await task.ConfigureAwait(false))
                                pq.Enqueue(enumerator, enumerator.Current);
                        }
                        // Раунд ждёт ровно снимок полёта — после него завершены все его задачи.
                        flight.RemoveAll(static pair => pair.Task.IsCompleted);
                    }
                }
                yield return group;
            }

            // MoveNext источника. Синхронное завершение разбирается сразу (в pq) — в полёт
            // уходит только отложенный MoveNext: у async-итераторов синхронно завершённые
            // MoveNextAsync().AsTask() возвращают один общий кэшированный Task (FromResult),
            // тождество которого нельзя использовать как ключ.
            void Advance(IAsyncEnumerator<ChunkEntry> enumerator)
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
        finally
        {
            // Ошибка/отмена/штатный конец: дожидаемся полёт (источник мог упасть — ошибка и
            // так увидена мержем), затем энумераторы.
            foreach (var (_, task) in flight)
            {
                try { await task.ConfigureAwait(false); }
                catch { /* причина — в исходной ошибке мержа */ }
            }
            foreach (var e in enumerators)
                await e.DisposeAsync().ConfigureAwait(false);
        }
    }
}
