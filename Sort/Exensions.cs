public static class Exensions
{
    public static int ReadBlock(this Stream stream, byte[] buffer, int stratIndex, int count, out bool eof)
    {
        var totalBytesRead = 0;
        eof = false;
        while (totalBytesRead < count)
        {
            var charsRead = stream.Read(buffer, stratIndex + totalBytesRead, count - totalBytesRead);
            if (charsRead == 0)
            {
                eof = true;
                break;
            }
            totalBytesRead += charsRead;
        }
        return totalBytesRead;
    }

    public static IEnumerable<T> Merge<T>(this IEnumerable<IEnumerable<T>> sources, IComparer<T>? comparer = default)
    {
        var heap = (from source in sources
                    let e = source.GetEnumerator()
                    where e.MoveNext()
                    select e).ToArray();
        try
        {
            var enumeratorComparer = new EnumeratorComparer<T>(comparer ?? Comparer<T>.Default);
            heap.AsSpan().BuildHeap(enumeratorComparer);

            while (true)
            {
                var min = heap[0];
                yield return min.Current;
                if (!min.MoveNext())
                {
                    min.Dispose();
                    if (heap.Length == 1) yield break;
                    heap[0] = heap[^1];
                    Array.Resize(ref heap, heap.Length - 1);
                }
                heap.AsSpan().Heapify(0, enumeratorComparer);
            }
        }
        finally
        {
            foreach (var e in heap)
            {
                e.Dispose();
            }
        }
    }

    private record EnumeratorComparer<T>(IComparer<T> comparer) : IComparer<IEnumerator<T>>
    {
        public int Compare(IEnumerator<T>? x, IEnumerator<T>? y)
        {
            return comparer.Compare(x!.Current, y!.Current);
        }
    }

    #region Heap
    public static void Heapify<T>(this Span<T> heap, int index, IComparer<T> comparer)
    {
        ArgumentNullException.ThrowIfNull(comparer);

        var min = index;
        while (true)
        {
            var leftChild = 2 * index + 1;
            var rightChild = 2 * index + 2;
            var v = heap[index];

            if (rightChild < heap.Length && comparer.Compare(v, heap[rightChild]) > 0)
            {
                min = rightChild;
                v = heap[min];
            }

            if (leftChild < heap.Length && comparer.Compare(v, heap[leftChild]) > 0)
            {
                min = leftChild;
            }

            if (min == index) break;

            var temp = heap[index];
            heap[index] = heap[min];
            heap[min] = temp;

            index = min;
        }
    }

    public static void BuildHeap<T>(this Span<T> heap, IComparer<T> comparer)
    {
        ArgumentNullException.ThrowIfNull(comparer);

        for (int i = heap.Length / 2; i >= 0; i--)
        {
            Heapify(heap, i, comparer);
        }
    }
    #endregion
}
