using System.Diagnostics;

public static class SortHelpers

{
    #region RadixQuickSort
    public interface ISortKeySelector<T, V> where V : struct, IComparable<V>
    {
        ReadOnlySpan<V> GetSortKey(T source);
    }

    private class StringSortKeySelector : ISortKeySelector<string, char>
    {
        public ReadOnlySpan<char> GetSortKey(string source)
        {
            return source.AsSpan();
        }
    }

    private class MemorySortKeySelector : ISortKeySelector<ReadOnlyMemory<char>, char>
    {
        public ReadOnlySpan<char> GetSortKey(ReadOnlyMemory<char> source)
        {
            return source.Span;
        }
    }

    public static void RadixQuickSort(Span<string> items)
    {
        RadixQuickSort(items, new StringSortKeySelector(), 0);
    }
    public static void RadixQuickSort(Span<ReadOnlyMemory<char>> items)
    {
        RadixQuickSort(items, new MemorySortKeySelector(), 0);
    }
    public static void RadixQuickSort<T, V>(Span<T> items, ISortKeySelector<T, V> selector)
        where V : struct, IComparable<V>
    {
        RadixQuickSort(items, selector, 0);
    }

    static void RadixQuickSort<T, V>(Span<T> items, ISortKeySelector<T, V> selector, int depth)
        where V : struct, IComparable<V>
    {
        var n = items.Length;
        if (n <= 1) return;

        //Оптимизация для простых случаев
        if (n == 2)
        {
            SwapIfGreater(items, selector, depth, 0, 1);
            return;
        }
        if (n == 3)
        {
            SwapIfGreater(items, selector, depth, 0, 2);
            SwapIfGreater(items, selector, depth, 1, 2);
            SwapIfGreater(items, selector, depth, 0, 1);
            return;
        }

        // Разбиваем массив
        // 0..a - разряд совпадает с опорным
        // a..b - разряд меньше опрного
        // c..d - разряд больше опорного опрного
        // d..n - разряд совпадает с опорным
        var pivot = selector.GetSortKey(items[n / 2]);
        var (a, b, c, d) = Partition(items, selector, pivot, depth);

        // Перемещаем элементы у которых разряд совпадает с опорным в центр массива
        var r = Math.Min(a, b - a);
        VecSwap(items, 0, b - r, r);

        r = Math.Min(d - c, n - d - 1);
        VecSwap(items, b, n - r, r);

        // Рекурсивно сортируем все три части
        r = b - a;
        RadixQuickSort(items[..r], selector, depth);
        RadixQuickSort(items.Slice(r, a + n - d - 1), selector, depth + 1);
        r = d - c;
        RadixQuickSort(items.Slice(n - r, r), selector, depth);
    }

    static (int, int, int, int) Partition<T, V>(Span<T> items, ISortKeySelector<T, V> selector, ReadOnlySpan<V> pivot, int depth)
        where V : struct, IComparable<V>
    {
        var a = 0;
        var b = a;
        var c = items.Length - 1;
        var d = c;
        while (true)
        {
            int cmp;
            while (b <= c && (cmp = SortKeyCompare<V>(selector.GetSortKey(items[b]), pivot, depth)) <= 0)
            {
                if (cmp == 0)
                {
                    Swap(ref items[a], ref items[b]);
                    a++;
                }
                b++;
            }
            while (b <= c && (cmp = SortKeyCompare<V>(selector.GetSortKey(items[c]), pivot, depth)) >= 0)
            {
                if (cmp == 0)
                {
                    Swap(ref items[c], ref items[d]);
                    d--;
                }
                c--;
            }

            if (b > c) break;

            Swap(ref items[b], ref items[c]);
            b++;
            c--;
        }
        return (a, b, c, d);
    }

    static void SwapIfGreater<T, V>(System.Span<T> items, ISortKeySelector<T, V> selector, int depth, int i, int j)
        where V : struct, IComparable<V>
    {
        if (SortKeyCompareToEnd(selector.GetSortKey(items[i]), selector.GetSortKey(items[j]), depth) > 0)
        {
            Swap(ref items[i], ref items[j]);
        }
    }

    static int SortKeyCompare<V>(ReadOnlySpan<V> a, ReadOnlySpan<V> b, int depth) where V : struct, IComparable<V>
    {
        Debug.Assert(a.Length >= depth);
        Debug.Assert(b.Length >= depth);
        if (a.Length == depth && b.Length == depth) return 0;
        if (a.Length == depth) return -1;
        if (b.Length == depth) return 1;
        return a[depth].CompareTo(b[depth]);
    }

    static int SortKeyCompareToEnd<V>(ReadOnlySpan<V> a, ReadOnlySpan<V> b, int depth) where V : struct, IComparable<V>
    {
        return a[depth..].SequenceCompareTo(b[depth..]);
    }

    static void Swap<V>(ref V a, ref V b)
    {
        V temp = a;
        a = b;
        b = temp;
    }

    static void VecSwap<T>(Span<T> items, int i, int j, int n)
    {
        while (n-- > 0)
        {
            Swap(ref items[i++], ref items[j++]);
        }
    }

    #endregion


}
