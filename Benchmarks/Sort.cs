using BenchmarkDotNet.Attributes;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using static System.Runtime.InteropServices.JavaScript.JSType;

public class Sort
{
    string[] lines;

    [GlobalSetup]
    public void Setup()
    {
        lines = File.ReadAllLines("testfile.txt");
    }

    [Benchmark(Baseline = true)]
    public void ArraySort()
    {
        Array.Sort(lines, StringComparer.Ordinal);
    }

    [Benchmark]
    public void RadixQuickSort()
    {
        RadixQuickSort(lines);
    }

    static void RadixQuickSort(Span<string> items, int depth = 0)
    {
        var n = items.Length;
        if (n <= 1) return;

        //Оптимизация для простых случаев
        if (n == 2)
        {
            SwapIfGreater(items, depth, 0, 1);
            return;
        }
        if (n == 3)
        {
            SwapIfGreater(items, depth, 0, 2);
            SwapIfGreater(items, depth, 1, 2);
            SwapIfGreater(items, depth, 0, 1);
            return;
        }

        // Разбиваем массив
        // 0..a - разряд совпадает с опорным
        // a..b - разряд меньше опрного
        // c..d - разряд больше опорного опрного
        // d..n - разряд совпадает с опорным
        var pivot = items[n / 2];
        var (a, b, c, d) = Partition(items, pivot, depth);

        // Перемещаем элементы у которых разряд совпадает с опорным в центр массива
        var r = Math.Min(a, b - a);
        VecSwap(items, 0, b - r, r);

        r = Math.Min(d - c, n - d - 1);
        VecSwap(items, b, n - r, r);

        // Рекурсивно сортируем все три части
        r = b - a;
        RadixQuickSort(items[..r], depth);
        RadixQuickSort(items.Slice(r, a + n - d - 1), depth + 1);
        r = d - c;
        RadixQuickSort(items.Slice(n - r, r), depth);
    }

    static (int, int, int, int) Partition(Span<string> items, ReadOnlySpan<char> pivot, int depth)
    {
        var a = 0;
        var b = a;
        var c = items.Length - 1;
        var d = c;
        while (true)
        {
            int cmp;
            while (b <= c && (cmp = SortKeyCompare(items[b], pivot, depth)) <= 0)
            {
                if (cmp == 0)
                {
                    Swap(items, a, b);
                    a++;
                }
                b++;
            }
            while (b <= c && (cmp = SortKeyCompare(items[c], pivot, depth)) >= 0)
            {
                if (cmp == 0)
                {
                    Swap(items, c, d);
                    d--;
                }
                c--;
            }

            if (b > c) break;

            Swap(items, b, c);
            b++;
            c--;
        }
        return (a, b, c, d);
    }

    static void SwapIfGreater(System.Span<string> items, int depth, int i, int j)
    {
        if (SortKeyCompareToEnd<char>(items[i], items[j], depth) > 0)
        {
            Swap(items, i, j);
        }
    }

    static int SortKeyCompare(ReadOnlySpan<char> a, ReadOnlySpan<char> b, int depth)
    {
        Debug.Assert(a.Length >= depth);
        Debug.Assert(b.Length >= depth);
        if (a.Length == depth && b.Length == depth) return 0;
        if (a.Length == depth) return -1;
        if (b.Length == depth) return 1;
        return a[depth].CompareTo(b[depth]);
    }

    static int SortKeyCompareToEnd<T>(ReadOnlySpan<T> a, ReadOnlySpan<T> b, int depth) where T : struct, IComparable<T>
    {
        return a[depth..].SequenceCompareTo(b[depth..]);
    }

    static void Swap<T>(Span<T> items, int i, int j)
    {
        (items[j], items[i]) = (items[i], items[j]);
    }

    static void VecSwap<T>(Span<T> items, int i, int j, int n)
    {
        while (n-- > 0)
        {
            Swap(items, i++, j);
        }
    }



}
