using BenchmarkDotNet.Attributes;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using static System.Runtime.InteropServices.JavaScript.JSType;

public class KeySort
{
    ReadOnlyMemory<byte>[] keys;

    [GlobalSetup]
    public void Setup()
    {
        var lines = File.ReadLines("testfile.txt");
        keys = lines.Select(l => (ReadOnlyMemory<byte>)CultureInfo.CurrentCulture.CompareInfo.GetSortKey(l).KeyData.AsMemory()).ToArray();
    }

    [Benchmark(Baseline = true)]
    public void ArraySort()
    {
        Array.Sort(keys, (a, b) => a.Span.SequenceCompareTo(b.Span));
    }

    [Benchmark]
    public void CountingRadixSort()
    {
        CountingRadixSort(keys, new ReadOnlyMemory<byte>[keys.Length]);
    }

    private static void CountingRadixSort(Span<ReadOnlyMemory<byte>> items, Span<ReadOnlyMemory<byte>> output, int depth=0)
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

        // Сохраняем количество значений байта
        // +1 чтобы вместить byte.MaxValue, +1 чтобы вместить конец массива
        Span<int> count = stackalloc int[byte.MaxValue + 1 + 1];

        foreach (var item in items)
        {
            var key = item.Span;
            ++count[depth < key.Length ? (key[depth] + 1) : 0];
        }

        // Изменяем count[i] чтобы он содержал 
        // смещение для результируюющего массива
        Span<int> positions = stackalloc int[byte.MaxValue + 1 + 1];
        count.CopyTo(positions);
        for (int i = 1; i < count.Length; ++i)
        {
            count[i] += count[i - 1];
        }

        // Формируем результирующий массив
        for (int i = items.Length - 1; i >= 0; i--)
        {
            var item = items[i];
            var key = item.Span;
            var r = depth < key.Length ? (key[depth] + 1) : 0;
            output[--count[r]] = item;
        }

        output.CopyTo(items);
        var offset = positions[0];
        for (int i = 1; i < positions.Length; i++)
        {
            if (positions[i] > 0)
            {
                CountingRadixSort(items.Slice(offset,positions[i]), output.Slice(offset, positions[i]), depth + 1);
                offset += positions[i];
            }
        }
    }


    static void SwapIfGreater(Span<ReadOnlyMemory<byte>> items, int depth, int i, int j)
    {
        if (SortKeyCompareToEnd(items[i].Span, items[j].Span, depth) > 0)
        {
            Swap(items, i, j);
        }
    }

    static int SortKeyCompareToEnd<T>(ReadOnlySpan<T> a, ReadOnlySpan<T> b, int depth) where T : struct, IComparable<T>
    {
        return a[depth..].SequenceCompareTo(b[depth..]);
    }

    static void Swap<T>(Span<T> items, int i, int j)
    {
        (items[j], items[i]) = (items[i], items[j]);
    }
}
