using Sort;
using System.Buffers;
using System.Buffers.Binary;
using System.Diagnostics;
using System.Globalization;
using System.Net.Security;
using System.Runtime.Serialization;

if ((args?.Length ?? 0) == 0)
{
    Console.WriteLine($"Usage: {Path.GetFileNameWithoutExtension(Environment.ProcessPath)} <file path> [<chunk size>]");
    return -1;
}

var file = args![0];
var chunkSize = args?.Length > 1 ? int.Parse(args[1]) : 1_000_000;

var comparer = new Comparer(StringComparison.CurrentCulture);

var sw = Stopwatch.StartNew();

var culture = CultureInfo.CurrentCulture;
var fileCount = 0;
var tempFiles =
    File.ReadLines(file)  // Читаем построчно
        .Chunk(chunkSize) // Разбиваем на куски по 1М строк
        .Select(chunk =>
        {
            var pairs = ArrayPool<MySortKey>.Shared.Rent(chunkSize);
            using var pooledMemory = MemoryPool<byte>.Shared.Rent(chunkSize * 256);
            var m = pooledMemory.Memory;
            var lineCount = 0;
            foreach (var line in chunk)
            {
                var dot = line.IndexOf('.');

                // Обращение к KeyData вызывает Array.Clone на каждый вызов, поэтому наямую сортировать SortKey очень долго
                // Получим массив один раз и сохраним отдельно
                var l = line.AsSpan(dot + 2);
                var sortKeyLength = culture.CompareInfo.GetSortKey(l, m.Span);


                // Дописываем число из начала строки в конец массива ключа
                // Отдельная сортировка по числу будет не нужна
                var x = int.Parse(line.AsSpan(0, dot));
                BinaryPrimitives.WriteInt32LittleEndian(m.Slice(sortKeyLength).Span, x);

                //Сохраняем в массив
                var len = sortKeyLength + sizeof(uint);
                pairs[lineCount++] = new MySortKey(line, m[..len]);
                m = m[len..];
            }
            RadixQuickSort<MySortKey, byte>(pairs.AsSpan(0, lineCount));

            // Сохраняем отсортированные строки в файл
            var tempFileName = Path.ChangeExtension(file, $".part-{fileCount++}" + Path.GetExtension(file));
            File.WriteAllLines(tempFileName, pairs.Take(lineCount).Select(x => x.OriginalString));
            return tempFileName;
        }).ToList();



Console.WriteLine($"SplitSort done in {sw.Elapsed}");
sw.Restart();

try
{
    var mergedLines = tempFiles
        .Select(f => File.ReadLines(f).Select(s => (s, s.IndexOf('.')))) // Читаем построчно все файлы, находим в строках точку
        .Merge(comparer)  //Слияние итераторов IEnumerable<IEnumerable<(string,int)>> в IEnumerable<(string,int)>
        .Select(x => x.Item1); // Оставляем только строки
    File.WriteAllLines(Path.ChangeExtension(file, ".sorted" + Path.GetExtension(file)), mergedLines);
}
finally
{
    tempFiles.ForEach(f => File.Delete(f));
}
Console.WriteLine($"Merge done in {sw.Elapsed}");

return 0;

static void RadixQuickSort<T, V>(Span<T> items, int depth = 0)
    where T : struct, IHasSortKey<V>
    where V : struct, IComparable<V>
{
    var n = items.Length;
    if (n <= 1) return;

    //Оптимизация для простых случаев
    if (n == 2)
    {
        SwapIfGreater<T, V>(items, depth, 0, 1);
        return;
    }
    if (n == 3)
    {
        SwapIfGreater<T, V>(items, depth, 0, 2);
        SwapIfGreater<T, V>(items, depth, 1, 2);
        SwapIfGreater<T, V>(items, depth, 0, 1);
        return;
    }

    // Разбиваем массив
    // 0..a - разряд совпадает с опорным
    // a..b - разряд меньше опрного
    // c..d - разряд больше опорного опрного
    // d..n - разряд совпадает с опорным
    var pivot = items[n / 2].GetSortKey();
    var (a, b, c, d) = Partition(items, pivot, depth);

    // Перемещаем элементы у которых разряд совпадает с опорным в центр массива
    var r = Math.Min(a, b - a);
    VecSwap(items, 0, b - r, r);

    r = Math.Min(d - c, n - d - 1);
    VecSwap(items, b, n - r, r);

    // Рекурсивно сортируем все три части
    r = b - a;
    RadixQuickSort<T, V>(items[..r], depth);
    RadixQuickSort<T, V>(items.Slice(r, a + n - d - 1), depth + 1);
    r = d - c;
    RadixQuickSort<T, V>(items.Slice(n - r, r), depth);

    static (int, int, int, int) Partition<T, V>(Span<T> items, Span<V> pivot, int depth)
        where T : struct, IHasSortKey<V>
        where V : struct, IComparable<V>
    {
        var a = 0;
        var b = a;
        var c = items.Length - 1;
        var d = c;
        var cmp = 0;

        while (true)
        {
            while (b <= c && (cmp = SortKeyCompare<V>(items[b].GetSortKey(), pivot, depth)) <= 0)
            {
                if (cmp == 0)
                {
                    Swap(ref items[a], ref items[b]);
                    a++;
                }
                b++;
            }
            while (b <= c && (cmp = SortKeyCompare<V>(items[c].GetSortKey(), pivot, depth)) >= 0)
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

    static void SwapIfGreater<T, V>(System.Span<T> items, int depth, int i, int j)
        where T : struct, IHasSortKey<V>
        where V : struct, IComparable<V>
    {
        if (SortKeyCompareToEnd(items[i].GetSortKey(), items[j].GetSortKey(), depth) > 0)
        {
            Swap(ref items[i], ref items[j]);
        }
    }

    static int SortKeyCompare<V>(Span<V> a, Span<V> b, int depth) where V : struct, IComparable<V>
    {
        Debug.Assert(a.Length >= depth);
        Debug.Assert(b.Length >= depth);
        if (a.Length == depth && b.Length == depth) return 0;
        if (a.Length == depth) return -1;
        if (b.Length == depth) return 1;
        return a[depth].CompareTo(b[depth]);
    }

    static int SortKeyCompareToEnd<V>(Span<V> a, Span<V> b, int depth) where V : struct, IComparable<V>
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
}

public record Comparer(StringComparison stringComparison) : IComparer<(string, int)>
{
    public int Compare((string, int) x, (string, int) y)
    {
        var spanX = x.Item1.AsSpan();
        var spanY = y.Item1.AsSpan();
        var xDot = x.Item2;
        var yDot = y.Item2;

        var cmp = spanX[(xDot + 2)..].CompareTo(spanY[(yDot + 2)..], stringComparison);
        if (cmp != 0) return cmp;
        return int.Parse(spanX[..xDot]) - int.Parse(spanY[..yDot]);
    }
}

public record struct MySortKey(string OriginalString, Memory<byte> mem) : IHasSortKey<byte>
{
    public Span<byte> GetSortKey()
    {
        Debug.Assert(!mem.IsEmpty); // Метод не должен вызываться для неицициализированной структуры
        return mem.Span;
    }
}
