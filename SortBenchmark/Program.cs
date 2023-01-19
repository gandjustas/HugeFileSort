using SortBenckmark;
using System;
using System.Buffers;
using System.Diagnostics;
using System.Net.Security;

var lines = File.ReadAllLines(args[0]);
//var lines = new[] {
//"415. Apple",
//"30432. Something something something",
//"1. Apple",
//"32. Cherry is the best",
//"2. Banana is yellow",
//};
//var lines1 = (string[])lines.Clone();
var comparer = new Comparer(StringComparison.Ordinal);

var sw = Stopwatch.StartNew();
Array.Sort(lines, comparer);
Console.WriteLine($"Array.Sort completed in {sw.Elapsed}");

lines = File.ReadAllLines(args[0]);
sw.Restart();
RadixSort(lines);
Console.WriteLine($"Radix sort completed in {sw.Elapsed}");

void RadixSort(string[] lines)
{
    var maxChar = '\0';
    var linesWithDotPos = new (string, int)[lines.Length];
    for (int i = 0; i < lines.Length; i++)
    {
        var line = lines[i];
        var dot = line.IndexOf('.');
        foreach(var c in line.AsSpan(dot + 2))
        {
            maxChar = (c > maxChar) ? c : maxChar;
        }
        linesWithDotPos[i] = (line, dot);
    }
    RadixSortIntl(linesWithDotPos.AsSpan(), maxChar, 0);
}

void RadixSortIntl(Span<(string, int)> linesWithDotPos, char maxChar, int charPos)
{
    Span<int> count = stackalloc int[(int)maxChar + 1];
    count.Clear();

    // Заполняем счетчик символов
    foreach (var (line,dot) in linesWithDotPos)
    {
        var idx = dot + 2 + charPos;
        if(idx < line.Length)
        {
            count[line[idx]]++;            
        } 
        else
        {
            count[0]++;
        }
    }

    // Сохраняем размеры полученных "корзин"
    int binCount = 0;
    var sortStrings = false;
    for (int i = 0; i < count.Length; i++)
    {
        if (count[i] > 0)
        {
            binCount++;
            if (i > 0) sortStrings = true;
        }
    }


    // Сохраняем корзины
    Span<int> bins = stackalloc int[binCount];  
    for (int i = 0, j =0; i < count.Length; i++)
    {
        if (count[i] > 0) bins[j++] = count[i];
    }

    // Сортировка по числу в начале строки если все строки попали в корзину \0
    if (binCount == 1)
    {
        if (!sortStrings)
        {
            linesWithDotPos.Sort( (x, y) => 
                int.Parse(x.Item1.AsSpan().Slice(0, x.Item2))
                    .CompareTo(
                        int.Parse(y.Item1.AsSpan().Slice(0, y.Item2))
                    )
            );
            return;
        }
    }

    // Получение индексов для корзин
    for (int i = 1; i < count.Length; i++)
    {
        count[i] += count[i - 1];
    }

    // Заполннеие массива результатов
    var output = ArrayPool<(string, int)>.Shared.Rent(linesWithDotPos.Length);    
    for (int i = linesWithDotPos.Length - 1; i >=0; i--)
    {
        var item = linesWithDotPos[i];
        var (line,dot) = item;
        var idx = dot + 2 + charPos;
        var pos = (idx < line.Length) ? line[idx] : 0;
        output[count[pos] - 1] = item;
        count[pos]--;
    }

    output.AsSpan(0..linesWithDotPos.Length).CopyTo(linesWithDotPos);
    ArrayPool<(string, int)>.Shared.Return(output);

    foreach (var bin in bins)
    {
        if (bin > 1)
        {
            RadixSortIntl(linesWithDotPos.Slice(0, bin), maxChar, charPos + 1);
        }
        linesWithDotPos = linesWithDotPos.Slice(bin);
    }
}