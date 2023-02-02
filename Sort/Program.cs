using System.IO;
using System.Reflection.PortableExecutable;

if ((args?.Length ?? 0) == 0)
{
    Console.WriteLine($"Usage: {Path.GetFileNameWithoutExtension(Environment.ProcessPath)} <file path> [<approximate chunk size>]");
    return -1;
}

var file = args![0];
var chunkSize = args?.Length > 1 ? (int.Parse(args[1]) * 1000_000 / 2) : 100_000_000; //In characters

var comparer = new Comparer(StringComparison.CurrentCulture);

var sw = System.Diagnostics.Stopwatch.StartNew();
List<string> tempFiles = new();
List<Item> chunk = new();
using (var reader = File.OpenText(file))
{
    var chunkBuffer = new char[chunkSize];
    var chunkReadPosition = 0;
    var eos = reader.EndOfStream;
    while (!eos)
    {
        // Читаем из файла весь буфер
        var charsRead = reader.ReadBlock(chunkBuffer.AsSpan(chunkReadPosition));
        eos = reader.EndOfStream;
        var m = chunkBuffer.AsMemory(0, chunkReadPosition + charsRead);

        // Заполняем список строк ReadOnlyMemory<char> для сортировки
        int linePos;
        while ((linePos = m.Span.IndexOf(Environment.NewLine)) >= 0 || (eos && m.Length > 0))
        {
            var line = linePos >= 0 ? m[..linePos] : m;
            chunk.Add(new Item(line, line.Span.IndexOf('.')));
            m = m[(linePos + Environment.NewLine.Length)..];
        }

        chunk.Sort(comparer);

        // Записываем строки из отсортированного списка во временный файл
        var tempFileName = Path.ChangeExtension(file, $".part-{tempFiles.Count}" + Path.GetExtension(file));
        using (var tempFile = File.CreateText(tempFileName))
        {
            foreach (var (l, _) in chunk)
            {
                tempFile.WriteLine(l);
            }
        }
        tempFiles.Add(tempFileName);

        if (eos) break;
        chunk.Clear();

        //Отсток буфера переносим в начало
        m.CopyTo(chunkBuffer);
        chunkReadPosition = m.Length;
    }
}

Console.WriteLine($"SplitSort done in {sw.Elapsed}");
sw.Restart();

try
{
    var mergedLines = tempFiles
        .Select(f => File.ReadLines(f).Select(s => new Item(s.AsMemory(), s.IndexOf('.')))) // Читаем построчно все файлы, находим в строках точку
        .Merge(comparer);  //Слияние итераторов IEnumerable<IEnumerable<T>> в IEnumerable<T>
    using var sortedFile = File.CreateText(Path.ChangeExtension(file, ".sorted" + Path.GetExtension(file)));

    foreach (var (l, _) in mergedLines)
    {
        sortedFile.WriteLine(l);
    }
}
finally
{
    tempFiles.ForEach(File.Delete);
}
Console.WriteLine($"Merge done in {sw.Elapsed}");

return 0;

public record struct Item(ReadOnlyMemory<char> Line, int DotPosition);
public record Comparer(StringComparison StringComparison) : IComparer<Item>
{
    public int Compare(Item x, Item y)
    {
        var spanX = x.Line.Span;
        var spanY = y.Line.Span;
        var xDot = x.DotPosition;
        var yDot = y.DotPosition;

        var cmp = spanX[(xDot + 2)..].CompareTo(spanY[(yDot + 2)..], StringComparison);
        if (cmp != 0) return cmp;
        return int.Parse(spanX[..xDot]) - int.Parse(spanY[..yDot]);
    }
}