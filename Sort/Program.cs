if ((args?.Length ?? 0) == 0)
{
    Console.WriteLine($"Usage: {Path.GetFileNameWithoutExtension(Environment.ProcessPath)} <file path> [<approximate chunk size>]");
    return -1;
}

var file = args![0];
var chunkSize = args?.Length > 1 ? (int.Parse(args[1]) * 1000_000 / 2) : (100_000_000); //In characters

var comparer = new Comparer(StringComparison.CurrentCulture);

var sw = System.Diagnostics.Stopwatch.StartNew();
List<string> tempFiles = new();
List<(ReadOnlyMemory<char>, int)> chunk = new();
using (var stream = File.OpenText(file))
{
    var chunkBuffer = new char[chunkSize];
    var chunkReadPosition = 0;
    while (true)
    {
        // Читаем из файла весь буфер
        var charsRead = stream.ReadBlock(chunkBuffer, chunkReadPosition, chunkSize - chunkReadPosition);
        var m = chunkBuffer.AsMemory(0, chunkReadPosition + charsRead);

        // Заполняем список строк ReadOnlyMemory<char> для сортировки
        int linePos;        
        while ((linePos = m.Span.IndexOf(Environment.NewLine)) >= 0)
        {
            var line = m[..linePos];
            chunk.Add((line, line.Span.IndexOf('.')));
            m = m[(linePos + Environment.NewLine.Length)..];
        }

        // Если это был конец файла, то добавим в список последнюю строку, если она не пустая
        if (stream.EndOfStream && m.Length > 0)
        {
            chunk.Add((m, m.Span.IndexOf('.')));
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
        
        if (stream.EndOfStream) break;
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
        .Select(f => File.ReadLines(f).Select(s => (s.AsMemory(), s.IndexOf('.')))) // Читаем построчно все файлы, находим в строках точку
        .Merge(comparer);  //Слияние итераторов IEnumerable<IEnumerable<T>> в IEnumerable<T>
    using var sortedFile = File.CreateText(Path.ChangeExtension(file, ".sorted" + Path.GetExtension(file)));
    foreach (var (l, _) in mergedLines)
    {
        sortedFile.WriteLine(l);
    }
}
finally
{
    tempFiles.ForEach(f => File.Delete(f));
}
Console.WriteLine($"Merge done in {sw.Elapsed}");

return 0;

public record Comparer(StringComparison stringComparison) : IComparer<(ReadOnlyMemory<char>, int)>
{
    public int Compare((ReadOnlyMemory<char>, int) x, (ReadOnlyMemory<char>, int) y)
    {
        var spanX = x.Item1.Span;
        var spanY = y.Item1.Span;
        var xDot = x.Item2;
        var yDot = y.Item2;

        var cmp = spanX[(xDot + 2)..].CompareTo(spanY[(yDot + 2)..], stringComparison);
        if (cmp != 0) return cmp;
        return int.Parse(spanX[..xDot]) - int.Parse(spanY[..yDot]);
    }
}