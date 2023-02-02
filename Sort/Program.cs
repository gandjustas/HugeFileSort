if ((args?.Length ?? 0) == 0)
{
    Console.WriteLine($"Usage: {Path.GetFileNameWithoutExtension(Environment.ProcessPath)} <file path> [<chunk size>]");
    return -1;
}

var file = args![0];
var chunkSize = args?.Length > 1 ? int.Parse(args[1]) : 1_000_000;

var comparer = new Comparer(StringComparison.InvariantCultureIgnoreCase);

var sw = System.Diagnostics.Stopwatch.StartNew();

var count = 0;
var tempFiles =
    File.ReadLines(file)                    // Читаем построчно
        .Select(s => new Item(s, s.IndexOf('.')))   // Находим точку
        .Chunk(chunkSize)                   // Разбиваем на куски по 1М строк
        .Select(chunk =>                    
        {
            Array.Sort(chunk, comparer);    // Каждый кусок сортируем
            var tempFileName = Path.ChangeExtension(file, $".part-{count++}" + Path.GetExtension(file));
            File.WriteAllLines(tempFileName, chunk.Select(x => x.Line)); // Сохраняем отсортированные строки в файл
            return tempFileName;
        }).ToList();

Console.WriteLine($"SplitSort done in {sw.Elapsed}");
sw.Restart();

try
{
    // Читаем построчно все файлы, находим в строках точку
    var mergedLines = tempFiles
        .Select(f => File.ReadLines(f).Select(s => new Item(s, s.IndexOf('.')))) 
        .Merge(comparer)  //Слияние итераторов IEnumerable<IEnumerable<T>> в IEnumerable<T>
        .Select(x => x.Line); // Оставляем только строки
    File.WriteAllLines(Path.ChangeExtension(file, ".sorted" + Path.GetExtension(file)), mergedLines);
}
finally
{
    tempFiles.ForEach(f => File.Delete(f));
}
Console.WriteLine($"Merge done in {sw.Elapsed}");

return 0;

public record struct Item(string Line, int DotPosition);
public record Comparer(StringComparison stringComparison) : IComparer<Item>
{
    public int Compare(Item x, Item y)
    {
        var spanX = x.Line.AsSpan();
        var spanY = y.Line.AsSpan();
        var xDot = x.DotPosition;
        var yDot = y.DotPosition;

        var cmp = spanX[(xDot + 2)..].CompareTo(spanY[(yDot + 2)..], stringComparison);
        if (cmp != 0) return cmp;
        return int.Parse(spanX[..xDot]) - int.Parse(spanY[..yDot]);
    }
}