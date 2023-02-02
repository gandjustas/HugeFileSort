if ((args?.Length ?? 0) == 0)
{
    Console.WriteLine($"Usage: {Path.GetFileNameWithoutExtension(Environment.ProcessPath)} <file path>");
    return -1;
}

var file = args![0];
var chunkSize = 1_000_000;

var comparer = new Comparer(StringComparison.CurrentCulture);

var sw = System.Diagnostics.Stopwatch.StartNew();

var count = 0;
var tempFiles =
    File.ReadLines(file)
        .Select(s => new Item(s, s.IndexOf('.')))
        .Chunk(chunkSize)
        .Select(chunk =>
        {
            Array.Sort(chunk, comparer);
            var tempFileName = Path.ChangeExtension(file, $".part-{count++}" + Path.GetExtension(file));
            File.WriteAllLines(tempFileName, chunk.Select(x => x.Line));
            return tempFileName;
        }).ToList();

Console.WriteLine($"SplitSort done in {sw.Elapsed}");
sw.Restart();

try
{
    var mergedLines = tempFiles
        .Select(f => File.ReadLines(f).Select(s => new Item(s, s.IndexOf('.'))))
        .Merge(comparer) // IEnumerable<IEnumerable<T>> -> IEnumerable<T>
        .Select(x => x.Line);
    File.WriteAllLines(Path.ChangeExtension(file, ".sorted" + Path.GetExtension(file)), mergedLines);
}
finally
{
    tempFiles.ForEach(File.Delete);
}
Console.WriteLine($"Merge done in {sw.Elapsed}");

return 0;
public record struct Item(string Line, int DotPosition);
public record Comparer(StringComparison StringComparison) : IComparer<Item>
{
    public int Compare(Item x, Item y)
    {
        var spanX = x.Line.AsSpan();
        var spanY = y.Line.AsSpan();
        var xDot = x.DotPosition;
        var yDot = y.DotPosition;

        var cmp = spanX[(xDot + 2)..].CompareTo(spanY[(yDot + 2)..], StringComparison);
        if (cmp != 0) return cmp;
        return int.Parse(spanX[..xDot]) - int.Parse(spanY[..yDot]);
    }
}