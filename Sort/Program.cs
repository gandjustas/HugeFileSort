using Sort;
using System.Text;

const int DefaultChunkSize = 100_000;
const int FileBufferSize = 4 * 1024 * 1024; //4 MB

if ((args?.Length ?? 0) == 0)
{
    Console.WriteLine($"Usage: {Path.GetFileNameWithoutExtension(Environment.ProcessPath)} <file path> [<chunk size>]");
    return -1;
}

var file = args![0];
var dir = Path.GetDirectoryName(file);
var fileName = Path.GetFileNameWithoutExtension(file);
var fileExt = Path.GetExtension(file);
var unique = new Random().Next().ToString("X8");

var chunkSize = args?.Length > 1 ? int.Parse(args[1]) : DefaultChunkSize;
var stringComparison = StringComparison.CurrentCulture;

FileStreamOptions fileReadOptions = new() 
{ 
    Options = FileOptions.SequentialScan,
    BufferSize = FileBufferSize
};

FileStreamOptions fileWriteOptions = new()
{
    Mode = FileMode.Create,
    Access = FileAccess.Write,
    BufferSize = FileBufferSize
};

Encoding encoding;
List<string> tempFiles;
using (var reader = new StreamReader(file, Encoding.UTF8, true, fileReadOptions))
{
    encoding = reader.CurrentEncoding;

    tempFiles = reader
        .EnumerateLines()
        .Select(l => (l, l.IndexOf('.')))
        .Chunk(chunkSize)
#if !DEBUG
        .AsParallel()
#endif
        .Select((chunk, n) => {            
            Array.Sort(chunk, Comparer);
            var tempFileName = Path.Combine(dir, $"{fileName}-{unique}-{n}{fileExt}");
            WriteAllLines(tempFileName, Encoding.UTF8, chunk);
            return tempFileName;
        }).ToList();
    
}

try
{
    var files = tempFiles
        .Select(f => new StreamReader(f, Encoding.UTF8, false, fileReadOptions))
        .ToList();
    WriteAllLines(file, encoding, 
        files.Select(
            f => f.EnumerateLines().Select(l => (l, l.IndexOf('.')))
        ).MergeLines(Comparer));
    files.ForEach(f => f.Dispose());
}
finally
{ 
    tempFiles.ForEach(f => File.Delete(f));
}

return 0;

void WriteAllLines(string file, Encoding encoding, IEnumerable<(string, int)> linesWithDotPosition)
{
    using (StreamWriter writer = new(file, encoding, fileWriteOptions))
    {
        writer.AutoFlush = false;
        foreach (var (line, _) in linesWithDotPosition)
        {
            writer.WriteLine(line);
        }
    }
}

int Comparer((string,int) x, (string, int) y)
{
    var cmp = x.Item1.AsSpan(x.Item2 + 2).CompareTo(y.Item1.AsSpan(y.Item2 + 2), stringComparison);
    if (cmp != 0) return cmp;
    return int.Parse(x.Item1.AsSpan(0, x.Item2))
            .CompareTo(
                int.Parse(y.Item1.AsSpan(0, y.Item2))
            );
}