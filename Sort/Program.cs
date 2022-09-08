using Sort;
using System.Text;

const int DefaultChunkSize = 100_000;

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

var comparer = new Comparer(StringComparison.CurrentCulture);
FileStreamOptions fileOptions = new() { 
    Options = FileOptions.SequentialScan,
    BufferSize = 4 * 1024 * 1024
};

Encoding encoding;
List<string> tempFiles;
using (var reader = new StreamReader(file, Encoding.UTF8, true, fileOptions))
{
    encoding = reader.CurrentEncoding;

    tempFiles = reader
        .EnumerateLines()
        .Chunk(chunkSize)
#if !DEBUG
        .AsParallel()
#endif
        .Select((chunk, n) => {            
            Array.Sort(chunk, comparer);
            var tempFileName = Path.Combine(dir, $"{fileName}-{unique}-{n}{fileExt}");
            File.WriteAllLines(tempFileName, chunk);
            return tempFileName;
        }).ToList();
    
}

try
{
        var files = tempFiles
            .Select(f => new StreamReader(f, Encoding.UTF8, false, fileOptions))
            .ToList();
        File.WriteAllLines(file, files.Select(f => f.EnumerateLines()).MergeLines(comparer), encoding);
        files.ForEach(f => f.Dispose());
}
finally
{ 
    tempFiles.ForEach(f => File.Delete(f));
}

return 0;


