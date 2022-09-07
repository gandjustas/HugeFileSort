using Sort;
using System.Diagnostics;
using System.Text;

const int DefaultChunkSize = 100_000;

if ((args?.Length ?? 0) == 0)
{
    Console.WriteLine($"Usage: {Path.GetFileNameWithoutExtension(Environment.ProcessPath)} <file path> [<chunk size>]");
    return -1;
}

var file = args![0];
var chunkSize = args?.Length > 1 ? int.Parse(args[1]) : DefaultChunkSize;

var comparer = new Comparer(StringComparison.CurrentCulture);

Encoding encoding;
List<string> tempFiles;
using (var reader = new StreamReader(file, true))
{
    encoding = reader.CurrentEncoding;

    tempFiles = reader
        .EnumerateLines()
        .Chunk(chunkSize)
#if !DEBUG
        .AsParallel()
#endif
        .Select(chunk => {
            Array.Sort(chunk, comparer);
            var tempFileName = Path.GetTempFileName();
            File.WriteAllLines(tempFileName, chunk);
            return tempFileName;
        }).ToList();

}

try
{
        var files = tempFiles
            .Select(f => File.OpenText(f))
            .ToList();
        File.WriteAllLines(file, files.Select(f => f.EnumerateLines()).MergeLines(comparer), encoding);
        files.ForEach(f => f.Dispose());
}
finally
{ 
    tempFiles.ForEach(f => File.Delete(f));
}

return 0;


