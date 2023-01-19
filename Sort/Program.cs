using Sort;
using System.Buffers;
using System.IO.Pipelines;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Channels;

//long DefaultChunkSize = GC.GetGCMemoryInfo().TotalAvailableMemoryBytes / (5 * 4 * 1024); //20% of memory, 4kb string ;
int DefaultChunkSize = 100_000;
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
var degreeOfParralelism = args?.Length > 2 ? int.Parse(args[2]) : 2;

var stringComparison = StringComparison.Ordinal;

Encoding encoding = Encoding.UTF8;

var stream = new FileStream(file, new FileStreamOptions { Mode = FileMode.Open, Access = FileAccess.Read, BufferSize = 0, Options = FileOptions.SequentialScan | FileOptions.Asynchronous });
var size = stream.Length;

var readChannel = Channel.CreateBounded<((string, int)[], bool)>(new BoundedChannelOptions(degreeOfParralelism)
{
    FullMode = BoundedChannelFullMode.Wait,
    SingleReader = degreeOfParralelism == 1,
    SingleWriter = true
});


var pipeReader = new PipeChunkReader(stream, encoding, FileBufferSize, readChannel.Writer, chunkSize);

var writeChnnel = Channel.CreateBounded<((string, int)[], bool)> (new BoundedChannelOptions(1)
{
    FullMode = BoundedChannelFullMode.Wait,
    SingleReader = true,
    SingleWriter = degreeOfParralelism == 1
});

async Task Sorter()
{
    var reader = readChannel.Reader;
    var writer = writeChnnel.Writer;
    await foreach (var p in reader.ReadAllAsync())
    {
        Array.Sort(p.Item1, Comparer);
        await writer.WriteAsync(p);
    }
}

var tasks = Task.WhenAll(
    Task.Run(pipeReader.Process),
    Task.WhenAll(Enumerable.Range(0,degreeOfParralelism).Select(_ => Task.Run(Sorter))).ContinueWith(_ => writeChnnel.Writer.Complete())
);

List<string> tempFiles = new((int)(stream.Length / chunkSize / 1024));
try
{
    var reader = writeChnnel.Reader;
    var counter = 0;
    await foreach (var (chunk,r) in reader.ReadAllAsync())
    {
        var tempFileName = Path.Combine(dir!, $"{fileName}-{unique}-{counter}{fileExt}");
        //using var output = new FileStream(tempFileName, new FileStreamOptions { Mode = FileMode.CreateNew, Access = FileAccess.Write, BufferSize = 0, Options = FileOptions.SequentialScan | FileOptions.Asynchronous });
        //await WriteAllLinesAsync(output, encoding, chunk);
        using var output = new StreamWriter(tempFileName, new FileStreamOptions { Mode = FileMode.CreateNew, Access = FileAccess.Write, BufferSize = 0, Options = FileOptions.SequentialScan });
        WriteAllLines(output, chunk);
        tempFiles.Add(tempFileName);
        counter++;
        if (r) ArrayPool<(string, int)>.Shared.Return(chunk);
    };

    await tasks;

    var files = tempFiles
        .Select(f => new StreamReader(f, Encoding.UTF8, false, new FileStreamOptions { Options = FileOptions.SequentialScan, BufferSize = 100 * 1024 * 1024 / tempFiles.Count }))
        .ToList();
    try
    {
        var lines = files
            .Select(
                f => f.EnumerateLines().Select(l => (l, l.IndexOf('.')))
            ).MergeLines(Comparer);
        using var output = new StreamWriter(file, encoding, new FileStreamOptions
        {
            Options = FileOptions.SequentialScan,
            BufferSize = FileBufferSize,
            Mode = FileMode.Create,            
            Access = FileAccess.Write,
            PreallocationSize = size
        });
        WriteAllLines(output, lines);
    }
    finally
    {
        files.ForEach(f => f.Dispose());
    }
}
finally
{
    tempFiles.ForEach(f => File.Delete(f));
}

return 0;

void WriteAllLines(StreamWriter writer, IEnumerable<(string, int)> linesWithDotPosition)
{
    writer.AutoFlush = false;
    foreach (var (line, _) in linesWithDotPosition)
    {
        writer.WriteLine(line);
    }
}

async Task WriteAllLinesAsync(FileStream stream, Encoding encoding, IEnumerable<(string, int)> linesWithDotPosition)
{
    var writer = PipeWriter.Create(stream, new StreamPipeWriterOptions(minimumBufferSize: FileBufferSize));
    var newLine = encoding.GetBytes(Environment.NewLine);
    foreach (var (line, _) in linesWithDotPosition)
    {
        encoding.GetBytes(line, writer);
        writer.Write(newLine);
        if (writer.UnflushedBytes > FileBufferSize)
        {
            await writer.FlushAsync();
        }
    }
    await writer.FlushAsync();
    await writer.CompleteAsync();
}
int Comparer((string, int) x, (string, int) y)
{
    var cmp = x.Item1.AsSpan(x.Item2 + 2).CompareTo(y.Item1.AsSpan(y.Item2 + 2), stringComparison);
    if (cmp != 0) return cmp;
    return ulong.Parse(x.Item1.AsSpan(0, x.Item2))
            .CompareTo(
                ulong.Parse(y.Item1.AsSpan(0, y.Item2))
            );
}

void RadixSort((string, int)[] linesWithDotPos)
{
    var maxChar = '\0';
    foreach (var (line, dot) in linesWithDotPos)
    {
        foreach (var c in line.AsSpan(dot + 2))
        {
            maxChar = (c > maxChar) ? c : maxChar;
        }
    }
    RadixSortIntl(linesWithDotPos.AsSpan(), maxChar, 0);
}

void RadixSortIntl(Span<(string, int)> linesWithDotPos, char maxChar, int charPos)
{
    Span<int> count = stackalloc int[(int)maxChar + 1];
    count.Clear();

    // Заполняем счетчик символов
    foreach (var (line, dot) in linesWithDotPos)
    {
        var idx = dot + 2 + charPos;
        if (idx < line.Length)
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
    for (int i = 0, j = 0; i < count.Length; i++)
    {
        if (count[i] > 0) bins[j++] = count[i];
    }

    // Сортировка по числу в начале строки если все строки попали в корзину \0
    if (binCount == 1)
    {
        if (!sortStrings)
        {
            linesWithDotPos.Sort((x, y) =>
                ulong.Parse(x.Item1.AsSpan(0, x.Item2))
                    .CompareTo(
                        ulong.Parse(y.Item1.AsSpan(0, y.Item2))
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
    for (int i = linesWithDotPos.Length - 1; i >= 0; i--)
    {
        var item = linesWithDotPos[i];
        var (line, dot) = item;
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
            RadixSortIntl(linesWithDotPos[..bin], maxChar, charPos + 1);
        }
        linesWithDotPos = linesWithDotPos[bin..];
    }

}