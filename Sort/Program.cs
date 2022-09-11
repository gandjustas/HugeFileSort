using Sort;
using System.Buffers;
using System.Text;

//long DefaultChunkSize = GC.GetGCMemoryInfo().TotalAvailableMemoryBytes / (5 * 4 * 1024); //20% of memory, 4kb string ;
long DefaultChunkSize = 100_000;
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

var stringComparison = StringComparison.Ordinal;

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
        .Chunk((int)chunkSize)
        .Select((chunk, n) =>
        {
            //if (stringComparison == StringComparison.Ordinal) RadixSort(chunk);
            //else
            Array.Sort(chunk, Comparer); 
            var tempFileName = Path.Combine(dir, $"{fileName}-{unique}-{n}{fileExt}");
            WriteAllLines(tempFileName, Encoding.UTF8, chunk);
            return tempFileName;
        }).ToList();

}

fileReadOptions.BufferSize = 100 * 1024 * 1024 / tempFiles.Count;

try
{
    var files = tempFiles
        .Select(f => new StreamReader(f, Encoding.UTF8, false, fileReadOptions))
        .ToList();
    var lines = files
        .Select(
            f => f.EnumerateLines().Select(l => (l, l.IndexOf('.')))
        ).MergeLines(Comparer);
    WriteAllLines(file, encoding, lines);
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