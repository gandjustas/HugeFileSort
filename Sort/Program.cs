using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Runtime;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Channels;

internal class Program : IDisposable
{
    const int BufferSize = 1024 * 1024;
    private static readonly byte[] NewLine = Encoding.UTF8.GetBytes(Environment.NewLine);

    private readonly string file;
    private readonly int maxChunkSize;
    private readonly int degreeOfParallelism;
    private readonly CultureInfo culture;
    private readonly Encoding encoding = Encoding.UTF8;
    private readonly CompareOptions compareOptions;
    private readonly Comparer comparer;
    private readonly List<string> tempFiles = new();
    int maxLineSize = 0;
    int maxKeyLength = 0;
    long fileSize = 0;


    System.Diagnostics.Stopwatch timer = new();

    private Channel<(byte[], int)> readToParse;
    private Channel<(List<SortKey>, byte[], byte[])> parseToSort;
    private Channel<(List<SortKey>, byte[], byte[])> sortToWrite;
    private Task[] parserThreads;
    private Task[] sorterThreads;
    private Task writerThread;

    public Program(string file, int chunkSize, StringComparison stringComparison, int degreeOfParallelism)
    {
        this.file = file;
        this.maxChunkSize = chunkSize;
        this.degreeOfParallelism = degreeOfParallelism;
        this.comparer = new Comparer(stringComparison);
        this.culture = stringComparison switch
        {
            StringComparison.InvariantCulture => CultureInfo.InvariantCulture,
            StringComparison.InvariantCultureIgnoreCase => CultureInfo.InvariantCulture,
            _ => CultureInfo.CurrentCulture
        };
        this.compareOptions = stringComparison switch
        {
            StringComparison.Ordinal => CompareOptions.Ordinal,
            StringComparison.OrdinalIgnoreCase => CompareOptions.OrdinalIgnoreCase,
            StringComparison.CurrentCultureIgnoreCase => CompareOptions.IgnoreCase,
            StringComparison.InvariantCultureIgnoreCase => CompareOptions.IgnoreCase,
            _ => CompareOptions.None
        };
    }
    public void Dispose()
    {
        tempFiles.ForEach(File.Delete);
    }

    public async Task SplitSort()
    {
        timer.Restart();
        
        var pool = PrepareParallelWork();

        using var stream = new FileStream(file, FileMode.Open, FileAccess.Read, FileShare.Read, 0, FileOptions.SequentialScan);
        fileSize = stream.Length;

        List<SortKey>? chunk = null;
        byte[]? keyBuffer = null;
        char[]? charBuffer = null;

        var readBuffer = pool.Rent(maxChunkSize);
        var remainingBytes = 0;
        var eof = false;


        while (!eof)
        {
            var bytesRead = stream.ReadBlock(readBuffer, remainingBytes, maxChunkSize - remainingBytes, out eof);
            int chunkSize = remainingBytes + bytesRead;
            if (!eof)
            {
                var lastNewLine = readBuffer.AsSpan(0, bytesRead).LastIndexOf(NewLine);
                if (lastNewLine >= 0) chunkSize = lastNewLine + NewLine.Length;
                remainingBytes = remainingBytes + bytesRead - chunkSize;
            }

            var oldBuffer = readBuffer;
            if (degreeOfParallelism > 0)
            {
                await readToParse.Writer.WriteAsync((readBuffer, chunkSize));
                readBuffer = pool.Rent(maxChunkSize);
            }
            else
            {
                chunk ??= new();
                
                chunk.AddRange(ParseChunk(chunkSize, readBuffer, 
                    keyBuffer ??= pool.Rent(maxChunkSize), 
                    charBuffer ??= new char[1024]));

                //Сортируем и записываем чанки на диск
                chunk.Sort(comparer);
                WriteChunk(chunk);
                chunk.Clear();
            }

            //Осаток буфера переносим в начало
            if (remainingBytes > 0) oldBuffer.AsSpan(chunkSize, remainingBytes).CopyTo(readBuffer.AsSpan());
        }

        if(degreeOfParallelism == 0)
        {
            if (readBuffer != null) pool.Return(readBuffer);
            if (keyBuffer != null) pool.Return(keyBuffer);
        }

        await CompleteParralelWork();
        Console.WriteLine($"SplitSort done in {timer.Elapsed}");
    }

    private ArrayPool<byte> PrepareParallelWork()
    {
        var pool = ArrayPool<byte>.Create();

        if (degreeOfParallelism > 0)
        {

            readToParse = Channel.CreateBounded<(byte[], int)>(new BoundedChannelOptions(1)
            {
                SingleWriter = true,
                SingleReader = degreeOfParallelism == 1
            });
            parseToSort = Channel.CreateBounded<(List<SortKey>, byte[], byte[])>(new BoundedChannelOptions(1)
            {
                SingleWriter = degreeOfParallelism == 1,
                SingleReader = degreeOfParallelism == 1
            });
            sortToWrite = Channel.CreateBounded<(List<SortKey>, byte[], byte[])>(new BoundedChannelOptions(1)
            {
                SingleWriter = degreeOfParallelism == 1,
                SingleReader = true
            });

            parserThreads =
                Enumerable
                .Range(0, degreeOfParallelism)
                .Select(_ => Task.Run(async () => {
                    var charBuffer = new char[1024];
                    await foreach (var (readBuffer, chunkSize) in readToParse.Reader.ReadAllAsync())
                    {
                        var keyBuffer = pool.Rent(maxChunkSize);
                        var chunk = ParseChunk(chunkSize, readBuffer, keyBuffer, charBuffer).ToList();
                        await parseToSort.Writer.WriteAsync((chunk, readBuffer, keyBuffer));

                    }
                })).ToArray();

            sorterThreads =
                Enumerable
                .Range(0, degreeOfParallelism)
                .Select(_ => Task.Run(async () => {
                    await foreach (var item in parseToSort.Reader.ReadAllAsync())
                    {
                        item.Item1.Sort(comparer);
                        await sortToWrite.Writer.WriteAsync(item);

                    }
                })).ToArray();

            writerThread = Task.Run(async () => {
                await foreach (var (chunk, readBuffer, keyBuffer) in sortToWrite.Reader.ReadAllAsync())
                {
                    WriteChunk(chunk);
                    pool.Return(readBuffer);
                    pool.Return(keyBuffer);
                }
            });
        };

        return pool;
    }

    private async Task CompleteParralelWork()
    {
        if(degreeOfParallelism > 0)
        {
            readToParse.Writer.Complete();
            await Task.WhenAll(parserThreads);
            parseToSort.Writer.Complete();
            await Task.WhenAll(sorterThreads);
            sortToWrite.Writer.Complete();
            await writerThread;
            GCSettings.LargeObjectHeapCompactionMode = GCLargeObjectHeapCompactionMode.CompactOnce;
            GC.Collect(2);
        }
    }

    private IEnumerable<SortKey> ParseChunk(int byteCount, byte[] readBuffer, byte[] keyBuffer, char[] charBuffer)
    {
        var readPos = 0;
        var sortKeyPos = 0;
        while (byteCount > 0)
        {
            var linePos = readBuffer.AsSpan(readPos, byteCount).IndexOf(NewLine);
            if (linePos == -1) linePos = byteCount;
            if (charBuffer.Length < linePos) charBuffer = new char[linePos];

            var lineLen = encoding.GetChars(readBuffer, readPos, linePos, charBuffer, 0);
            var line = charBuffer.AsMemory(0, lineLen);
            var dot = line.Span.IndexOf('.');
            var x = int.Parse(line.Span[0..dot]);

            var sortKeyLen = culture.CompareInfo.GetSortKey(line.Span[(dot + 2)..], keyBuffer.AsSpan(sortKeyPos), compareOptions);
            BinaryPrimitives.WriteInt32BigEndian(keyBuffer.AsSpan(sortKeyPos + sortKeyLen, sizeof(int)), x);
            sortKeyLen += sizeof(int);

            var lineSize = linePos + NewLine.Length;
            yield return new SortKey(readBuffer.AsMemory(readPos, lineSize), keyBuffer.AsMemory(sortKeyPos, sortKeyLen));

            readPos += lineSize;
            byteCount -= lineSize;
            sortKeyPos += sortKeyLen;
            maxLineSize = Math.Max(maxLineSize, lineSize);
            maxKeyLength = Math.Max(maxKeyLength, sortKeyLen);
        }
    }

    void WriteChunk(List<SortKey> chunk)
    {
        // Записываем строки из отсортированного списка во временный файл
        var tempFileName = Path.ChangeExtension(file, $".part-{tempFiles.Count}.tmp");
        using var tempFile = new FileStream(tempFileName, FileMode.Create, FileAccess.Write, FileShare.None, BufferSize, FileOptions.SequentialScan);
        
        Span<byte> buffer = stackalloc byte[sizeof(int)];
        foreach (var (line, key) in chunk)
        {
            BinaryPrimitives.WriteInt32LittleEndian(buffer, line.Length);
            tempFile.Write(buffer);
            tempFile.Write(line.Span);

            BinaryPrimitives.WriteInt32LittleEndian(buffer, key.Length);
            tempFile.Write(buffer);
            tempFile.Write(key.Span);
        }
        tempFiles.Add(tempFileName);
    }

    public void Merge()
    {
        timer.Restart();

        var mergedLines = tempFiles
            .Select(ReadTempFile) // Читаем построчно все файлы, находим в строках точку
            .Merge(comparer);  //Слияние итераторов IEnumerable<IEnumerable<T>> в IEnumerable<T>

        string sortedFileName = Path.ChangeExtension(file, ".sorted" + Path.GetExtension(file));
        using var sortedFile = new FileStream(sortedFileName, FileMode.Create, FileAccess.Write, FileShare.None, BufferSize, FileOptions.SequentialScan);
        sortedFile.SetLength(fileSize);
        foreach (var (l, _) in mergedLines)
        {
            sortedFile.Write(l.Span);
        }
        Console.WriteLine($"Merge done in {timer.Elapsed}");
    }

    private IEnumerable<SortKey> ReadTempFile(string file)
    {
        using var stream = new FileStream(file, FileMode.Open, FileAccess.Read, FileShare.Read, BufferSize, FileOptions.SequentialScan);

        var readBuffer = new byte[Math.Max(BufferSize, maxLineSize + maxLineSize + sizeof(int) * 2)];

        var bytesRemaining = 0;
        var eof = false;

        while (!eof)
        {
            var bytesRead = stream.ReadBlock(readBuffer, bytesRemaining, readBuffer.Length - bytesRemaining, out eof);
            if (bytesRead == 0) eof = true;
            bytesRemaining += bytesRead;

            var parsePosition = 0;
            while (bytesRemaining - parsePosition > maxLineSize * 2 || (eof && parsePosition < bytesRemaining))
            {

                var lineSize = BinaryPrimitives.ReadInt32LittleEndian(readBuffer.AsSpan(parsePosition, sizeof(int)));
                parsePosition += sizeof(int);

                var lineOffset = parsePosition;
                parsePosition += lineSize;

                var keyLen = BinaryPrimitives.ReadInt32LittleEndian(readBuffer.AsSpan(parsePosition, sizeof(int)));
                parsePosition += sizeof(int);

                yield return new SortKey(readBuffer.AsMemory(lineOffset, lineSize), readBuffer.AsMemory(parsePosition, keyLen));
                parsePosition += keyLen;
            }

            readBuffer.AsSpan(parsePosition).CopyTo(readBuffer);
            bytesRemaining -= parsePosition;
        }

    }

    private static async Task<int> Main(string[] args)
    {
        if ((args?.Length ?? 0) == 0)
        {
            Console.WriteLine($"Usage: {Path.GetFileNameWithoutExtension(Environment.ProcessPath)} <file path> [<approximate chunk size> in MB]");
            return -1;
        }

        var file = args![0];
        var chunkSize = args?.Length > 1 ? int.Parse(args[1]) : 100; //В мегабайтах

        using var app = new Program(file, chunkSize * 1024 * 1024 / 2, StringComparison.CurrentCulture, Environment.ProcessorCount / 8);
        await app.SplitSort();
        app.Merge();

        return 0;
    }

}

