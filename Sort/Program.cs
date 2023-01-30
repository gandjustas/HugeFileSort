using System.Buffers;
using System.Buffers.Binary;
using System.Diagnostics;
using System.Globalization;
using System.Runtime;
using System.Text;
using System.Threading.Channels;

internal class Program : IDisposable
{
    const int BufferSize = 1024 * 1024;
    const int BufferMargin = 4096;
    private static readonly byte[] NewLine = Encoding.UTF8.GetBytes(Environment.NewLine);
    private static readonly SortKeySelector selector = new SortKeySelector();

    private readonly string file;
    private readonly int chunkSize;
    private readonly int degreeOfParallelism;
    private readonly CultureInfo culture;
    private readonly Encoding encoding = Encoding.UTF8;
    private readonly CompareOptions compareOptions;
    private readonly Comparer comparer;
    private readonly List<string> tempFiles = new();
    int maxLineSize = 0;
    int maxLineLength = 0;
    int chunkCapacity;

    readonly Channel<(byte[], int, int)> readToParse;
    readonly Channel<(byte[], char[], List<SortKey>)> parseToSort;
    readonly Channel<(byte[], char[], List<SortKey>)> sortToWrite;

    System.Diagnostics.Stopwatch timer = new();


    public Program(string file, int chunkSize, StringComparison stringComparison, int degreeOfParallelism)
    {
        this.file = file;
        this.chunkSize = chunkSize;
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

        this.readToParse = Channel.CreateBounded<(byte[], int, int)>(new BoundedChannelOptions(1)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleWriter = true,
            SingleReader = degreeOfParallelism == 1,
        });

        this.parseToSort = Channel.CreateBounded<(byte[], char[], List<SortKey>)>(new BoundedChannelOptions(1)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleWriter = degreeOfParallelism == 1,
            SingleReader = degreeOfParallelism == 1,
        });

        this.sortToWrite = Channel.CreateBounded<(byte[], char[], List<SortKey>)>(new BoundedChannelOptions(1)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleWriter = degreeOfParallelism == 1,
            SingleReader = true
        });
        chunkCapacity = chunkSize / 1000;
    }
    public void Dispose()
    {
        tempFiles.ForEach(File.Delete);
    }

    public async Task SplitSort()
    {
        timer.Restart();

        Task[]? parsers = null;
        Task[]? sorters = null;
        Task? writer = null;
        if (degreeOfParallelism > 0)
        {
            parsers =
                Enumerable
                .Range(0, degreeOfParallelism)
                .Select(_ => Task.Run(Parser))
                .ToArray();
            sorters =
                Enumerable
                .Range(0, degreeOfParallelism)
                .Select(_ => Task.Run(Sorter))
                .ToArray();
            writer = Task.Run(Writer);
        }

        using var stream = new FileStream(file, FileMode.Open);

        var readBuffer = ArrayPool<byte>.Shared.Rent(BufferMargin + chunkSize); // запас для sortkey
        var readPosition = 0;
        List<SortKey> chunk = new();

        char[]? charBuffer = null;
        var eof = false;
        while (!eof)
        {
            while (!eof && readPosition < chunkSize)
            {
                var bytesRead = await stream.ReadAsync(readBuffer, BufferMargin + readPosition, chunkSize - readPosition);
                if (bytesRead == 0) eof = true;
                readPosition += bytesRead;
            }

            var oldReadBuffer = readBuffer;
            var lastIndexOfNewLine = readBuffer.AsSpan(BufferMargin, readPosition).LastIndexOf(NewLine);
            var remainder = (eof || lastIndexOfNewLine == -1) ? readPosition : (lastIndexOfNewLine + NewLine.Length);
            readPosition -= remainder;
            if (degreeOfParallelism > 0)
            {
                await readToParse.Writer.WriteAsync((readBuffer, BufferMargin, remainder));
                readBuffer = ArrayPool<byte>.Shared.Rent(BufferMargin + chunkSize);
            }
            else
            {
                charBuffer ??= ArrayPool<char>.Shared.Rent(chunkSize);
                chunk.AddRange(ParseLines(readBuffer, BufferMargin, remainder, charBuffer, 0));
                chunkCapacity = (chunkCapacity = chunk.Count) / 2;

                //Сортируем и записываем чанки на диск
                chunk.Sort(comparer);

                //SortHelpers.RadixQuickSort(CollectionsMarshal.AsSpan(chunk), selector);

                await WriteChunk(chunk);

                chunk.Clear();
            }

            //Осаток буфера переносим в начало
            if (readPosition > 0) oldReadBuffer.AsSpan(BufferMargin + remainder, readPosition).CopyTo(readBuffer.AsSpan(BufferMargin));
        }

        if (degreeOfParallelism == 0)
        {
            ArrayPool<byte>.Shared.Return(readBuffer);
            if (charBuffer != null) ArrayPool<char>.Shared.Return(charBuffer);
        }
        else
        {
            readToParse.Writer.Complete();
            await Task.WhenAll(parsers!);
            parseToSort.Writer.Complete();
            await Task.WhenAll(sorters!);
            sortToWrite.Writer.Complete();
            await writer!;

        }

        Console.WriteLine($"SplitSort done in {timer.Elapsed}");
    }
    private async Task Parser()
    {
        await foreach (var (buffer, parsePosition, remainingBytes) in readToParse.Reader.ReadAllAsync())
        {
            var charBuffer = ArrayPool<char>.Shared.Rent(chunkSize);
            List<SortKey> chunk = new List<SortKey>(chunkCapacity);
            chunk.AddRange(ParseLines(buffer, parsePosition, remainingBytes, charBuffer, 0));
            chunkCapacity = (chunkCapacity = chunk.Count) / 2;
            await parseToSort.Writer.WriteAsync((buffer, charBuffer, chunk));
        }

    }
    private async Task Sorter()
    {
        await foreach (var item in parseToSort.Reader.ReadAllAsync())
        {
            item.Item3.Sort(comparer);
            await sortToWrite.Writer.WriteAsync(item);
        }
    }

    private async Task Writer()
    {
        await foreach (var (readBuffer, charBuffer, chunk) in sortToWrite.Reader.ReadAllAsync())
        {
            await WriteChunk(chunk);
            ArrayPool<byte>.Shared.Return(readBuffer);
            ArrayPool<char>.Shared.Return(charBuffer);
            GCSettings.LargeObjectHeapCompactionMode = GCLargeObjectHeapCompactionMode.CompactOnce;
            GC.Collect(2);
        }
    }

    private IEnumerable<SortKey> ParseLines(byte[] readBuffer, int parsePosition, int remainingBytes, char[] charBuffer, int charPosition)
    {
        var sortKeyPos = 0;
        while (remainingBytes > 0)
        {
            var linePos = readBuffer.AsSpan(parsePosition, remainingBytes).IndexOf(NewLine);
            if (linePos == -1) linePos = remainingBytes;

            var lineLen = encoding.GetChars(readBuffer, parsePosition, linePos, charBuffer, charPosition);
            var line = charBuffer.AsMemory(charPosition, lineLen);
            var dot = line.Span.IndexOf('.');
            var x = int.Parse(line.Span[0..dot]);

            var sortKeyLen = culture.CompareInfo.GetSortKey(line.Span[(dot + 2)..], readBuffer.AsSpan(sortKeyPos, parsePosition - sortKeyPos + linePos), compareOptions);
            BinaryPrimitives.WriteInt32BigEndian(readBuffer.AsSpan(sortKeyPos + sortKeyLen, sizeof(int)), x);
            sortKeyLen += sizeof(int);

            charPosition += lineLen;
            parsePosition += linePos + NewLine.Length;
            remainingBytes -= linePos + NewLine.Length;

            yield return new SortKey(line, readBuffer.AsMemory(sortKeyPos, sortKeyLen));
            sortKeyPos += sortKeyLen;
            maxLineSize = Math.Max(maxLineSize, linePos);
            maxLineLength = Math.Max(maxLineLength, lineLen);
        }        
    }

    char[]? line;
    byte[]? writeBuffer;
    async Task WriteChunk(List<SortKey> chunk)
    {
        line ??= new char[maxLineLength];
        writeBuffer ??= new byte[maxLineSize * 2];

        // Записываем строки из отсортированного списка во временный файл
        var tempFileName = Path.ChangeExtension(file, $".part-{tempFiles.Count}.tmp");
        using var tempFile = new FileStream(tempFileName, FileMode.Create, FileAccess.Write, FileShare.None, BufferSize);

        foreach (var (line, key) in chunk)
        {
            line.CopyTo(this.line);
            var bytesConverted = encoding.GetBytes(this.line, 0, line.Length, writeBuffer, sizeof(int));
            BinaryPrimitives.WriteInt32LittleEndian(writeBuffer, bytesConverted);
            await tempFile.WriteAsync(writeBuffer, 0, bytesConverted + sizeof(int));
            BinaryPrimitives.WriteInt32LittleEndian(writeBuffer, key.Length);
            key.CopyTo(writeBuffer.AsMemory(sizeof(int)));
            await tempFile.WriteAsync(writeBuffer, 0, key.Length + sizeof(int));
        }
        tempFiles.Add(tempFileName);
    }

    public void Merge()
    {
        timer.Restart();

        var mergedLines = tempFiles
            .Select(ReadTempFile) // Читаем построчно все файлы, находим в строках точку
            .Merge(comparer);  //Слияние итераторов IEnumerable<IEnumerable<T>> в IEnumerable<T>

        using var sortedFile = new StreamWriter(Path.ChangeExtension(file, ".sorted" + Path.GetExtension(file)), false, encoding, BufferSize);
        sortedFile.AutoFlush = false;
        foreach (var (l, _) in mergedLines)
        {
            sortedFile.WriteLine(l);
        }
        Console.WriteLine($"Merge done in {timer.Elapsed}");
    }

    private IEnumerable<SortKey> ReadTempFile(string file)
    {
        using var stream = new FileStream(file, FileMode.Open);

        var readBuffer = new byte[BufferSize];
        var lineBuffer = new char[maxLineLength];

        var bytesRemaining = 0;
        var eof = false;

        while (!eof)
        {
            var parsePosition = 0;
            var charsRead = stream.Read(readBuffer, bytesRemaining, readBuffer.Length - bytesRemaining);
            if (charsRead == 0) eof = true;
            bytesRemaining += charsRead;

            while (bytesRemaining - parsePosition > maxLineSize * 2 || (eof && parsePosition < bytesRemaining))
            {

                var lineSize = BinaryPrimitives.ReadInt32LittleEndian(readBuffer.AsSpan(parsePosition, sizeof(int)));
                parsePosition += sizeof(int);

                var charCount = encoding.GetChars(readBuffer, parsePosition, lineSize, lineBuffer, 0);
                parsePosition += lineSize;

                var keyLen = BinaryPrimitives.ReadInt32LittleEndian(readBuffer.AsSpan(parsePosition, sizeof(int)));
                parsePosition += sizeof(int);

                yield return new SortKey(lineBuffer[..charCount], readBuffer.AsMemory(parsePosition, keyLen));
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
        var chunkSize = args?.Length > 1 ? int.Parse(args[1]) : 200; //В мегабайтах


        using var app = new Program(file, chunkSize * 1024 * 1024 / 2, StringComparison.CurrentCulture, Environment.ProcessorCount / 8);
        await app.SplitSort();
        app.Merge();

        return 0;
    }

}

