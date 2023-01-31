using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;


internal class Program : IDisposable
{
    const int BufferSize = 1024 * 1024;
    const int BufferMargin = 4096;
    private static readonly byte[] NewLine = Encoding.UTF8.GetBytes(Environment.NewLine);
    private static readonly SortKeySelector selector = new SortKeySelector();
    
    private readonly string file;
    private readonly int maxChunkSize;
    private readonly CultureInfo culture;
    private readonly Encoding encoding = Encoding.UTF8;
    private readonly CompareOptions compareOptions;
    private readonly Comparer comparer;
    private readonly List<string> tempFiles = new();
    int maxKeySize = 0;
    int maxLineSize = 0;


    System.Diagnostics.Stopwatch timer = new();


    public Program(string file, int chunkSize, StringComparison stringComparison)
    {
        this.file = file;
        this.maxChunkSize = chunkSize;
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

    public void SplitSort()
    {
        timer.Restart();

        //using var reader = new StreamReader(file, encoding);
        using var stream = new FileStream(file, FileMode.Open, FileAccess.Read, FileShare.Read, 0, FileOptions.RandomAccess);
        long offset = 0;

        List<SortKey> chunk = new();

        var readBuffer = new byte[BufferMargin + maxChunkSize]; // BufferMargin запас для sortkey
        var remainingBytes = 0;

        var charBuffer = new char[maxChunkSize];
        var eof = false;
        while (!eof)
        {
            var bytesRead = stream.ReadBlock(readBuffer, BufferMargin + remainingBytes, maxChunkSize - remainingBytes, out eof);
            int chunkSize = remainingBytes + bytesRead;
            if (!eof)
            {
                var lastNewLine = readBuffer.AsSpan(BufferMargin, bytesRead).LastIndexOf(NewLine);
                if (lastNewLine >= 0) chunkSize = lastNewLine + NewLine.Length;
                remainingBytes = remainingBytes + bytesRead - chunkSize;
            }

            chunk.AddRange(ParseChunk(offset - BufferMargin, readBuffer, BufferMargin, chunkSize));

            //Сортируем и записываем чанки на диск
            chunk.Sort(comparer);

            //SortHelpers.RadixQuickSort(CollectionsMarshal.AsSpan(chunk), selector);

            WriteChunk(chunk);

            chunk.Clear();

            //Осаток буфера переносим в начало
            if (remainingBytes > 0) readBuffer.AsSpan(BufferMargin + chunkSize, remainingBytes).CopyTo(readBuffer.AsSpan(BufferMargin));
            offset += chunkSize;
        }

        Console.WriteLine($"SplitSort done in {timer.Elapsed}");
    }

    private IEnumerable<SortKey> ParseChunk(long offset, byte[] readBuffer, int startIndex, int count)
    {
        var charBuffer = new char[1024];
        var sortKeyPos = 0;
        while (count > 0)
        {
            var linePos = readBuffer.AsSpan(startIndex, count).IndexOf(NewLine);
            if (linePos == -1) linePos = count;
            if (linePos > charBuffer.Length) 
            {
                Array.Resize(ref charBuffer, linePos);
            }

            var lineLen = encoding.GetChars(readBuffer, startIndex, linePos, charBuffer, 0);
            var line = charBuffer.AsMemory(0, lineLen);
            var dot = line.Span.IndexOf('.');
            var x = int.Parse(line.Span[0..dot]);

            var sortKeyLen = culture.CompareInfo.GetSortKey(line.Span[(dot + 2)..], readBuffer.AsSpan(sortKeyPos, startIndex - sortKeyPos + linePos), compareOptions);
            BinaryPrimitives.WriteInt32BigEndian(readBuffer.AsSpan(sortKeyPos + sortKeyLen, sizeof(int)), x);
            sortKeyLen += sizeof(int);

            yield return new SortKey(readBuffer.AsMemory(sortKeyPos, sortKeyLen), offset + startIndex, linePos + NewLine.Length);

            startIndex += linePos + NewLine.Length;
            count -= linePos + NewLine.Length;
            sortKeyPos += sortKeyLen;
            maxLineSize = Math.Max(maxLineSize, linePos + NewLine.Length);
            maxKeySize = Math.Max(maxKeySize, sortKeyLen);
        }
    }

    byte[]? writeBuffer;
    void WriteChunk(List<SortKey> chunk)
    {
        writeBuffer ??= new byte[maxKeySize + sizeof(long) + sizeof(int) * 2];

        // Записываем строки из отсортированного списка во временный файл
        var tempFileName = Path.ChangeExtension(file, $".part-{tempFiles.Count}.tmp");
        using var tempFile = new FileStream(tempFileName, FileMode.Create, FileAccess.Write, FileShare.None, BufferSize, FileOptions.SequentialScan);

        foreach (var (key, offset, length) in chunk)
        {
            var writeSize = 0;
            BinaryPrimitives.WriteInt64LittleEndian(writeBuffer.AsSpan(writeSize), offset);
            writeSize += sizeof(long);

            BinaryPrimitives.WriteInt32LittleEndian(writeBuffer.AsSpan(writeSize), length);
            writeSize += sizeof(int);

            BinaryPrimitives.WriteInt32LittleEndian(writeBuffer.AsSpan(writeSize), key.Length);
            writeSize += sizeof(int);

            key.CopyTo(writeBuffer.AsMemory(writeSize));
            writeSize += key.Length;

            tempFile.Write(writeBuffer, 0, writeSize);            
        }
        tempFiles.Add(tempFileName);
    }

    public void Merge()
    {
        timer.Restart();

        var merged = tempFiles
            .Select(ReadTempFile) // Читаем построчно все файлы, находим в строках точку
            .Merge(comparer);  //Слияние итераторов IEnumerable<IEnumerable<T>> в IEnumerable<T>

        using var sourceFile = new FileStream(file, FileMode.Open, FileAccess.Read, FileShare.Read, 0, FileOptions.RandomAccess);
        var sortedFileName = Path.ChangeExtension(file, ".sorted" + Path.GetExtension(file));
        using var sortedFile = new FileStream(sortedFileName, FileMode.Create, FileAccess.Write, FileShare.None, BufferSize);
        var buffer = new byte[maxLineSize];
        foreach (var (_, offset, length) in merged)
        {
            sourceFile.Seek(offset, SeekOrigin.Begin);
            var bytesRead = sourceFile.ReadBlock(buffer, 0, length, out var _);
            sortedFile.Write(buffer, 0, bytesRead);
        }
        Console.WriteLine($"Merge done in {timer.Elapsed}");
    }

    private IEnumerable<SortKey> ReadTempFile(string file)
    {
        using var stream = new FileStream(file, FileMode.Open);

        var readBuffer = new byte[BufferSize];

        var bytesRemaining = 0;
        var eof = false;

        while (!eof)
        {
            var bytesRead = stream.ReadBlock(readBuffer, bytesRemaining, readBuffer.Length - bytesRemaining, out eof);

            bytesRemaining += bytesRead;

            var parsePosition = 0;
            while (bytesRemaining - parsePosition > maxLineSize * 2 || (eof && parsePosition < bytesRemaining))
            {

                var offset = BinaryPrimitives.ReadInt64LittleEndian(readBuffer.AsSpan(parsePosition, sizeof(long)));
                parsePosition += sizeof(long);

                var length = BinaryPrimitives.ReadInt32LittleEndian(readBuffer.AsSpan(parsePosition, sizeof(int)));
                parsePosition += sizeof(int);

                var keyLen = BinaryPrimitives.ReadInt32LittleEndian(readBuffer.AsSpan(parsePosition, sizeof(int)));
                parsePosition += sizeof(int);

                yield return new SortKey(readBuffer.AsMemory(parsePosition, keyLen), offset, length);
                parsePosition += keyLen;
            }

            readBuffer.AsSpan(parsePosition).CopyTo(readBuffer);
            bytesRemaining -= parsePosition;
        }

    }

    private static int Main(string[] args)
    {
        if ((args?.Length ?? 0) == 0)
        {
            Console.WriteLine($"Usage: {Path.GetFileNameWithoutExtension(Environment.ProcessPath)} <file path> [<approximate chunk size> in MB]");
            return -1;
        }

        var file = args![0];
        var chunkSize = args?.Length > 1 ? int.Parse(args[1])  : 200; //В мегабайтах


        using var app = new Program(file, chunkSize * 1024 * 1024, StringComparison.CurrentCulture);
        app.SplitSort();
        app.Merge();

        return 0;
    }

}

