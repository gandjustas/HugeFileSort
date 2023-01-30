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
    private readonly int chunkSize;
    private readonly CultureInfo culture;
    private readonly Encoding encoding = Encoding.UTF8;
    private readonly CompareOptions compareOptions;
    private readonly Comparer comparer;
    private readonly List<string> tempFiles = new();
    int maxLineSize = 0;
    int maxLineLength = 0;


    System.Diagnostics.Stopwatch timer = new();


    public Program(string file, int chunkSize, StringComparison stringComparison)
    {
        this.file = file;
        this.chunkSize = chunkSize;
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
        using var stream = new FileStream(file, FileMode.Open);

        List<SortKey> chunk = new();

        var readBuffer = new byte[BufferMargin + chunkSize]; // запас для sortkey
        var remainingBytes = 0;

        var charBuffer = new char[chunkSize];
        var eof = false;
        while (!eof)
        {
            var charPosition = 0;
            var parsePosition = BufferMargin;
            var sortKeyPos = 0;

            while (!eof && remainingBytes < chunkSize)
            {
                var charsRead = stream.Read(readBuffer, BufferMargin + remainingBytes, chunkSize - remainingBytes);
                if (charsRead == 0) eof = true;
                remainingBytes += charsRead;
            }

            int linePos;
            while ((linePos = readBuffer.AsSpan(parsePosition, remainingBytes).IndexOf(NewLine)) >= 0
                || (eof && remainingBytes > 0))
            {
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

                chunk.Add(new SortKey(line, readBuffer.AsMemory(sortKeyPos, sortKeyLen)));
                sortKeyPos += sortKeyLen;
                maxLineSize = Math.Max(maxLineSize, linePos);
                maxLineLength = Math.Max(maxLineLength, lineLen);
            }

            //Сортируем и записываем чанки на диск
            chunk.Sort(comparer);
            
            //SortHelpers.RadixQuickSort(CollectionsMarshal.AsSpan(chunk), selector);

            WriteChunk(chunk);

            chunk.Clear();

            //Осаток буфера переносим в начало
            if (remainingBytes > 0) readBuffer.AsSpan(parsePosition, remainingBytes).CopyTo(readBuffer.AsSpan(BufferMargin));
        }

        Console.WriteLine($"SplitSort done in {timer.Elapsed}");
    }


    char[]? line;
    byte[]? writeBuffer;
    void WriteChunk(List<SortKey> chunk)
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
            tempFile.Write(writeBuffer, 0, bytesConverted + sizeof(int));
            BinaryPrimitives.WriteInt32LittleEndian(writeBuffer, key.Length);
            key.CopyTo(writeBuffer.AsMemory(sizeof(int)));
            tempFile.Write(writeBuffer, 0, key.Length + sizeof(int));            
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

    private static int Main(string[] args)
    {
        if ((args?.Length ?? 0) == 0)
        {
            Console.WriteLine($"Usage: {Path.GetFileNameWithoutExtension(Environment.ProcessPath)} <file path> [<approximate chunk size> in MB]");
            return -1;
        }

        var file = args![0];
        var chunkSize = args?.Length > 1 ? int.Parse(args[1])  : 200; //В мегабайтах


        using var app = new Program(file, chunkSize * 1024 * 1024 / 2, StringComparison.CurrentCulture);
        app.SplitSort();
        app.Merge();

        return 0;
    }

}

