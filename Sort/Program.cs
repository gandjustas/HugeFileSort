using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.IO.Compression;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;


internal class Program : IDisposable
{
    const int BufferSize = 1024 * 1024;
    private static readonly byte[] NewLine = Encoding.UTF8.GetBytes(Environment.NewLine);

    private readonly string file;
    private readonly int maxChunkSize;
    private readonly CultureInfo culture;
    private readonly Encoding encoding = Encoding.UTF8;
    private readonly CompareOptions compareOptions;
    private readonly Comparer comparer;
    private readonly List<string> tempFiles = new();
    int maxLineSize = 0;
    int maxKeyLength = 0;
    long fileSize = 0;


    System.Diagnostics.Stopwatch timer = new();


    public Program(string file, int chunkSize, StringComparison stringComparison)
    {
        this.file = file;
        this.maxChunkSize = chunkSize;
        this.comparer = new Comparer();
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

        using var stream = new FileStream(file, FileMode.Open, FileAccess.Read, FileShare.Read, 0, FileOptions.SequentialScan);
        fileSize = stream.Length;

        List<SortKey> chunk = new();

        var keyBuffer = new byte[maxChunkSize];
        var readBuffer = new byte[maxChunkSize];
        var remainingBytes = 0;

        var charBuffer = new char[1024];
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

            chunk.AddRange(ParseChunk(chunkSize, readBuffer, keyBuffer, charBuffer));

            //Сортируем и записываем чанки на диск
            chunk.Sort(comparer);

            //SortHelpers.RadixQuickSort(CollectionsMarshal.AsSpan(chunk), selector);

            WriteChunk(chunk);

            chunk.Clear();

            //Осаток буфера переносим в начало
            if (remainingBytes > 0) readBuffer.AsSpan(chunkSize, remainingBytes).CopyTo(readBuffer.AsSpan());
        }

        Console.WriteLine($"SplitSort done in {timer.Elapsed}");
    }

    private IEnumerable<SortKey> ParseChunk(int byteCount, byte[] readBuffer, byte[] keyBuffer, char[] charBuffer)
    {
        var readPos = 0;
        var key = keyBuffer.AsMemory();
        while (byteCount > 0)
        {
            var linePos = readBuffer.AsSpan(readPos, byteCount).IndexOf(NewLine);
            if (linePos == -1) linePos = byteCount;
            if (charBuffer.Length < linePos) charBuffer = new char[linePos];

            // Надо обязательно вызывать именно эту перегрузку, потому что остальные аллоцируют память
            var lineLen = encoding.GetChars(readBuffer, readPos, linePos, charBuffer, 0);
            var line = charBuffer.AsMemory(0, lineLen);
            var s = line.Span;
            var dot = s.IndexOf('.');
            var x = int.Parse(s[0..dot]);

            var keyLen = culture.CompareInfo.GetSortKey(s[(dot + 2)..], key.Span, compareOptions);
            BinaryPrimitives.WriteInt32BigEndian(key[keyLen..].Span, x);
            keyLen += sizeof(int);

            var lineSize = linePos + NewLine.Length;
            yield return new SortKey(readBuffer.AsMemory(readPos, lineSize), key[..keyLen]);
            key = key[keyLen..];

            readPos += lineSize;
            byteCount -= lineSize;
            maxLineSize = Math.Max(maxLineSize, lineSize);
            maxKeyLength = Math.Max(maxKeyLength, keyLen);
        }
    }

    void WriteChunk(List<SortKey> chunk)
    {
        // Записываем строки из отсортированного списка во временный файл
        var tempFileName = Path.ChangeExtension(file, $".part-{tempFiles.Count}.tmp");
        using var tempFile = new FileStream(tempFileName, FileMode.Create, FileAccess.Write, FileShare.None, 0, FileOptions.SequentialScan);
        using var stream = new BrotliStream(tempFile, CompressionMode.Compress);

        Span<byte> buffer = stackalloc byte[sizeof(int)];
        foreach (var (line, key) in chunk)
        {
            BinaryPrimitives.WriteInt32LittleEndian(buffer, line.Length);
            stream.Write(buffer);
            stream.Write(line.Span);

            BinaryPrimitives.WriteInt32LittleEndian(buffer, key.Length);
            stream.Write(buffer);
            stream.Write(key.Span);
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
        using var f = new FileStream(file, FileMode.Open, FileAccess.Read, FileShare.Read, 0, FileOptions.SequentialScan);
        using var stream = new BrotliStream(f, CompressionMode.Decompress);

        var maxBlockSize = maxLineSize + maxKeyLength + sizeof(int) * 2;
        var readBuffer = new byte[Math.Max(BufferSize, maxBlockSize)];

        var bytesRemaining = 0;
        var eof = false;

        while (!eof)
        {
            var bytesRead = stream.ReadBlock(readBuffer, bytesRemaining, readBuffer.Length - bytesRemaining, out eof);
            if (bytesRead == 0) eof = true;
            var mem = readBuffer.AsMemory(0, bytesRemaining + bytesRead);

            while (mem.Length > maxBlockSize || (eof && mem.Length > 0))
            {

                var lineSize = BinaryPrimitives.ReadInt32LittleEndian(mem.Span);
                mem = mem[sizeof(int)..];

                var line = mem[..lineSize];
                mem = mem[lineSize..];

                var keyLen = BinaryPrimitives.ReadInt32LittleEndian(mem.Span);
                mem = mem[sizeof(int)..];

                yield return new SortKey(line, mem[..keyLen]);
                mem = mem[keyLen..];
            }

            mem.CopyTo(readBuffer);

            bytesRemaining = mem.Length;
        }
    }

    private static int Main(string[] args)
    {
        if ((args?.Length ?? 0) == 0)
        {
            Console.WriteLine($"Usage: {Path.GetFileNameWithoutExtension(Environment.ProcessPath)} <file path> [<max chunk size> in MB]");
            return -1;
        }

        var file = args![0];
        var chunkSize = args?.Length > 1 ? int.Parse(args[1]) : 200; //В мегабайтах        
        chunkSize = int.Clamp(chunkSize, 100, int.MaxValue / (1024 * 1024));

        using var app = new Program(file, chunkSize * 1024 * 1024, StringComparison.CurrentCulture);
        app.SplitSort();
        app.Merge();

        return 0;
    }

}

