using Microsoft.Extensions.Configuration;
using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.IO.Compression;
using System.Reflection.Metadata.Ecma335;
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
    private readonly Encoding encoding;
    private readonly CompareOptions compareOptions;
    private readonly Comparer comparer;
    private readonly List<string> tempFiles = new();
    int maxLineSize = 0;
    int maxKeyLength = 0;
    long fileSize = 0;


    System.Diagnostics.Stopwatch timer = new();

    private Channel<(byte[], int)> readToParse;
    private Channel<(List<SortKey>, byte[], byte[])> parseToSort;
    private Channel<(List<SortKey>, byte[], byte[])> sortToCompress;
    private Channel<(byte[], int)> compressToWrite;
    private Task[] parserThreads;
    private Task[] sorterThreads;
    private Task[] compressThreads;
    private Task writerThread;
    private ArrayPool<byte>? pool = ArrayPool<byte>.Create();

    public Program(string file, Encoding encoding, StringComparison stringComparison, int chunkSize, int degreeOfParallelism)
    {
        this.file = file;
        this.maxChunkSize = chunkSize;
        this.degreeOfParallelism = degreeOfParallelism;
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
        this.encoding = encoding;
    }
    public void Dispose()
    {
        tempFiles.ForEach(File.Delete);
    }

    public async Task SplitSort()
    {
        timer.Restart();

        PrepareParallelWork();

        using var stream = new FileStream(file, FileMode.Open, FileAccess.Read, FileShare.Read, 0, FileOptions.SequentialScan);
        fileSize = stream.Length;

        List<SortKey>? chunk = null;
        byte[]? keyBuffer = null;
        char[]? charBuffer = null;

        var readBuffer = pool!.Rent(maxChunkSize);
        var remainingBytes = 0;
        var eof = false;


        while (!eof)
        {
            var bytesRead = stream.ReadBlock(readBuffer, remainingBytes, readBuffer.Length - remainingBytes, out eof);
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

        if (degreeOfParallelism == 0)
        {
            if (readBuffer != null) pool.Return(readBuffer);
            if (keyBuffer != null) pool.Return(keyBuffer);
        }

        await CompleteParralelWork();

        pool = null;
        GCSettings.LargeObjectHeapCompactionMode = GCLargeObjectHeapCompactionMode.CompactOnce;
        GC.Collect(2);

        Console.WriteLine($"SplitSort done in {timer.Elapsed}");
    }

    private void PrepareParallelWork()
    {
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
            sortToCompress = Channel.CreateBounded<(List<SortKey>, byte[], byte[])>(new BoundedChannelOptions(1)
            {
                SingleWriter = degreeOfParallelism == 1,
                SingleReader = degreeOfParallelism == 1
            });
            compressToWrite = Channel.CreateBounded<(byte[], int)>(new BoundedChannelOptions(1)
            {
                SingleWriter = degreeOfParallelism == 1,
                SingleReader = true
            });

            parserThreads =
                Enumerable
                .Range(0, degreeOfParallelism)
                .Select(_ => Task.Run(ParallelParser)).ToArray();

            sorterThreads =
                Enumerable
                .Range(0, degreeOfParallelism)
                .Select(_ => Task.Run(ParallelSorter)).ToArray();

            compressThreads =
                Enumerable
                .Range(0, degreeOfParallelism)
                .Select(_ => Task.Run(ParallelCompressor)).ToArray();

            writerThread = Task.Run(ParallelWriter);
        }
    }

    private async Task ParallelParser()
    {
        var charBuffer = new char[1024];
        await foreach (var (readBuffer, chunkSize) in readToParse.Reader.ReadAllAsync())
        {
            var keyBuffer = pool!.Rent(maxChunkSize);
            var chunk = ParseChunk(chunkSize, readBuffer, keyBuffer, charBuffer).ToList();
            await parseToSort.Writer.WriteAsync((chunk, readBuffer, keyBuffer));

        }
    }

    private async Task ParallelSorter()
    {
        await foreach (var item in parseToSort.Reader.ReadAllAsync())
        {
            item.Item1.Sort(comparer);
            await sortToCompress.Writer.WriteAsync(item);
        }
    }

    private async Task ParallelCompressor()
    {
        var buffer = new byte[1024]; //Buffer with margin
        var outputSize = BrotliEncoder.GetMaxCompressedLength(maxChunkSize * 2);
        await foreach (var (chunk, readBuffer, keyBuffer) in sortToCompress.Reader.ReadAllAsync())
        {
            using var encoder = new BrotliEncoder(4, 22);
            var output = pool!.Rent(outputSize);
            var dest = output.AsMemory();

            var compressed = 0;
            foreach (var sk in chunk)
            {
                if (sk.Length > buffer.Length)
                {
                    buffer = new byte[sk.Length];
                }

                sk.Write(buffer, 0);

                var source = buffer.AsMemory(0, sk.Length);
                while (true)
                {
                    var r = encoder.Compress(source.Span, dest.Span, out var bytesConsumed, out var bytesWritten, false);
                    compressed += bytesWritten;
                    if (bytesConsumed > 0) source = source[bytesConsumed..];
                    if (bytesWritten > 0) dest = dest[bytesWritten..];
                    if (r == OperationStatus.Done) break;
                    if (r == OperationStatus.InvalidData || r == OperationStatus.NeedMoreData)
                    {
                        throw new InvalidOperationException();
                    }
                    var old = output;
                    outputSize *= 2;
                    output = pool.Rent(outputSize);

                    old.CopyTo(output, 0);
                    pool.Return(old);
                    dest = output.AsMemory(compressed);

                }
            }

            while (true)
            {
                var r = encoder.Flush(dest.Span, out var bytesWritten);
                compressed += bytesWritten;
                if (r == OperationStatus.Done) break;
                if (r == OperationStatus.InvalidData || r == OperationStatus.NeedMoreData)
                {
                    throw new InvalidOperationException();
                }
                var old = output;
                outputSize *= 2;
                output = pool.Rent(outputSize);

                old.CopyTo(output, 0);
                pool.Return(old);
                dest = output.AsMemory(compressed);
            }
            outputSize = compressed * 11 / 10;
            await compressToWrite.Writer.WriteAsync((output, compressed));

            pool.Return(readBuffer);
            pool.Return(keyBuffer);
        }
    }

    private async Task ParallelWriter()
    {
        await foreach (var (buffer, bufferLength) in compressToWrite.Reader.ReadAllAsync())
        {
            var tempFileName = Path.ChangeExtension(file, $".part-{tempFiles.Count}.tmp");
            using (var tempFile = new FileStream(tempFileName, FileMode.Create, FileAccess.Write, FileShare.None, 0, FileOptions.SequentialScan))
            {
                await tempFile.WriteAsync(buffer.AsMemory(0, bufferLength));
            }
            pool!.Return(buffer);
            tempFiles.Add(tempFileName);
        }
    }

    private async Task CompleteParralelWork()
    {
        if (degreeOfParallelism > 0)
        {
            readToParse.Writer.Complete();
            await Task.WhenAll(parserThreads);
            parseToSort.Writer.Complete();
            await Task.WhenAll(sorterThreads);
            sortToCompress.Writer.Complete();
            await Task.WhenAll(compressThreads);
            compressToWrite.Writer.Complete();
            await writerThread;

        }
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

    private static async Task<int> Main(string[] args)
    {
        if ((args?.Length ?? 0) == 0)
        {
            Console.WriteLine($"Usage: {Path.GetFileNameWithoutExtension(Environment.ProcessPath)} <file path> [options]");
            return -1;
        }

        Dictionary<string, string> switchMappings = new()
               {
                   { "-d", "dop" },
                   { "-s", "size" },
                   { "-m", "mode" },
                   { "-e", "enc" },
                   { "--dop", "dop" },
                   { "--size", "size" },
                   { "--mode", "mode" },
                   { "--encoding", "enc" },
               };
        var config = new ConfigurationBuilder()
            .AddCommandLine(args!, switchMappings)
            .Build();



        if (!int.TryParse(config["dop"], out var dop))
        {
            dop = Environment.ProcessorCount / 4;
        }
        dop = int.Clamp(dop, 0, int.Max((Environment.ProcessorCount - 4) / 3 + 1, 1));

        if (!int.TryParse(config["size"], out var chunkSize))
        {
            chunkSize = 200 / int.Max(dop, 1);
        }
        chunkSize = int.Clamp(chunkSize, 100, int.MaxValue / (1024 * 1024));

        if (!Enum.TryParse<StringComparison>(config["mode"], out var comparison))
        {
            comparison = StringComparison.CurrentCulture;
        }

        var encoding = Encoding.UTF8;
        var enc = config["enc"];
        if (enc != null) encoding = Encoding.GetEncoding(enc);


        var file = args![0];
        try
        {
            using var app = new Program(file, encoding, comparison, chunkSize * 1024 * 1024, dop);
            await app.SplitSort();
            app.Merge();
        }
        catch (Exception e)
        {
            await Console.Out.WriteLineAsync(e.ToString());
            throw;
        }

        return 0;
    }

}

