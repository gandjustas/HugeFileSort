using BenchmarkDotNet.Attributes;
using System;
using System.Buffers;
using System.Collections;
using System.Collections.Generic;
using System.IO.Pipelines;
using System.Linq;
using System.Net.Mail;
using System.Reflection.PortableExecutable;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;
using System.Xml;

[MemoryDiagnoser]
public class ReadLines
{
    private readonly string file = "testfile.txt";
    const int BufferSize = 1024 * 1024;

    [MethodImpl(MethodImplOptions.NoInlining | MethodImplOptions.NoOptimization)]
    private static void Noop(ReadOnlySpan<char> s)
    {
#if DEBUG
        Console.WriteLine(s.ToString());
#endif
        s = s[(s.IndexOf('.') + 1)..];
    }

    [Benchmark]
    public void FileReadLines()
    {
        foreach (var line in File.ReadLines(file))
        {
            Noop(line);
        }
    }

    [Benchmark]
    public void StreamReader()
    {
        using var reader = new StreamReader(file);
        string? line;
        while ((line = reader.ReadLine()) != null)
        {
            Noop(line);

        }
    }

    private IEnumerable<ReadOnlyMemory<char>> IterateReader(StreamReader reader, int bufferSize)
    {
        var chunkBuffer = new char[bufferSize];
        var chunkReadPosition = 0;
        while (true)
        {
            var charsRead = reader.ReadBlock(chunkBuffer, chunkReadPosition, chunkBuffer.Length - chunkReadPosition);
            var eos = reader.EndOfStream;
            var m = chunkBuffer.AsMemory(0, chunkReadPosition + charsRead);

            int linePos;
            while ((linePos = m.Span.IndexOf(Environment.NewLine)) >= 0)
            {
                var line = m[..linePos];
                yield return line;
                m = m[(linePos + Environment.NewLine.Length)..];
            }

            // Если это был конец файла, то добавим в список последнюю строку, если она не пустая
            if (eos && m.Length > 0)
            {
                yield return m;
            }


            if (eos) break;

            //Отсток буфера переносим в начало
            m.CopyTo(chunkBuffer);
            chunkReadPosition = m.Length;
        }
    }

    [Benchmark(Baseline = true)]
    public void ReaderIterateArrayOfChars()
    {
        using var reader = new StreamReader(file);
        foreach (var line in IterateReader(reader, BufferSize))
        {
            Noop(line.Span);
        }
    }

    [Benchmark]
    public void ReaderIterateArrayOfChars_LargeBuffer()
    {
        using var reader = new StreamReader(file, Encoding.UTF8, false, BufferSize);
        foreach (var line in IterateReader(reader, BufferSize))
        {
            Noop(line.Span);
        }
    }

    [Benchmark]
    public void ReaderIterateArrayOfChars_LargeBuffer_NoBuffering()
    {
        using var stream = new FileStream(file, FileMode.Open, FileAccess.Read, FileShare.None, 0, FileOptions.SequentialScan | (FileOptions)0x20000000);
        using var reader = new StreamReader(stream, Encoding.UTF8, false, BufferSize);
        foreach (var line in IterateReader(reader, BufferSize))
        {
            Noop(line.Span);
        }
    }

    private static readonly byte[] NewLine = Encoding.UTF8.GetBytes(Environment.NewLine);
    private IEnumerable<ReadOnlyMemory<byte>> IterateStream(FileStream stream, int bufferSize)
    {
        var chunkBuffer = new byte[bufferSize];
        var chunkReadPosition = 0;
        var m = chunkBuffer.AsMemory();
        while (true)
        {
            var charsRead = stream.Read(chunkBuffer, chunkReadPosition, chunkBuffer.Length - chunkReadPosition);
            if (charsRead == 0)
            {
                if (m.Length > 0) yield return m;
                break;
            }

            m = chunkBuffer.AsMemory(0, chunkReadPosition + charsRead);

            int linePos;
            while ((linePos = m.Span.IndexOf(NewLine)) >= 0)
            {
                var line = m[..linePos];
                yield return line;
                m = m[(linePos + NewLine.Length)..];
            }

            //Отсток буфера переносим в начало
            m.CopyTo(chunkBuffer);
            chunkReadPosition = m.Length;
        }
    }

    private void StreamIterate(FileStream stream)
    {
        foreach (var bytes in IterateStream(stream, BufferSize))
        {
            ProcessBytes(bytes);
        }
    }

    private IEnumerable<ReadOnlyMemory<byte>> IterateStreamNoBuffering(FileStream stream, int bufferSize)
    {
        var chunkBuffer = new byte[bufferSize * 2];
        Memory<byte> m = new ();
        while (true)
        {
            var charsRead = stream.Read(chunkBuffer, bufferSize, chunkBuffer.Length / 2);
            if (charsRead == 0)
            {
                if (m.Length > 0) yield return m;
                break;
            }
            var bufferLen = m.Length + charsRead;
            m = chunkBuffer.AsMemory(bufferSize - m.Length, bufferLen);

            int linePos;
            while ((linePos = m.Span.IndexOf(NewLine)) >= 0)
            {
                var line = m[..linePos];
                yield return line;
                m = m[(linePos + NewLine.Length)..];
            }

            
            m.CopyTo(chunkBuffer.AsMemory(bufferSize - m.Length,m.Length));
        }
    }

    private void StreamIterateNoBuffering(FileStream stream)
    {
        foreach (var bytes in IterateStreamNoBuffering(stream, BufferSize))
        {
            ProcessBytes(bytes);
        }
    }


    private static void ProcessBytes(ReadOnlyMemory<byte> bytes)
    {
        Span<char> line = stackalloc char[bytes.Length];
        Encoding.UTF8.GetChars(bytes.Span, line);
        Noop(line);
    }

    [Benchmark]
    public void StreamIterateArrayOfBytes()
    {
        using var stream = new FileStream(file, FileMode.Open);
        StreamIterate(stream);
    }

    [Benchmark]
    public void StreamIterateArrayOfBytes_NoBuffering()
    {
        using var stream = new FileStream(file, FileMode.Open, FileAccess.Read, FileShare.None, 0, FileOptions.SequentialScan | (FileOptions)0x20000000);
        StreamIterateNoBuffering(stream);
    }

    [Benchmark]
    public void Pipelines_Default()
    {
        using var stream = new FileStream(file, FileMode.Open);
        ReadLineUsingPipelineVer2Async(stream).Wait();
    }

    [Benchmark]
    public void Pipelines_Buffer()
    {
        using var stream = new FileStream(file, FileMode.Open);
        ReadLineUsingPipelineVer2Async(stream, BufferSize).Wait();
    }

    [Benchmark]
    public void Pipelines_Buffer_NoBuffering()
    {
        using var stream = new FileStream(file, FileMode.Open, FileAccess.Read, FileShare.None, 0, FileOptions.SequentialScan | (FileOptions)0x20000000);
        ReadLineUsingPipelineVer2Async(stream, BufferSize).Wait();
    }

    public async Task ReadLineUsingPipelineVer2Async(FileStream stream, int bufferSize = -1)
    {
        var reader = PipeReader.Create(stream, new StreamPipeReaderOptions(bufferSize: bufferSize, leaveOpen: true));

        while (true)
        {
            var result = await reader.ReadAsync();
            var buffer = result.Buffer;

            ProcessLine(ref buffer);

            reader.AdvanceTo(buffer.Start, buffer.End);

            if (result.IsCompleted) break;
        }

        await reader.CompleteAsync();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void ProcessLine(ref ReadOnlySequence<byte> buffer)
    {


        if (buffer.IsSingleSegment)
        {
            var span = buffer.FirstSpan;
            int consumed;
            while (span.Length > 0)
            {
                var newLine = span.IndexOf(NewLine);

                if (newLine == -1) break;

                var line = span[..newLine];

                ProcessLine(line);

                consumed = line.Length + NewLine.Length;
                span = span.Slice(consumed);
                buffer = buffer.Slice(consumed);
            }
        }
        else
        {
            var sequenceReader = new SequenceReader<byte>(buffer);

            while (!sequenceReader.End)
            {
                while (sequenceReader.TryReadTo(out ReadOnlySpan<byte> line, NewLine))
                {
                    ProcessLine(line);
                }

                buffer = buffer.Slice(sequenceReader.Position);
                sequenceReader.Advance(buffer.Length);
            }
        }

    }

    private static void ProcessLine(ReadOnlySpan<byte> bytes)
    {
        Span<char> line = stackalloc char[bytes.Length];
        Encoding.UTF8.GetChars(bytes, line);
        Noop(line);
    }

}
