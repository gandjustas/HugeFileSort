using System.IO.Pipelines;
using System.Runtime.ExceptionServices;
using System.Text;
using BenchmarkDotNet.Attributes;

/// <summary>Входной файл бенчмарков: UTF-8 строки фиксированной байтовой длины вида "N. текст".</summary>
static class InputFile
{
    /// <summary>Размер блока чтения во всех сценариях.</summary>
    public const int BlockSize = 1024 * 1024;

    const string LineBody = "некоторый текст строки бенчмарка 0123456789";

    /// <summary>Размер входного файла в МБ, переопределяется переменной окружения HFS_BENCH_INPUT_MB.</summary>
    public static long SizeMb
    {
        get
        {
            var raw = Environment.GetEnvironmentVariable("HFS_BENCH_INPUT_MB");
            return long.TryParse(raw, out var mb) && mb > 0 ? mb : 256;
        }
    }

    /// <summary>Полный путь к входному файлу, переопределяется переменной окружения HFS_BENCH_INPUT_PATH.</summary>
    public static string FullPath { get; } =
        Environment.GetEnvironmentVariable("HFS_BENCH_INPUT_PATH") is { Length: > 0 } path
            ? path
            : Path.Combine(Path.GetTempPath(), "hfs-bench-input.txt");

    /// <summary>Ожидаемый размер файла при текущем <see cref="SizeMb"/> (число полных строк).</summary>
    public static long ExpectedLength
    {
        get
        {
            var lineBytes = Encoding.UTF8.GetByteCount($"{0L,12}. {LineBody}\n");
            return SizeMb * 1024 * 1024 / lineBytes * lineBytes;
        }
    }

    /// <summary>Блок для синхронного чтения: неинициализированный массив 1 МБ, переиспользуется без аллокаций.</summary>
    public static readonly byte[] Block = GC.AllocateUninitializedArray<byte>(BlockSize);

    /// <summary>Файл открывается без внутреннего буфера FileStream: чтения идут напрямую в блоки вызывающего.</summary>
    public static FileStream OpenRead() =>
        new(FullPath, FileMode.Open, FileAccess.Read, FileShare.Read, bufferSize: 0, FileOptions.SequentialScan);

    /// <summary>Создаёт файл, если он отсутствует или имеет другой размер.</summary>
    public static void EnsureCreated()
    {
        FileInfo info = new(FullPath);
        if (info.Exists && info.Length == ExpectedLength)
            return;

        var lineBytes = Encoding.UTF8.GetByteCount($"{0L,12}. {LineBody}\n");
        var lineCount = SizeMb * 1024 * 1024 / lineBytes;

        using FileStream fs = new(FullPath, FileMode.Create, FileAccess.Write, FileShare.None, BlockSize);
        using StreamWriter writer = new(fs, new UTF8Encoding(encoderShouldEmitUTF8Identifier: false), BlockSize);
        for (long i = 0; i < lineCount; i++)
        {
            writer.Write($"{i,12}. {LineBody}");
            writer.Write('\n');
        }
    }

    public static void Delete() => File.Delete(FullPath);
}

/// <summary>Сравнение способов чтения большого файла: подсчёт переводов строк '\n' в UTF-8 файле.</summary>
[MemoryDiagnoser]
public class FileReadingBenchmarks
{
    [GlobalSetup]
    public void Setup() => InputFile.EnsureCreated();

    [GlobalCleanup]
    public void Cleanup() => InputFile.Delete();

    /// <summary>Базовый сценарий: синхронный FileStream.Read в блок 1 МБ.</summary>
    [Benchmark(Baseline = true)]
    public long FileStreamSync() => CountLinesFileStreamSync();

    /// <summary>PipeReader.Create поверх FileStream: чтение и подсчёт в одном потоке.</summary>
    [Benchmark]
    public Task<long> PipeReaderOverStream() => CountLinesPipeReaderOverStreamAsync();

    /// <summary>Pipe и выделенный поток-продюсер: синхронно читает файл блоками в Pipe, потребитель считает параллельно.</summary>
    [Benchmark]
    public Task<long> PipeWithProducerThread() => CountLinesPipeWithProducerThreadAsync();

    /// <summary>Синхронное чтение блоками 1 МБ в переиспользуемый неинициализированный массив.</summary>
    public static long CountLinesFileStreamSync()
    {
        using var fs = InputFile.OpenRead();
        long lines = 0;
        int read;
        while ((read = fs.Read(InputFile.Block)) > 0)
            lines += InputFile.Block.AsSpan(0, read).Count((byte)'\n');
        return lines;
    }

    /// <summary>Чтение через PipeReader с сегментами 1 МБ, подсчёт по сегментам последовательности.</summary>
    public static async Task<long> CountLinesPipeReaderOverStreamAsync()
    {
        using var fs = InputFile.OpenRead();
        PipeReader reader = PipeReader.Create(fs, new StreamPipeReaderOptions(bufferSize: InputFile.BlockSize));
        long lines = 0;
        while (true)
        {
            var result = await reader.ReadAsync().ConfigureAwait(false);
            var buffer = result.Buffer;
            foreach (var segment in buffer)
                lines += segment.Span.Count((byte)'\n');
            reader.AdvanceTo(buffer.End);
            if (result.IsCompleted)
                break;
        }
        await reader.CompleteAsync().ConfigureAwait(false);
        return lines;
    }

    /// <summary>Продюсер в отдельном потоке синхронно читает файл прямо в память Pipe (без копий), потребитель считает строки.</summary>
    public static async Task<long> CountLinesPipeWithProducerThreadAsync()
    {
        Pipe pipe = new(new PipeOptions(
            pauseWriterThreshold: 4L * InputFile.BlockSize,
            resumeWriterThreshold: 2L * InputFile.BlockSize));

        Exception? producerError = null;
        Thread producer = new(() =>
        {
            try
            {
                using var fs = InputFile.OpenRead();
                var writer = pipe.Writer;
                while (true)
                {
                    var memory = writer.GetMemory(InputFile.BlockSize);
                    int read = fs.Read(memory.Span);
                    if (read == 0)
                        break;
                    writer.Advance(read);
                    writer.FlushAsync().AsTask().GetAwaiter().GetResult();
                }
                writer.Complete();
            }
            catch (Exception ex)
            {
                producerError = ex;
                pipe.Writer.Complete(ex);
            }
        })
        { IsBackground = true };
        producer.Start();

        try
        {
            long lines = 0;
            var reader = pipe.Reader;
            while (true)
            {
                var result = await reader.ReadAsync().ConfigureAwait(false);
                var buffer = result.Buffer;
                foreach (var segment in buffer)
                    lines += segment.Span.Count((byte)'\n');
                reader.AdvanceTo(buffer.End);
                if (result.IsCompleted)
                    break;
            }
            reader.Complete();
            producer.Join();
            if (producerError is not null)
                ExceptionDispatchInfo.Capture(producerError).Throw();
            return lines;
        }
        catch
        {
            pipe.Reader.Complete(new IOException("Потребитель завершился с ошибкой"));
            producer.Join();
            throw;
        }
    }
}
