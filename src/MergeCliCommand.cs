using System.Diagnostics;
using System.Globalization;
using System.IO.Compression;
using System.IO.Pipelines;
using System.Text;
using System.Threading.Channels;
using DotMake.CommandLine;

/// <summary>
/// Команда merge и конвейер слияния: продюсер на каждый чанк читает и разжимает его пакетами,
/// sort key пакета вычисляется под глобальным CPU-слотом; k-way merge по sort key через хип
/// (MergeSorted); запись — через Pipe (глубокие пороги) в выделенный поток с sync FileStream
/// без буфера и с преаллокацией. Стадии перекрываются, backpressure приходит только от записи на диск.
/// </summary>
[CliCommand(Parent = typeof(RootCliCommand), Description = "Merge sorted chunk files listed on stdin into one file")]
internal partial class MergeCliCommand
{
    // Размер пакета записей (несжатых байт): определяет глубину опережения чтения каждого чанка
    // и память под буферы пакетов (тексты + sort key ~1-2x текста).
    private const int BatchBytes = 4 * 1024 * 1024;

    [CliArgument(Description = "Destination file (will be overwritten)",
        ValidationRules = CliValidationRules.LegalPath)]
    public FileInfo Destination { get; set; } = null!;

    [CliOption(Name = "flush-mb", Description = "Unflushed bytes threshold for the output writer, MB (default: 32)")]
    public int FlushMb { get; set; } = 32;

    [CliOption(Name = "compare-options", Description = "Text comparison used at split: None (default), Ordinal, IgnoreCase, IgnoreNonSpace, IgnoreSymbols, or a combination")]
    public string CompareOptionsName { get; set; } = "None";

    /// <summary>Точный размер итогового файла для преаллокации; передаётся командой sort.</summary>
    internal long? PreallocatedLength { get; set; }

    public Task<int> RunAsync(CliContext context) =>
        RootCliCommand.RunWithSettingsAsync(CompareOptionsName, ExecuteAsync, context);

    private async Task<int> ExecuteAsync(Encoding encoding, CompareOptions compareOptions, CancellationToken ct)
    {
        List<FileInfo> chunks = [];
        while (Console.ReadLine() is { } line)
        {
            line = line.Trim();
            if (line.Length != 0)
                chunks.Add(new FileInfo(line));
        }
        await MergeAsync(encoding, compareOptions, chunks, ct);
        return 0;
    }

    /// <summary>Мержит чанки в Destination с готовыми настройками; используется и командой sort.</summary>
    internal async Task MergeAsync(Encoding encoding, CompareOptions compareOptions, IReadOnlyList<FileInfo> chunkFiles, CancellationToken ct)
    {
        var sw = Stopwatch.StartNew();
        var newLine = encoding.GetBytes(Environment.NewLine);
        var dotSpace = encoding.GetBytes(". ");
        var flushThreshold = FlushMb * 1024 * 1024;
        // Запись: Pipe позволяет циклу merge опережать диск на 2x flushThreshold.
        var pipe = new Pipe(new PipeOptions(
            minimumSegmentSize: 1 << 20,
            pauseWriterThreshold: (long)flushThreshold * 2,
            resumeWriterThreshold: flushThreshold,
            useSynchronizationContext: false));
        using var output = new FileStream(Destination.FullName, FileMode.Create, FileAccess.Write, FileShare.None,
            bufferSize: 0, FileOptions.SequentialScan);
        if (PreallocatedLength is { } size)
            output.SetLength(size);
        output.Seek(0, SeekOrigin.Begin);
        var drainer = Task.Factory.StartNew(() => DrainPipe(pipe.Reader, output, ct), ct, TaskCreationOptions.LongRunning, TaskScheduler.Default);

        // Продюсеры чанков: sort key пакетов считаются под глобальным CPU-слотом (=ProcessorCount),
        // чтобы суммарная CPU-работа не порождала больше активных потоков, чем ядер.
        var cpuSlots = new SemaphoreSlim(Environment.ProcessorCount);
        var channels = new Channel<Batch>[chunkFiles.Count];
        var producers = new Task[chunkFiles.Count];
        for (var i = 0; i < chunkFiles.Count; i++)
        {
            var file = chunkFiles[i];
            var channel = Channel.CreateBounded<Batch>(new BoundedChannelOptions(2) { SingleWriter = true, SingleReader = true });
            channels[i] = channel;
            producers[i] = ProduceChunk(file, channel, cpuSlots, encoding, compareOptions, ct);
        }

        Exception? mergeError = null;
        try
        {
            var sources = channels.Select(c => MergeExtensions.ReadEntries(c.Reader, ct));
            var number = new char[32];
            var writer = pipe.Writer;
            await foreach (var group in sources.MergeSorted(ChunkEntryComparer.Instance, MergeGroup, ct).WithCancellation(ct).ConfigureAwait(false))
            {
                foreach (var (value, text) in group)
                {
                    value.TryFormat(number, out var digits, null, null);
                    encoding.GetBytes(number.AsSpan(0, digits), writer);
                    writer.Write(dotSpace);
                    writer.Write(text.AsSpan());
                    writer.Write(newLine);
                }
                if (writer.UnflushedBytes >= flushThreshold)
                    await writer.FlushAsync(ct).ConfigureAwait(false);
            }
            await writer.FlushAsync(ct).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            mergeError = ex;
        }
        finally
        {
            await pipe.Writer.CompleteAsync().ConfigureAwait(false);
            try
            {
                await drainer.ConfigureAwait(false);
                // Обрезаем преаллокацию по фактическому размеру.
                if (PreallocatedLength is not null)
                    output.SetLength(output.Position);
            }
            catch (Exception ex)
            {
                mergeError ??= ex;
            }
        }
        if (mergeError is not null)
            throw mergeError;
        Console.WriteLine($"Merged: {Destination.FullName}");
        Console.WriteLine(sw.Elapsed);
    }

    /// <summary>Продюсер одного чанка: декомпрессия → пакеты записей → канал.</summary>
    private async Task ProduceChunk(FileInfo file, Channel<Batch> channel, SemaphoreSlim cpuSlots, Encoding encoding, CompareOptions compareOptions, CancellationToken ct)
    {
        try
        {
            var fileStream = new FileStream(file.FullName, FileMode.Open, FileAccess.Read, FileShare.Read,
                bufferSize: 4 << 20, FileOptions.SequentialScan | FileOptions.Asynchronous);
            // Окно сжатия чанков ограничено бюджетом (до 2^31) — разрешаем декодеру столько же.
            await using var zstd = new ZstandardStream(fileStream,
                new ZstandardDecompressionOptions { MaxWindowLog2 = 31 }, leaveOpen: false);
            await zstd.ProduceBatches(channel.Writer, encoding, CultureInfo.CurrentCulture.CompareInfo,
                compareOptions, cpuSlots, BatchBytes, ct).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            channel.Writer.TryComplete(ex);
            throw;
        }
    }

    /// <summary>Потребитель Pipe: пишет данные в файл синхронно (выделенный поток, буфер FileStream отключён).</summary>
    private static void DrainPipe(PipeReader reader, FileStream output, CancellationToken ct)
    {
        try
        {
            while (true)
            {
                var result = reader.ReadAsync(ct).AsTask().GetAwaiter().GetResult();
                var buffer = result.Buffer;
                foreach (var segment in buffer)
                    output.Write(segment.Span);
                reader.AdvanceTo(buffer.End);
                if (result.IsCompleted)
                    break;
            }
            reader.Complete();
        }
        catch (Exception ex)
        {
            reader.Complete(ex);
            throw;
        }
    }

    private static readonly Comparer<(long Value, int Index)> ValueIndexComparer =
        Comparer<(long Value, int Index)>.Create((x, y) => x.Value.CompareTo(y.Value));

    /// <summary>
    /// Сливает записи одной культурной группы (равные sort key, тексты могут различаться,
    /// например «ß» и «ss» в de-DE) в единый поток (значение, текст) по возрастанию значения.
    /// Штатный случай — одна запись в группе: значения отдаются напрямую, без хипа и LINQ-цепочек.
    /// </summary>
    private IEnumerable<(long Value, ArraySegment<byte> Text)> MergeGroup(IReadOnlyList<ReadChunkEntry> group)
    {
        if (group.Count == 1)
            return SingleGroup(group[0]);

        return group
            .Select((entry, index) => EnumerateValues(entry, index))
            .MergeSorted(ValueIndexComparer)
            .Select(pair => (pair.Value, group[pair.Index].Text));

        static IEnumerable<(long Value, ArraySegment<byte> Text)> SingleGroup(ReadChunkEntry entry)
        {
            var text = entry.Text;
            var values = entry.ValuesArray;
            for (var i = 0; i < entry.ValuesCount; i++)
                yield return (values[entry.ValuesStart + i], text);
        }

        static IEnumerable<(long Value, int Index)> EnumerateValues(ReadChunkEntry entry, int index)
        {
            for (var i = 0; i < entry.ValuesCount; i++)
                yield return (entry.ValuesArray[entry.ValuesStart + i], index);
        }
    }
}
