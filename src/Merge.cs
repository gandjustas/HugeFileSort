using System.Diagnostics;
using System.Globalization;
using System.IO.Compression;
using System.IO.Pipelines;
using System.Numerics;
using System.Text;
using System.Threading.Channels;

/// <summary>
/// Фаза слияния: продюсер на каждый чанк читает и разжимает его пакетами, sort key пакета
/// вычисляется под глобальным CPU-слотом; k-way merge по sort key через хип (MergeSorted);
/// запись — через Pipe (глубокие пороги) в выделенный поток с sync FileStream без буфера
/// и с преаллокацией. Стадии перекрываются, backpressure приходит только от записи на диск.
/// </summary>
class Merge<T>(FileInfo destination, Encoding encoding, int flushMb, CompareOptions compareOptions, long? preallocatedLength = null) where T : IBinaryInteger<T>
{
    // Размер пакета записей (несжатых байт): определяет глубину опережения чтения каждого чанка
    // и память под буферы пакетов (тексты + sort key ~1-2x текста).
    private const int BatchBytes = 4 * 1024 * 1024;

    readonly byte[] newLine = encoding.GetBytes(Environment.NewLine);
    readonly byte[] dotSpace = encoding.GetBytes(". ");
    readonly CompareOptions compareOptions = compareOptions;
    readonly int flushThreshold = flushMb * 1024 * 1024;

    /// <summary>
    /// Мержит отсортированные чанки в destination: культурно равные записи (равные sort key)
    /// группируются и сливаются по значению.
    /// </summary>
    public async Task RunAsync(IReadOnlyList<FileInfo> chunkFiles, CancellationToken ct)
    {
        var sw = Stopwatch.StartNew();
        // Запись: Pipe позволяет циклу merge опережать диск на 2x flushThreshold.
        var pipe = new Pipe(new PipeOptions(
            minimumSegmentSize: 1 << 20,
            pauseWriterThreshold: (long)flushThreshold * 2,
            resumeWriterThreshold: flushThreshold,
            useSynchronizationContext: false));
        using var output = new FileStream(destination.FullName, FileMode.Create, FileAccess.Write, FileShare.None,
            bufferSize: 0, FileOptions.SequentialScan);
        if (preallocatedLength is { } size)
            output.SetLength(size);
        output.Seek(0, SeekOrigin.Begin);
        var drainer = Task.Factory.StartNew(() => DrainPipe(pipe.Reader, output, ct), ct, TaskCreationOptions.LongRunning, TaskScheduler.Default);

        // Продюсеры чанков: sort key пакетов считаются под глобальным CPU-слотом (=ProcessorCount),
        // чтобы суммарная CPU-работа не порождала больше активных потоков, чем ядер.
        var cpuSlots = new SemaphoreSlim(Environment.ProcessorCount);
        var channels = new Channel<Batch<T>>[chunkFiles.Count];
        var producers = new Task[chunkFiles.Count];
        for (var i = 0; i < chunkFiles.Count; i++)
        {
            var file = chunkFiles[i];
            var channel = Channel.CreateBounded<Batch<T>>(new BoundedChannelOptions(2) { SingleWriter = true, SingleReader = true });
            channels[i] = channel;
            producers[i] = ProduceChunk(file, channel, cpuSlots, ct);
        }

        Exception? mergeError = null;
        try
        {
            var sources = channels.Zip(chunkFiles, (c, f) => (c, f))
                .Select(pair => MergeExtensions.ReadEntries(pair.c.Reader, pair.f.Name, ct));
            var number = new char[32];
            var writer = pipe.Writer;
            await foreach (var group in sources.MergeSorted(ChunkEntryComparer<T>.Instance, MergeGroup, ct).WithCancellation(ct).ConfigureAwait(false))
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
                if (preallocatedLength is not null)
                    output.SetLength(output.Position);
            }
            catch (Exception ex)
            {
                mergeError ??= ex;
            }
        }
        if (mergeError is not null)
            throw mergeError;
        Console.WriteLine($"Merged: {destination.FullName}");
        Console.WriteLine(sw.Elapsed);
    }

    /// <summary>Продюсер одного чанка: декомпрессия → пакеты записей → канал.</summary>
    private async Task ProduceChunk(FileInfo file, Channel<Batch<T>> channel, SemaphoreSlim cpuSlots, CancellationToken ct)
    {
        try
        {
            var fileStream = new FileStream(file.FullName, FileMode.Open, FileAccess.Read, FileShare.Read,
                bufferSize: 4 << 20, FileOptions.SequentialScan | FileOptions.Asynchronous);
            // Окно сжатия чанков доходит до 2^31 — разрешаем декодеру столько же.
            await using var zstd = new ZstandardStream(fileStream,
                new ZstandardDecompressionOptions { MaxWindowLog2 = 31 }, leaveOpen: false);
            await zstd.ProduceBatches(channel.Writer, encoding, CultureInfo.CurrentCulture.CompareInfo,
                compareOptions, cpuSlots, BatchBytes, file.Name, ct).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            channel.Writer.TryComplete(ex);
            throw;
        }
    }

    /// <summary>Потребитель Pipe: пишет данные в файл синхронно (выделенный поток, буфер FileStream отключён).</summary>
    private void DrainPipe(PipeReader reader, FileStream output, CancellationToken ct)
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

    private static readonly Comparer<(T Value, int Index)> ValueIndexComparer =
        Comparer<(T Value, int Index)>.Create((x, y) => x.Value.CompareTo(y.Value));

    /// <summary>
    /// Сливает записи одной культурной группы (равные sort key, тексты могут различаться,
    /// например «ß» и «ss» в de-DE) в единый поток (значение, текст) по возрастанию значения.
    /// Штатный случай — одна запись в группе: значения отдаются напрямую, без хипа и LINQ-цепочек.
    /// </summary>
    private IEnumerable<(T Value, ArraySegment<byte> Text)> MergeGroup(IReadOnlyList<ReadChunkEntry<T>> group)
    {
        if (group.Count == 1)
            return SingleGroup(group[0]);

        return group
            .Select((entry, index) => EnumerateValues(entry, index))
            .MergeSorted(ValueIndexComparer)
            .Select(pair => (pair.Value, group[pair.Index].Text));

        static IEnumerable<(T Value, ArraySegment<byte> Text)> SingleGroup(ReadChunkEntry<T> entry)
        {
            var text = entry.Text;
            var values = entry.ValuesArray;
            for (var i = 0; i < entry.ValuesCount; i++)
                yield return (values[entry.ValuesStart + i], text);
        }

        static IEnumerable<(T Value, int Index)> EnumerateValues(ReadChunkEntry<T> entry, int index)
        {
            for (var i = 0; i < entry.ValuesCount; i++)
                yield return (entry.ValuesArray[entry.ValuesStart + i], index);
        }
    }
}
