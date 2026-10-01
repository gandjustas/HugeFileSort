using System.Buffers;
using System.Globalization;
using System.IO.Pipelines;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Channels;
using ZstdSharp;

/// <summary>
/// Фаза слияния: мержит отсортированные промежуточные файлы в один итоговый.
/// Sort key уже вычислены при split и сохранены в чанках — слияние только сравнивает их как байты.
/// </summary>
class Merge<T>(FileInfo destination, Encoding encoding, int flushMb, CompareOptions compareOptions, int prefetch, long? preallocatedLength = null) where T : IBinaryInteger<T>
{
    readonly FileInfo destination = destination;
    readonly Encoding encoding = encoding;
    readonly byte[] newLine = encoding.GetBytes(Environment.NewLine);
    readonly byte[] dotSpace = encoding.GetBytes(". ");
    readonly int flushThreshold = flushMb * 1024 * 1024;
    readonly CompareInfo compareInfo = CultureInfo.CurrentCulture.CompareInfo;
    readonly CompareOptions compareOptions = compareOptions;
    readonly int prefetch = prefetch;
    readonly long? preallocatedLength = preallocatedLength;
    // Буфер файлового потока записи — чуть меньше порога сброса PipeWriter.
    readonly int writeStreamBufferSize = Math.Clamp(flushMb * 1024 * 1024 - 64 * 1024, 64 * 1024, 32 * 1024 * 1024);


    /// <summary>
    /// Мержит отсортированные чанки в destination: k потоков ReadChunkEntries сливаются через хип
    /// (MergeSorted), культурно равные записи (равные sort key) группируются и сливаются по значению.
    /// </summary>
    public async Task RunAsync(IReadOnlyList<FileInfo> chunkFiles, CancellationToken ct)
    {
        var sw = System.Diagnostics.Stopwatch.StartNew();
        await using var fileStream = new FileStream(destination.FullName, FileMode.Create, FileAccess.Write, FileShare.None,
            bufferSize: writeStreamBufferSize, FileOptions.SequentialScan);
        if (preallocatedLength is { } size)
            fileStream.SetLength(size);
        fileStream.Seek(0, SeekOrigin.Begin);
        var writer = PipeWriter.Create(fileStream, new StreamPipeWriterOptions(leaveOpen: true));

        // Подготовщики: по задаче на чанк-файл (декомпрессия, разбор, вычисление sort key),
        // записи — через bounded-каналы; merge забирает готовые ключи.
        var channels = new Channel<ReadChunkEntry<T>>[chunkFiles.Count];
        var prepareTasks = new Task[chunkFiles.Count];
        for (var i = 0; i < chunkFiles.Count; i++)
        {
            var file = chunkFiles[i];
            var channel = Channel.CreateBounded<ReadChunkEntry<T>>(new BoundedChannelOptions(128)
            {
                SingleWriter = true,
                SingleReader = true,
            });
            channels[i] = channel;
            prepareTasks[i] = Task.Run(async () =>
            {
                try
                {
                    await using var stream = new DecompressionStream(new FileStream(file.FullName, FileMode.Open, FileAccess.Read, FileShare.Read, bufferSize: 4 << 20, FileOptions.SequentialScan | FileOptions.Asynchronous), leaveOpen: false);
                    await foreach (var entry in stream.ReadChunkEntries<T>(encoding, compareInfo, compareOptions, ct)
                                       .ConfigureAwait(false))
                    {
                        await channel.Writer.WriteAsync(entry, ct).ConfigureAwait(false);
                    }
                    channel.Writer.Complete();
                }
                catch (Exception ex)
                {
                    channel.Writer.Complete(ex);
                    throw;
                }
            }, ct);
        }

        StartPrefetch(chunkFiles, ct);

        var sources = channels.Select(c => c.Reader.ReadAllAsync(ct));
        var number = new char[32];
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
        await writer.CompleteAsync().ConfigureAwait(false);
        if (preallocatedLength is not null)
            fileStream.SetLength(fileStream.Position);
        Console.WriteLine($"Merged: {destination.FullName}");
        Console.WriteLine(sw.Elapsed);
    }

    /// <summary>Прогрев кэша: до N чанк-файлов читаются фоновыми задачами целиком (SequentialScan).
    /// Помогает на SSD; на HDD параллельные чтения устраивают шторм поисков — по умолчанию выключен.</summary>
    private void StartPrefetch(IReadOnlyList<FileInfo> chunkFiles, CancellationToken ct)
    {
        if (prefetch <= 0)
            return;
        _ = Task.Run(async () =>
        {
            try
            {
                using var gate = new SemaphoreSlim(prefetch);
                var tasks = chunkFiles.Select(async file =>
                {
                    await gate.WaitAsync(ct).ConfigureAwait(false);
                    try
                    {
                        using var fs = new FileStream(file.FullName, FileMode.Open, FileAccess.Read,
                            FileShare.Read | FileShare.Delete, 1 << 20,
                            FileOptions.SequentialScan | FileOptions.Asynchronous);
                        var buf = ArrayPool<byte>.Shared.Rent(1 << 20);
                        try
                        {
                            while (await fs.ReadAsync(buf.AsMemory(0, buf.Length), ct).ConfigureAwait(false) > 0)
                            {
                            }
                        }
                        finally
                        {
                            ArrayPool<byte>.Shared.Return(buf);
                        }
                    }
                    catch (IOException)
                    {
                        // Прогрев не обязателен — ошибки чтения игнорируем
                    }
                    finally
                    {
                        gate.Release();
                    }
                });
                await Task.WhenAll(tasks).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
            }
        }, ct);
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
            .Select((entry, index) => entry.Values.Select(value => (Value: value, Index: index)))
            .MergeSorted(ValueIndexComparer)
            .Select(pair => (pair.Value, group[pair.Index].Text));

        static IEnumerable<(T Value, ArraySegment<byte> Text)> SingleGroup(ReadChunkEntry<T> entry)
        {
            var text = entry.Text;
            foreach (var value in entry.Values)
                yield return (value, text);
        }
    }
}
