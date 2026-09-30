using System.Buffers;
using System.Globalization;
using System.IO.Pipelines;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Text;
using ZstdSharp;

/// <summary>
/// Фаза слияния: мержит отсортированные промежуточные файлы в один итоговый.
/// Sort key уже вычислены при split и сохранены в чанках — слияние только сравнивает их как байты.
/// </summary>
class Merge<T>(FileInfo destination, Encoding encoding, int flushMb, long? preallocatedLength = null) where T : IBinaryInteger<T>
{
    readonly FileInfo destination = destination;
    readonly Encoding encoding = encoding;
    readonly byte[] newLine = encoding.GetBytes(Environment.NewLine);
    readonly byte[] dotSpace = encoding.GetBytes(". ");
    readonly int flushThreshold = flushMb * 1024 * 1024;
    readonly long? preallocatedLength = preallocatedLength;
    // Буфер файлового потока записи — чуть меньше порога сброса PipeWriter.
    readonly int writeStreamBufferSize = Math.Clamp(flushMb * 1024 * 1024 - 64 * 1024, 64 * 1024, 1024 * 1024);


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
        var sources = chunkFiles.Select(c => new DecompressionStream(c.OpenRead(), leaveOpen: false)
            .ReadChunkEntries<T>(ct));


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
