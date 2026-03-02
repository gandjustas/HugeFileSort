using System.Buffers;
using System.Diagnostics;
using System.IO.Pipelines;
using System.Numerics;
using System.Text;

class Sorter<V> where V: INumberBase<V>
{
    readonly FileInfo source;
    readonly FileInfo destination;
    readonly Encoding encoding;
    private readonly byte[] newLine;
    // private readonly ArraySegmentComparer<byte> sortKeysComparer = new();
    // private readonly CompareInfo compareInfo = CultureInfo.CurrentCulture.CompareInfo;
    // readonly List<FileInfo> chunks = [];
    // readonly ArrayPool<byte> arrayPool = ArrayPool<byte>.Shared;
    // readonly record struct Splitted(byte[] Array, IReadOnlyDictionary<ArraySegment<byte>, List<long>> Lookup);
    // readonly record struct Sorted(byte[] Array, List<(ArraySegment<byte>, List<long>)> Lookup);

    public Sorter(FileInfo source, FileInfo? destination, Encoding encoding)
    {
        this.source = source;
        if (destination is null)
        {
            var file = source.FullName;
            destination = new FileInfo(Path.ChangeExtension(file, ".sorted" + Path.GetExtension(file)));
        }
        this.destination = destination;
        this.encoding = encoding;
        newLine = encoding.GetBytes(Environment.NewLine);

    }

    public async Task Sort(CancellationToken ct)
    {
        var sw = Stopwatch.StartNew();

        StringToUtf8BytesComparer comparer = new(encoding, Random.Shared.Next());
        Dictionary<string, List<V>> dict = new(comparer);
        var l = dict.GetAlternateLookup<ReadOnlySpan<byte>>();

        var reader = PipeReader.Create(source.OpenRead());
        while (true)
        {
            var result = await reader.ReadAsync(ct).ConfigureAwait(false);
            var buffer = result.Buffer;
            SequenceReader<byte> sequenceReader = new(buffer);
            while (!sequenceReader.End)
            {
                while (sequenceReader.TryReadTo(out ReadOnlySpan<byte> line, newLine))
                {
                    var dotPos = line.IndexOf((byte)'.');
                    var x = V.Parse(line[..dotPos], null);
                    line = line[(dotPos + 2)..];
                    l.AddItemToLookup(line, x);
                }

                buffer = buffer.Slice(sequenceReader.Position);
                sequenceReader.Advance(buffer.Length);
            }

            reader.AdvanceTo(buffer.Start, buffer.End);
            if (result.IsCompleted) break;
        }

        var sorted = dict
            .AsParallel()
            .Select(x => { x.Value.Sort(); return (x.Key, x.Value); })
            .OrderBy(p => p.Key);

        var writer = PipeWriter.Create(destination.OpenWrite());
        foreach (var (line, values) in sorted)
        {
            foreach (var value in values)
            {
                writer.Write(value, "d");
                writer.Write(". ", encoding);
                writer.Write(line, encoding);
                writer.Write(newLine);
            }
            await writer.FlushAsync(ct);
        }
        await reader.CompleteAsync();
        await writer.CompleteAsync();

        Console.WriteLine(sw.Elapsed);
    }
}

