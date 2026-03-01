using System.Buffers;
using System.CommandLine;
using System.Data;
using System.Diagnostics;
using System.IO.Pipelines;
using System.Numerics;
using System.Runtime.InteropServices;

Argument<FileInfo> source = new("source")
{
    Description = "Source file to sort or check",
};
Argument<FileInfo> destination = new("destination")
{
    Description = "Destination file (will be overwritten)",
    Arity = ArgumentArity.ZeroOrOne
};

Argument<int> size = new("size")
{
    Description = "Minimum size of file in MB",
};

Command sort = new("sort", "Sort huge file")
{
    Arguments = {
        ArgumentValidation.AcceptExistingOnly(source),
        ArgumentValidation.AcceptLegalFilePathsOnly(destination)
    },
};
sort.SetAction((r, ct) => Sort(
    r.GetRequiredValue(source),
    r.GetValue(destination),
    ct));

Command check = new("check-sorted", "Check file is sorted")
{
    Arguments = {
        ArgumentValidation.AcceptExistingOnly(source),
    },
};
check.SetAction((r, ct) => Check(
    r.GetRequiredValue(source),
    ct));


Command generate = new("generate", "Generate huge file")
{
    Arguments = {
        ArgumentValidation.AcceptLegalFilePathsOnly(destination),
        size
    },
};

generate.SetAction((r, ct) => Generate(
    r.GetRequiredValue(destination),
    r.GetRequiredValue(size),
    ct));


RootCommand rootCommand = [sort, check, generate];

var parseResult = rootCommand.Parse(args);
return await parseResult.InvokeAsync();

static async Task Generate(FileInfo file, int size, CancellationToken ct)
{
    var encoding = System.Text.Encoding.UTF8;
    var newLine = encoding.GetBytes(Environment.NewLine);
    var random = Random.Shared;
    var buffer = new char[1024];
    long written = 0;
    
    var writer = PipeWriter.Create(file.Create());
    while (written < size * 1024L* 1024L)
    {
        writer.Write(random.Next(), "d");        
        writer.Write(". ", encoding);        
        writer.Write(RandomString(random, random.Next(buffer.Length), buffer), encoding);
        writer.Write(newLine);
        if(writer.UnflushedBytes > size)
        {
            written += writer.UnflushedBytes;
            await writer.FlushAsync(ct);
        }
    }

    await writer.CompleteAsync();
}

static ReadOnlySpan<char> RandomString(Random random, int length, Span<char> span)
{
    const string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    for (int i = 0; i < length; i++)
    {
        span[i] = chars[random.Next(chars.Length)];
    }
    return span[..length];
}

static async Task<int> Check(FileInfo source, CancellationToken ct)
{
    var encoding = System.Text.Encoding.UTF8;
    var newLine = encoding.GetBytes(Environment.NewLine);
    var reader = PipeReader.Create(source.OpenRead());

    (string? s, long x) last = (null, 0L);
    var sorted = true;
    await ProcessLines(reader, newLine, line =>
    {
        var dotPos = line.IndexOf((byte)'.');
        var x = long.Parse(line[..dotPos], null);
        var s = encoding.GetString(line[(dotPos + 2)..]);
        var cmp = s.CompareTo(last.s);
        sorted = sorted && cmp >= 0 && (cmp > 0 || x >= last.x);
        last = (s, x);
    }, ct);
    return sorted ? 0 : 1;

}


static async Task Sort(FileInfo source, FileInfo? destination, CancellationToken ct)
{
    var sw = Stopwatch.StartNew();

    if (destination is null)
    {
        var file = source.FullName;
        destination = new FileInfo(Path.ChangeExtension(file, ".sorted" + Path.GetExtension(file)));
    }
    var reader = PipeReader.Create(source.OpenRead());

    var encoding = System.Text.Encoding.UTF8;
    var newLine = encoding.GetBytes(Environment.NewLine);
    StringToUtf8BytesComparer comparer = new(encoding, Random.Shared.Next());
    var dict = await ReadDictionary<long>(reader, newLine, comparer, ct);

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

static async Task<IReadOnlyDictionary<string, List<T>>> ReadDictionary<T>(PipeReader reader, byte[] newLine, IEqualityComparer<string> comparer, CancellationToken ct) where T : struct, INumberBase<T>
{
    Dictionary<string, List<T>> d = new(comparer);
    var l = d.GetAlternateLookup<ReadOnlySpan<byte>>();
    await ProcessLines(reader, newLine, line =>
    {
        var dotPos = line.IndexOf((byte)'.');
        var x = T.Parse(line[..dotPos], null);
        line = line[(dotPos + 2)..];

        ref var list = ref CollectionsMarshal.GetValueRefOrAddDefault(l, line, out bool exist);
        if (!exist)
        {
            list = [x];
        }
        else
        {
            list!.Add(x);
        }

    }, ct);

    return d;
}


static async Task ProcessLines(PipeReader reader, byte[] newLine, Action<ReadOnlySpan<byte>> processLine, CancellationToken ct)
{
    while (true)
    {
        var result = await reader.ReadAsync(ct);
        var buffer = result.Buffer;
        SequenceReader<byte> sequenceReader = new(buffer);
        while (!sequenceReader.End)
        {
            while (sequenceReader.TryReadTo(out ReadOnlySpan<byte> line, newLine))
            {
                processLine(line);
            }

            buffer = buffer.Slice(sequenceReader.Position);
            sequenceReader.Advance(buffer.Length);
        }

        reader.AdvanceTo(buffer.Start, buffer.End);
        if (result.IsCompleted) break;
    }
}