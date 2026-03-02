using System.Buffers;
using System.CommandLine;
using System.IO.Pipelines;

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
sort.SetAction((r, ct) => 
    new Sorter<long>(
        r.GetRequiredValue(source),
        r.GetValue(destination),
        System.Text.Encoding.UTF8)
    .Sort(ct)
    );

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
    await reader.ProcessLines(newLine, line =>
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
