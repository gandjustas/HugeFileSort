using System.Buffers;
using System.CommandLine;
using System.Diagnostics;
using System.IO.Pipelines;

Argument<FileInfo> source = new("source")
{
    Description = "Source file to sort",
};
Argument<FileInfo> destination = new("destination")
{
    Description = "Destination file (will be overwritten)",
    Arity = ArgumentArity.ZeroOrOne
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


RootCommand rootCommand = [sort];

var parseResult = rootCommand.Parse(args);
if (parseResult.Errors.Count == 0)
{
    var sw = Stopwatch.StartNew();
    await parseResult.InvokeAsync();
    Console.WriteLine(sw.Elapsed);
}
else
{
    await parseResult.InvokeAsync();
}




static async Task Sort(FileInfo source, FileInfo? destination, CancellationToken ct)
{
    if (destination is null)
    {
        var file = source.FullName;
        destination = new FileInfo(Path.ChangeExtension(file, ".sorted" + Path.GetExtension(file)));
    }
    var reader = PipeReader.Create(source.OpenRead());
    var writer = PipeWriter.Create(destination.OpenWrite());

    await CopyLines(reader, writer, ct);
    await reader.CompleteAsync();
    await writer.CompleteAsync();
}

static async Task CopyLines(PipeReader reader, PipeWriter writer, CancellationToken ct)
{
    var encoding = System.Text.Encoding.UTF8;
    var newLine = encoding.GetBytes(Environment.NewLine);

    while (true)
    {
        var result = await reader.ReadAsync(ct);
        var buffer = result.Buffer;
        SequenceReader<byte> sequenceReader = new(buffer);
        while (!sequenceReader.End)
        {
            while (sequenceReader.TryReadTo(out ReadOnlySpan<byte> line, newLine))
            {
                writer.Write(line);
                writer.Write(newLine);
            }

            buffer = buffer.Slice(sequenceReader.Position);
            sequenceReader.Advance(buffer.Length);
        }

        reader.AdvanceTo(buffer.Start, buffer.End);
        await writer.FlushAsync(ct);
        if (result.IsCompleted) break;
    }
}