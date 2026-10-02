using System.IO.Pipelines;
using System.Text;
using DotMake.CommandLine;

[CliCommand(Parent = typeof(RootCliCommand), Description = "Generate huge file")]
internal partial class GenerateCliCommand
{
    [CliArgument(Description = "Destination file (will be overwritten)",
        ValidationRules = CliValidationRules.LegalPath)]
    public FileInfo Destination { get; set; } = null!;

    [CliArgument(Description = "Minimum size of file in MB", Required = true)]
    public int Size { get; set; }

    public Task<int> RunAsync(CliContext context) =>
        RootCliCommand.RunWithSettingsAsync("None", (encoding, _, ct) => GenerateAsync(encoding, ct), context);

    private async Task<int> GenerateAsync(Encoding encoding, CancellationToken ct)
    {
        var newLine = encoding.GetBytes(Environment.NewLine);
        var random = Random.Shared;
        var textBuffer = new char[1024];
        var numberBuffer = new char[16];
        long written = 0;

        PipeWriter writer = PipeWriter.Create(Destination.Create());
        while (written < Size * 1024L * 1024L)
        {
            random.Next().TryFormat(numberBuffer, out var digits, "d");
            encoding.GetBytes(numberBuffer.AsSpan(0, digits), writer);
            encoding.GetBytes(". ", writer);
            encoding.GetBytes(RandomString(random, random.Next(textBuffer.Length), textBuffer), writer);
            writer.Write(newLine);
            if (writer.UnflushedBytes >= 8 << 20)
            {
                written += writer.UnflushedBytes;
                await writer.FlushAsync(ct);
            }
        }

        await writer.CompleteAsync();
        return 0;
    }

    private static ReadOnlySpan<char> RandomString(Random random, int length, Span<char> span)
    {
        const string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        span = span[..length];
        for (var i = 0; i < length; i++)
            span[i] = chars[random.Next(chars.Length)];
        return span;
    }
}
