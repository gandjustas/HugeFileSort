using System.Diagnostics;
using System.Globalization;
using System.IO.Pipelines;
using System.Text;
using DotMake.CommandLine;

[CliCommand(Parent = typeof(RootCliCommand), Description = "Diagnostic: run SplitToChunks only")]
internal partial class ProbeSplitCommand
{
    [CliArgument(Description = "Source file", ValidationRules = CliValidationRules.ExistingFile)]
    public FileInfo Source { get; set; } = null!;

    public async Task<int> RunAsync(CliContext context)
    {
        var encoding = Encoding.UTF8;
        var ct = context.CancellationToken;
        var split = new SplitCliCommand { Source = Source };
        split.Initialize(encoding, CompareOptions.Ordinal);
        var newLine = encoding.GetBytes(Environment.NewLine);
        var reader = PipeReader.Create(Source.OpenRead());
        var sw = Stopwatch.StartNew();
        var n = 0;
        long totalKeys = 0;
        await foreach (var chunk in split.SplitToChunks(reader.Split(newLine, ct), 32 * 1024 * 1024, ct))
        {
            n++;
            var used = chunk.Used;
            totalKeys += used;
            Console.WriteLine($"chunk {n}: used={used / 1048576}MB t={sw.Elapsed.TotalSeconds:F1}s");
            chunk.Dispose();
        }
        Console.WriteLine($"DONE chunks={n} keyBytes={totalKeys / 1048576}MB t={sw.Elapsed.TotalSeconds:F1}s");
        return 0;
    }
}
