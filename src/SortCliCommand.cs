using System.Diagnostics;
using System.Globalization;
using System.Text;
using DotMake.CommandLine;

/// <summary>
/// Команда sort: разбивает исходник на отсортированные чанки командой split и сливает их
/// командой merge, передавая им свои опции.
/// </summary>
[CliCommand(Parent = typeof(RootCliCommand), Description = "Sort huge file: split into sorted chunks, then merge")]
internal partial class SortCliCommand
{
    [CliArgument(Description = "Source file to sort", ValidationRules = CliValidationRules.ExistingFile)]
    public FileInfo Source { get; set; } = null!;

    [CliArgument(Description = "Destination file (will be overwritten)",
        Arity = CliArgumentArity.ZeroOrOne, ValidationRules = CliValidationRules.LegalPath)]
    public FileInfo? Destination { get; set; }

    [CliOption(Name = "parallel", Description = "Number of chunks sorted in parallel (default: 1)")]
    public int Parallelism { get; set; } = 1;

    [CliOption(Name = "compare-options", Description = "Text comparison: None (default), Ordinal, IgnoreCase, IgnoreNonSpace, IgnoreSymbols, or a combination")]
    public string CompareOptionsName { get; set; } = "None";

    [CliOption(Name = "min-chunk-mb", Description = "Lower bound of the adaptive chunk buffer, MB (default: 32)")]
    public int MinChunkMb { get; set; } = 32;

    [CliOption(Name = "max-chunk-mb", Description = "Upper bound of the adaptive chunk buffer, MB (default: 500)")]
    public int MaxChunkMb { get; set; } = 500;

    [CliOption(Name = "flush-mb", Description = "Unflushed bytes threshold for the output writer, MB (default: 32)")]
    public int FlushMb { get; set; } = 32;

    [CliOption(Name = "zstd-level", Description = "Zstd compression level for chunk files, 1-22 (default: 9)")]
    public int ZstdLevel { get; set; } = 9;

    public Task<int> RunAsync(CliContext context) =>
        RootCliCommand.RunWithSettingsAsync(CompareOptionsName, SortAsync, context);

    private async Task<int> SortAsync(Encoding encoding, CompareOptions compareOptions, CancellationToken ct)
    {
        var sw = Stopwatch.StartNew();

        // Параметры split-команды — из опций sort.
        var split = new SplitCliCommand
        {
            Source = Source,
            Parallelism = Parallelism,
            CompareOptionsName = CompareOptionsName,
            MinChunkMb = MinChunkMb,
            MaxChunkMb = MaxChunkMb,
            ZstdLevel = ZstdLevel,
        };
        var chunks = await split.SplitAsync(encoding, compareOptions, ct);
        foreach (var file in chunks)
            Console.WriteLine(file.FullName);
        Console.WriteLine($"Chunks: {chunks.Count}");
        Console.WriteLine(sw.Elapsed);

        // Параметры merge-команды: destination (или имя по умолчанию), порог сброса,
        // режим сравнения и преаллокация итогового файла по размеру исходника.
        var merge = new MergeCliCommand
        {
            Destination = Destination ?? new FileInfo(Path.ChangeExtension(Source.FullName, ".sorted" + Path.GetExtension(Source.FullName))),
            FlushMb = FlushMb,
            CompareOptionsName = CompareOptionsName,
            PreallocatedLength = Source.Length,
        };
        await merge.MergeAsync(encoding, compareOptions, chunks, ct);

        foreach (var file in chunks)
        {
            try
            {
                file.Delete();
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"Delete failed: {file.FullName}: {ex.Message}");
            }
        }
        return 0;
    }
}
