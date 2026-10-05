using DotMake.CommandLine;
using System.Diagnostics;
using System.Globalization;
using System.Text;
using System.Threading.Channels;

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

    [CliOption(Name = "zstd-level", Description = "Zstd compression level for chunk files, 1-22 (default: 9)")]
    public int ZstdLevel { get; set; } = 9;

    public Task<int> RunAsync(CliContext context) =>
        RootCliCommand.RunWithSettingsAsync(CompareOptionsName, SortAsync, context);

    private async Task<int> SortAsync(Encoding encoding, CompareOptions compareOptions, CancellationToken ct)
    {
        Stopwatch sw = Stopwatch.StartNew();

        // Параметры split-команды — из опций sort.
        SplitCliCommand split = new()
        {
            Source = Source,
            Parallelism = Parallelism,
            CompareOptionsName = CompareOptionsName,
            MinChunkMb = MinChunkMb,
            MaxChunkMb = MaxChunkMb,
            ZstdLevel = ZstdLevel,
        };
        var files = Channel.CreateUnbounded<string>();
        await split.SplitAsync(encoding, compareOptions, files.Writer, ct);
        Console.WriteLine($"Split: {sw.Elapsed}");

        // Параметры merge-команды: destination (или имя по умолчанию), порог сброса,
        // режим сравнения и преаллокация итогового файла по размеру исходника.
        MergeCliCommand merge = new()
        {
            Destination = Destination ?? new FileInfo(Path.ChangeExtension(Source.FullName, ".sorted" + Path.GetExtension(Source.FullName))),
            CompareOptionsName = CompareOptionsName,
            PreallocatedLength = Source.Length,
        };
        // Пути чанков уходят в merge прямо из канала split'а; список возвращает merge.
        var chunks = await merge.MergeAsync(encoding, compareOptions, files.Reader.ReadAllAsync(ct), ct);
        Console.WriteLine($"Chunks: {chunks.Count}");

        foreach (var file in chunks)
        {
            try
            {
                File.Delete(file);
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"Delete failed: {file}: {ex.Message}");
            }
        }
        return 0;
    }
}
