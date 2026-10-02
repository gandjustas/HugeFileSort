using System.Globalization;
using System.IO.Pipelines;
using System.Text;
using DotMake.CommandLine;

[CliCommand(Parent = typeof(RootCliCommand), Description = "Check file is sorted")]
internal partial class CheckSortedCliCommand
{
    [CliArgument(Description = "Source file to sort or check", ValidationRules = CliValidationRules.ExistingFile)]
    public FileInfo Source { get; set; } = null!;

    [CliOption(Name = "compare-options", Description = "Text comparison: None (default), Ordinal, IgnoreCase, IgnoreNonSpace, IgnoreSymbols, or a combination")]
    public string CompareOptionsName { get; set; } = "None";

    public Task<int> RunAsync(CliContext context) =>
        RootCliCommand.RunWithSettingsAsync(CompareOptionsName, CheckAsync, context);

    private async Task<int> CheckAsync(Encoding encoding, CompareOptions compareOptions, CancellationToken ct)
    {
        var compareInfo = CultureInfo.CurrentCulture.CompareInfo;
        var newLine = encoding.GetBytes(Environment.NewLine);
        var dotSpace = encoding.GetBytes(". ");
        PipeReader reader = PipeReader.Create(Source.OpenRead());

        (string? s, long x) last = (null, 0L);
        var sorted = true;
        await reader.ProcessLines(newLine, line =>
        {
            var dotPos = line.IndexOf(dotSpace);
            if (dotPos < 0)
                throw new FormatException("Line does not contain '. ' delimiter");
            var x = DecodeNumber(line[..dotPos], encoding);
            var s = encoding.GetString(line[(dotPos + dotSpace.Length)..]);
            var cmp = compareOptions == CompareOptions.Ordinal
                ? string.Compare(s, last.s, StringComparison.Ordinal)
                : compareInfo.Compare(s, last.s, compareOptions);
            sorted = sorted && cmp >= 0 && (cmp > 0 || x >= last.x);
            last = (s, x);
        }, ct);
        return sorted ? 0 : 1;
    }

    private static long DecodeNumber(ReadOnlySpan<byte> bytes, Encoding encoding)
    {
        if (bytes.Length > 128)
            throw new FormatException("Number is too long");
        Span<char> chars = stackalloc char[128];
        var written = encoding.GetChars(bytes, chars);
        return long.Parse(chars[..written], null);
    }
}
