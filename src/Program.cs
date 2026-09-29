using System.Buffers;
using System.Diagnostics;
using System.Globalization;
using System.IO.Pipelines;
using System.Text;
using DotMake.CommandLine;

Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);

return await Cli.RunAsync<RootCliCommand>(args);

[CliCommand(Description = "Huge file sorter")]
internal partial class RootCliCommand
{
    [CliCommand(Description = "Sort huge file: split into sorted chunks, then merge")]
    internal partial class SortCliCommand : HugeFileCommand
    {
        [CliArgument(Description = "Source file to sort", ValidationRules = CliValidationRules.ExistingFile)]
        public FileInfo Source { get; set; } = null!;

        [CliArgument(Description = "Destination file (will be overwritten)",
            Arity = CliArgumentArity.ZeroOrOne, ValidationRules = CliValidationRules.LegalPath)]
        public FileInfo? Destination { get; set; }

        [CliOption(Name = "parallel", Description = "Number of chunks sorted in parallel (default: 1)")]
        public int Parallelism { get; set; } = 1;

        [CliOption(Name = "ordinal", Description = "Ordinal byte-wise comparison: skips sort key computation and sorts text bytes directly")]
        public bool Ordinal { get; set; }

        public Task<int> RunAsync(CliContext context) =>
            RunWithEncodingAsync(SortAsync, context);

        private async Task<int> SortAsync(Encoding encoding, CancellationToken ct)
        {
            var sw = Stopwatch.StartNew();
            var compareOptions = Ordinal ? CompareOptions.Ordinal : CompareOptions.None;
            var chunks = await new SplitSort<long>(Source, encoding, compareOptions, Parallelism).RunAsync(ct);
            foreach (var file in chunks)
                Console.WriteLine(file.FullName);
            Console.WriteLine($"Chunks: {chunks.Count}");
            Console.WriteLine(sw.Elapsed);

            var destination = Destination ?? new FileInfo(Path.ChangeExtension(Source.FullName, ".sorted" + Path.GetExtension(Source.FullName)));
            await new Merge<long>(destination, encoding).RunAsync(chunks, ct);

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

    [CliCommand(Description = "Split a huge file into sorted chunk files, printing their paths to stdout")]
    internal partial class SplitCliCommand : HugeFileCommand
    {
        [CliArgument(Description = "Source file to split", ValidationRules = CliValidationRules.ExistingFile)]
        public FileInfo Source { get; set; } = null!;

        [CliOption(Name = "parallel", Description = "Number of chunks sorted in parallel (default: 1)")]
        public int Parallelism { get; set; } = 1;

        [CliOption(Name = "ordinal", Description = "Ordinal byte-wise comparison: skips sort key computation and sorts text bytes directly")]
        public bool Ordinal { get; set; }

        public Task<int> RunAsync(CliContext context)
        {
            var compareOptions = Ordinal ? CompareOptions.Ordinal : CompareOptions.None;
            return RunWithEncodingAsync(async (encoding, ct) =>
            {
                var chunks = await new SplitSort<long>(Source, encoding, compareOptions, Parallelism).RunAsync(ct);
                foreach (var file in chunks)
                    Console.WriteLine(file.FullName);
                return 0;
            }, context);
        }
    }

    [CliCommand(Description = "Merge sorted chunk files listed on stdin into one file")]
    internal partial class MergeCliCommand : HugeFileCommand
    {
        [CliArgument(Description = "Destination file (will be overwritten)",
            ValidationRules = CliValidationRules.LegalPath)]
        public FileInfo Destination { get; set; } = null!;

        public Task<int> RunAsync(CliContext context) =>
            RunWithEncodingAsync(MergeAsync, context);

        private async Task<int> MergeAsync(Encoding encoding, CancellationToken ct)
        {
            List<FileInfo> chunks = [];
            while (Console.ReadLine() is { } line)
            {
                line = line.Trim();
                if (line.Length != 0)
                    chunks.Add(new FileInfo(line));
            }
            await new Merge<long>(Destination, encoding).RunAsync(chunks, ct);
            return 0;
        }
    }

    [CliCommand(Description = "Check file is sorted")]
    internal partial class CheckSortedCliCommand : HugeFileCommand
    {
        [CliArgument(Description = "Source file to sort or check", ValidationRules = CliValidationRules.ExistingFile)]
        public FileInfo Source { get; set; } = null!;

        public Task<int> RunAsync(CliContext context) =>
            RunWithEncodingAsync(CheckAsync, context);

        private async Task<int> CheckAsync(Encoding encoding, CancellationToken ct)
        {
            var newLine = encoding.GetBytes(Environment.NewLine);
            var dotSpace = encoding.GetBytes(". ");
            var reader = PipeReader.Create(Source.OpenRead());

            (string? s, long x) last = (null, 0L);
            var sorted = true;
            await reader.ProcessLines(newLine, line =>
            {
                var dotPos = line.IndexOf(dotSpace);
                if (dotPos < 0)
                    throw new FormatException("Line does not contain '. ' delimiter");
                var x = DecodeNumber(line[..dotPos], encoding);
                var s = encoding.GetString(line[(dotPos + dotSpace.Length)..]);
                var cmp = s.CompareTo(last.s);
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

    [CliCommand(Description = "Generate huge file")]
    internal partial class GenerateCliCommand : HugeFileCommand
    {
        [CliArgument(Description = "Destination file (will be overwritten)",
            ValidationRules = CliValidationRules.LegalPath)]
        public FileInfo Destination { get; set; } = null!;

        [CliArgument(Description = "Minimum size of file in MB", Required = true)]
        public int Size { get; set; }

        public Task<int> RunAsync(CliContext context) =>
            RunWithEncodingAsync(GenerateAsync, context);

        private async Task<int> GenerateAsync(Encoding encoding, CancellationToken ct)
        {
            var newLine = encoding.GetBytes(Environment.NewLine);
            var random = Random.Shared;
            var textBuffer = new char[1024];
            var numberBuffer = new char[16];
            long written = 0;

            var writer = PipeWriter.Create(Destination.Create());
            while (written < Size * 1024L * 1024L)
            {
                random.Next().TryFormat(numberBuffer, out var digits, "d");
                encoding.GetBytes(numberBuffer.AsSpan(0, digits), writer);
                encoding.GetBytes(". ", writer);
                encoding.GetBytes(RandomString(random, random.Next(textBuffer.Length), textBuffer), writer);
                writer.Write(newLine);
                if (writer.UnflushedBytes > Size)
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
}

/// <summary>База команд, работающих с текстовыми файлами: общая опция кодировки и её разбор.</summary>
internal abstract class HugeFileCommand
{
    [CliOption(Name = "encoding", Description = "Text encoding of the file (default: utf-8)")]
    public string EncodingName { get; set; } = "utf-8";

    protected Task<int> RunWithEncodingAsync(Func<Encoding, CancellationToken, Task<int>> run, CliContext context) =>
        RunWithEncodingAsync(run, context.CancellationToken);

    private async Task<int> RunWithEncodingAsync(Func<Encoding, CancellationToken, Task<int>> run, CancellationToken ct)
    {
        if (!TryGetEncoding(EncodingName, out var encoding))
        {
            Console.Error.WriteLine($"Unknown encoding: '{EncodingName}'");
            return 1;
        }
        return await run(encoding, ct).ConfigureAwait(false);
    }

    private static bool TryGetEncoding(string name, out Encoding encoding)
    {
        try
        {
            encoding = Encoding.GetEncoding(name);
            return true;
        }
        catch (ArgumentException)
        {
            encoding = null!;
            return false;
        }
    }
}
