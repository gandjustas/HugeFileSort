using System.Buffers;
using System.Diagnostics;
using System.Globalization;
using System.IO.Pipelines;
using System.Reflection;
using System.Text;
using DotMake.CommandLine;

Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);

return await Cli.RunAsync<RootCliCommand>(args);

[CliCommand(Description = "Huge file sorter")]
internal partial class RootCliCommand
{
    [CliOption(Name = "encoding", Recursive = true, Description = "Text encoding of the file(s) (default: utf-8)")]
    public string EncodingName { get; set; } = "utf-8";

    [CliCommand(Description = "Sort huge file: split into sorted chunks, then merge")]
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

        [CliOption(Name = "max-chunk-mb", Description = "Upper bound of the adaptive chunk buffer, MB (default: 100)")]
        public int MaxChunkMb { get; set; } = 100;

        [CliOption(Name = "flush-mb", Description = "Unflushed bytes threshold for the output writer, MB (default: 1)")]
        public int FlushMb { get; set; } = 1;

        [CliOption(Name = "zstd-level", Description = "Zstd compression level for chunk files (default: 9)")]
        public int ZstdLevel { get; set; } = 9;

        public Task<int> RunAsync(CliContext context) =>
            RootCliCommand.RunWithSettingsAsync(CompareOptionsName, (encoding, compareOptions, ct) => SortAsync(encoding, compareOptions, ct), context);

        private async Task<int> SortAsync(Encoding encoding, CompareOptions compareOptions, CancellationToken ct)
        {
            var sw = Stopwatch.StartNew();
            var chunks = await new SplitSort<long>(Source, encoding, compareOptions, Parallelism,
                MinChunkMb, MaxChunkMb, FlushMb, ZstdLevel).RunAsync(ct);
            foreach (var file in chunks)
                Console.WriteLine(file.FullName);
            Console.WriteLine($"Chunks: {chunks.Count}");
            Console.WriteLine(sw.Elapsed);

            var destination = Destination ?? new FileInfo(Path.ChangeExtension(Source.FullName, ".sorted" + Path.GetExtension(Source.FullName)));
            await new Merge<long>(destination, encoding, FlushMb).RunAsync(chunks, ct);

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
    internal partial class SplitCliCommand
    {
        [CliArgument(Description = "Source file to split", ValidationRules = CliValidationRules.ExistingFile)]
        public FileInfo Source { get; set; } = null!;

        [CliOption(Name = "parallel", Description = "Number of chunks sorted in parallel (default: 1)")]
        public int Parallelism { get; set; } = 1;

        [CliOption(Name = "compare-options", Description = "Text comparison: None (default), Ordinal, IgnoreCase, IgnoreNonSpace, IgnoreSymbols, or a combination")]
        public string CompareOptionsName { get; set; } = "None";

        [CliOption(Name = "min-chunk-mb", Description = "Lower bound of the adaptive chunk buffer, MB (default: 32)")]
        public int MinChunkMb { get; set; } = 32;

        [CliOption(Name = "max-chunk-mb", Description = "Upper bound of the adaptive chunk buffer, MB (default: 100)")]
        public int MaxChunkMb { get; set; } = 100;

        [CliOption(Name = "flush-mb", Description = "Unflushed bytes threshold for the chunk writer, MB (default: 1)")]
        public int FlushMb { get; set; } = 1;

        [CliOption(Name = "zstd-level", Description = "Zstd compression level for chunk files (default: 9)")]
        public int ZstdLevel { get; set; } = 9;

        public Task<int> RunAsync(CliContext context) =>
            RootCliCommand.RunWithSettingsAsync(CompareOptionsName, async (encoding, compareOptions, ct) =>
            {
                var files = await new SplitSort<long>(Source, encoding, compareOptions, Parallelism,
                    MinChunkMb, MaxChunkMb, FlushMb, ZstdLevel).RunAsync(ct);
                foreach (var file in files)
                    Console.WriteLine(file.FullName);
                return 0;
            }, context);
    }

    [CliCommand(Description = "Merge sorted chunk files listed on stdin into one file")]
    internal partial class MergeCliCommand
    {
        [CliArgument(Description = "Destination file (will be overwritten)",
            ValidationRules = CliValidationRules.LegalPath)]
        public FileInfo Destination { get; set; } = null!;

        [CliOption(Name = "flush-mb", Description = "Unflushed bytes threshold for the output writer, MB (default: 1)")]
        public int FlushMb { get; set; } = 1;

        public Task<int> RunAsync(CliContext context) =>
            RootCliCommand.RunWithSettingsAsync("None", (encoding, _, ct) => MergeAsync(encoding, ct), context);

        private async Task<int> MergeAsync(Encoding encoding, CancellationToken ct)
        {
            List<FileInfo> chunks = [];
            while (Console.ReadLine() is { } line)
            {
                line = line.Trim();
                if (line.Length != 0)
                    chunks.Add(new FileInfo(line));
            }
            await new Merge<long>(Destination, encoding, FlushMb).RunAsync(chunks, ct);
            return 0;
        }
    }

    [CliCommand(Description = "Check file is sorted")]
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
            var comparison = compareOptions == CompareOptions.Ordinal
                ? StringComparison.Ordinal
                : StringComparison.CurrentCulture;
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

    [CliCommand(Description = "Diagnostic: run SplitToChunks only")]
    internal partial class ProbeSplitCommand
    {
        [CliArgument(Description = "Source file", ValidationRules = CliValidationRules.ExistingFile)]
        public FileInfo Source { get; set; } = null!;

        public async Task<int> RunAsync(CliContext context)
        {
            var encoding = Encoding.UTF8;
            var ct = context.CancellationToken;
            var sort = new SplitSort<long>(Source, encoding, CompareOptions.Ordinal, 1, 32, 100, 1, 9);
            var newLine = encoding.GetBytes(Environment.NewLine);
            var reader = PipeReader.Create(Source.OpenRead());
            var b = BindingFlags.NonPublic | BindingFlags.Instance;
            var t = typeof(SplitSort<long>);
            var splitToChunks = t.GetMethod("SplitToChunks", b)!.Invoke(sort,
                [reader.Split(newLine, ct), 32 * 1024 * 1024, ct])!;
            var sw = Stopwatch.StartNew();
            var n = 0;
            var totalKeys = 0;
            var usedProperty = typeof(SplitSort<long>).Assembly.GetType("Chunk`1")!
                .MakeGenericType(typeof(long)).GetProperty("Used")!;
            await foreach (var chunk in (IAsyncEnumerable<object?>)splitToChunks)
            {
                n++;
                var used = (int)usedProperty.GetValue(chunk)!;
                totalKeys += used;
                Console.WriteLine($"chunk {n}: used={used / 1048576}MB t={sw.Elapsed.TotalSeconds:F1}s");
            }
            Console.WriteLine($"DONE chunks={n} keyBytes={totalKeys / 1048576}MB t={sw.Elapsed.TotalSeconds:F1}s");
            return 0;
        }
    }

    [CliCommand(Description = "Generate huge file")]
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

    /// <summary>
    /// Разбирает общие настройки (кодировка — рекурсивная опция с root-команды, опции сравнения — из подкоманды)
    /// и запускает действие; при невалидных значениях печатает ошибку и возвращает 1.
    /// </summary>
    internal static Task<int> RunWithSettingsAsync(string compareOptionsName, Func<Encoding, CompareOptions, CancellationToken, Task<int>> run, CliContext context)
    {
        // Рекурсивная опция объявлена на RootCliCommand; значение достаём из дерева разбора —
        // оно совпадает в любом месте командной строки (root или подкоманда).
        var encodingName = context.Result.ParseResult.GetValue<string>("--encoding") ?? "utf-8";
        if (!TryGetEncoding(encodingName, out var encoding))
        {
            Console.Error.WriteLine($"Unknown encoding: '{encodingName}'");
            return Task.FromResult(1);
        }

        if (!Enum.TryParse(compareOptionsName, true, out CompareOptions compareOptions))
        {
            Console.Error.WriteLine($"Invalid compare options: '{compareOptionsName}'");
            return Task.FromResult(1);
        }

        return run(encoding, compareOptions, context.CancellationToken);
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

/// <summary>Устанавливает результат после завершения операции, выдающей список файлов.</summary>
internal static class FileListExtensions
{
    public static async Task<int> WithFilesAsync(this Task<List<FileInfo>> filesTask, Func<List<FileInfo>, int> done)
    {
        var files = await filesTask.ConfigureAwait(false);
        return done(files);
    }
}
