using System.Buffers;
using System.IO.Pipelines;
using System.Text;
using DotMake.CommandLine;

Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);

return await Cli.RunAsync<RootCliCommand>(args);

[CliCommand(Description = "Huge file sorter")]
internal partial class RootCliCommand
{
    [CliCommand(Description = "Sort huge file: split into sorted chunks, then merge")]
    internal partial class SortCliCommand
    {
        [CliArgument(Description = "Source file to sort or check",
            ValidationRules = CliValidationRules.ExistingFile)]
        public FileInfo Source { get; set; } = null!;

        [CliArgument(Description = "Destination file (will be overwritten)",
            Arity = CliArgumentArity.ZeroOrOne,
            ValidationRules = CliValidationRules.LegalPath)]
        public FileInfo? Destination { get; set; }

        [CliOption(Name = "encoding", Description = "Text encoding of the file (default: utf-8)")]
        public string EncodingName { get; set; } = "utf-8";

        public async Task<int> RunAsync(CliContext context)
        {
            if (!CliEncodings.TryGet(EncodingName, out var encoding))
            {
                Console.Error.WriteLine($"Unknown encoding: '{EncodingName}'");
                return 1;
            }
            var ct = context.CancellationToken;
            var sw = System.Diagnostics.Stopwatch.StartNew();

            var chunks = await new SplitSort<long>(Source, encoding).RunAsync(ct);
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
                catch
                {
                    // Пропускаем ошибки, чтобы удалить как можно больше чанков
                }
            }
            return 0;
        }
    }

    [CliCommand(Description = "Split a huge file into sorted chunk files, printing their paths to stdout")]
    internal partial class SplitCliCommand
    {
        [CliArgument(Description = "Source file to split",
            ValidationRules = CliValidationRules.ExistingFile)]
        public FileInfo Source { get; set; } = null!;

        [CliOption(Name = "encoding", Description = "Text encoding of the file (default: utf-8)")]
        public string EncodingName { get; set; } = "utf-8";

        public async Task<int> RunAsync(CliContext context)
        {
            if (!CliEncodings.TryGet(EncodingName, out var encoding))
            {
                Console.Error.WriteLine($"Unknown encoding: '{EncodingName}'");
                return 1;
            }
            var chunks = await new SplitSort<long>(Source, encoding).RunAsync(context.CancellationToken);
            foreach (var file in chunks)
                Console.WriteLine(file.FullName);
            return 0;
        }
    }

    [CliCommand(Description = "Merge sorted chunk files listed on stdin into one file")]
    internal partial class MergeCliCommand
    {
        [CliArgument(Description = "Destination file (will be overwritten)",
            ValidationRules = CliValidationRules.LegalPath)]
        public FileInfo Destination { get; set; } = null!;

        [CliOption(Name = "encoding", Description = "Text encoding of the files (default: utf-8)")]
        public string EncodingName { get; set; } = "utf-8";

        public async Task<int> RunAsync(CliContext context)
        {
            if (!CliEncodings.TryGet(EncodingName, out var encoding))
            {
                Console.Error.WriteLine($"Unknown encoding: '{EncodingName}'");
                return 1;
            }
            var chunks = new List<FileInfo>();
            string? line;
            while ((line = Console.ReadLine()) is not null)
            {
                line = line.Trim();
                if (line.Length == 0) continue;
                chunks.Add(new FileInfo(line));
            }
            await new Merge<long>(Destination, encoding).RunAsync(chunks, context.CancellationToken);
            return 0;
        }
    }

    [CliCommand(Description = "Check file is sorted")]
    internal partial class CheckSortedCliCommand
    {
        [CliArgument(Description = "Source file to sort or check",
            ValidationRules = CliValidationRules.ExistingFile)]
        public FileInfo Source { get; set; } = null!;

        [CliOption(Name = "encoding", Description = "Text encoding of the file (default: utf-8)")]
        public string EncodingName { get; set; } = "utf-8";

        public async Task<int> RunAsync(CliContext context)
        {
            if (!CliEncodings.TryGet(EncodingName, out var encoding))
            {
                Console.Error.WriteLine($"Unknown encoding: '{EncodingName}'");
                return 1;
            }
            return await Check(Source, encoding, context.CancellationToken);
        }

        private static async Task<int> Check(FileInfo source, Encoding encoding, CancellationToken ct)
        {
            var newLine = encoding.GetBytes(Environment.NewLine);
            var dotSpace = encoding.GetBytes(". ");
            var reader = PipeReader.Create(source.OpenRead());

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
    internal partial class GenerateCliCommand
    {
        [CliArgument(Description = "Destination file (will be overwritten)",
            ValidationRules = CliValidationRules.LegalPath)]
        public FileInfo Destination { get; set; } = null!;

        [CliArgument(Description = "Minimum size of file in MB", Required = true)]
        public int Size { get; set; }

        [CliOption(Name = "encoding", Description = "Text encoding of the file (default: utf-8)")]
        public string EncodingName { get; set; } = "utf-8";

        public async Task<int> RunAsync(CliContext context)
        {
            if (!CliEncodings.TryGet(EncodingName, out var encoding))
            {
                Console.Error.WriteLine($"Unknown encoding: '{EncodingName}'");
                return 1;
            }
            await Generate(Destination, encoding, Size, context.CancellationToken);
            return 0;
        }

        private static async Task Generate(FileInfo file, Encoding encoding, int size, CancellationToken ct)
        {
            var newLine = encoding.GetBytes(Environment.NewLine);
            var random = Random.Shared;
            var buffer = new char[1024];
            long written = 0;

            var writer = PipeWriter.Create(file.Create());
            var number = new char[16];
            while (written < size * 1024L * 1024L)
            {
                random.Next().TryFormat(number, out var digits, "d");
                encoding.GetBytes(number[..digits], writer);
                encoding.GetBytes(". ", writer);
                encoding.GetBytes(RandomString(random, random.Next(buffer.Length), buffer), writer);
                writer.Write(newLine);
                if (writer.UnflushedBytes > size)
                {
                    written += writer.UnflushedBytes;
                    await writer.FlushAsync(ct);
                }
            }

            await writer.CompleteAsync();
        }

        private static ReadOnlySpan<char> RandomString(Random random, int length, Span<char> span)
        {
            const string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
            for (int i = 0; i < length; i++)
            {
                span[i] = chars[random.Next(chars.Length)];
            }
            return span[..length];
        }
    }
}

/// <summary>Разрешает имя кодировки из CLI (CodePages-провайдер даёт windows-125x и прочие).</summary>
internal static class CliEncodings
{
    public static bool TryGet(string name, out Encoding encoding)
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
