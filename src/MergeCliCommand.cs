using System.Buffers;
using System.Diagnostics;
using System.Globalization;
using System.IO.Compression;
using System.IO.Pipelines;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Channels;
using System.Threading.Tasks.Dataflow;
using DotMake.CommandLine;
using Microsoft.Extensions.ObjectPool;

/// <summary>
/// Команда merge и конвейер слияния. На каждый чанк — три сущности: пайп (сегменты 1МБ,
/// watermark 8/16МБ), продюсер, перекачивающий FileStream → zstd в пайп (ноль копий), и
/// парсер — обычная задача, разбирающая пайп в записи, материализующая их в арендованный
/// массив (текст, ключ, развёрнутые значения — ChunkEntry) и кладущая в ограниченный канал
/// на 16 записей. Сортировка ключей выполняется параллельно в задачах парсинга; поток мержа
/// только сравнивает ключи и собирает группы, группы уходят в ActionBlock, который пишет их
/// в выходной FileStream (буфер 4МБ) синхронно — запись не останавливает мерж.
/// </summary>
[CliCommand(Parent = typeof(RootCliCommand), Description = "Merge sorted chunk files listed on stdin into one file")]
internal partial class MergeCliCommand
{
    private const int MinimumSegmentSize = 1 << 20;
    // Водяные знаки пайпа чанка: продюсер опережает парсер на 8-16МБ.
    private const int PipeResumeThreshold = 8 * MinimumSegmentSize;
    private const int PipePauseThreshold = 16 * MinimumSegmentSize;
    // Ограничение канала материализованных записей одного чанка.
    private const int ChannelCapacity = 16;
    // Буфер выходного FileStream.
    private const int OutputBufferSize = 4 * MinimumSegmentSize;

    [CliArgument(Description = "Destination file (will be overwritten)",
        ValidationRules = CliValidationRules.LegalPath)]
    public FileInfo Destination { get; set; } = null!;

    [CliOption(Name = "compare-options", Description = "Text comparison used at split: None (default), Ordinal, IgnoreCase, IgnoreNonSpace, IgnoreSymbols, or a combination")]
    public string CompareOptionsName { get; set; } = "None";

    /// <summary>Точный размер итогового файла для преаллокации; передаётся командой sort.</summary>
    internal long? PreallocatedLength { get; set; }

    public Task<int> RunAsync(CliContext context) =>
        RootCliCommand.RunWithSettingsAsync(CompareOptionsName, ExecuteAsync, context);

    private async Task<int> ExecuteAsync(Encoding encoding, CompareOptions compareOptions, CancellationToken ct)
    {
        await MergeAsync(encoding, compareOptions, ReadStdinLines(ct), ct);
        return 0;
    }

    private static async IAsyncEnumerable<string> ReadStdinLines([EnumeratorCancellation] CancellationToken ct = default)
    {
        while (Console.ReadLine() is { } line)
            yield return line;
    }

    /// <summary>Мержит чанки в Destination с готовыми настройками; используется и командой sort. Возвращает список чанков.</summary>
    internal async Task<IReadOnlyList<string>> MergeAsync(Encoding encoding, CompareOptions compareOptions, IAsyncEnumerable<string> chunkFiles, CancellationToken ct)
    {
        Stopwatch sw = Stopwatch.StartNew();

        var isOrdinal = compareOptions == CompareOptions.Ordinal;
        var compareInfo = CultureInfo.CurrentCulture.CompareInfo;
        var memoryPool = new ScratchMemoryPool(ArrayPool<byte>.Create(8 * MinimumSegmentSize,
            (int)(GC.GetGCMemoryInfo().TotalAvailableMemoryBytes / MinimumSegmentSize / 8 / 2)), 8 * MinimumSegmentSize);
        var newLine = encoding.GetBytes(Environment.NewLine);
        var dotSpace = encoding.GetBytes(". ");

        using FileStream output = new(Destination.FullName, FileMode.Create, FileAccess.Write, FileShare.None,
            bufferSize: OutputBufferSize);
        if (PreallocatedLength is { } size)
            output.SetLength(size);
        output.Seek(0, SeekOrigin.Begin);

        // Группы пулятся: писатель возвращает группу в пул после записи.
        ObjectPool<MergeGroup> groupPool = new DefaultObjectPool<MergeGroup>(new MergeGroupPoolPolicy());
        ActionBlock<MergeGroup> writer = new(group =>
        {
            WriteGroup(group, output, encoding, dotSpace, newLine);
            groupPool.Return(group);
        }, new ExecutionDataflowBlockOptions
        {
            BoundedCapacity = 4,
            MaxDegreeOfParallelism = 1,
            CancellationToken = ct,
        });

        // Пайп + продюсер + парсер + канал на каждый чанк; ошибки задач приходят в мерж
        // через канал (ReadAllAsync бросает исключение источника).
        List<Pipe> pipes = [];
        List<Channel<ChunkEntry>> channels = [];
        List<Task> producers = [];
        List<Task> materializers = [];
        List<IAsyncEnumerable<ChunkEntry>> sources = [];
        List<string> chunks = [];
        try
        {
            await foreach (var file in chunkFiles.WithCancellation(ct))
            {
                var trimmed = file.Trim();
                if (trimmed.Length == 0) continue;
                chunks.Add(trimmed);
                Pipe chunkPipe = new(new PipeOptions(
                    pool: memoryPool,
                    minimumSegmentSize: MinimumSegmentSize,
                    resumeWriterThreshold: PipeResumeThreshold,
                    pauseWriterThreshold: PipePauseThreshold,
                    useSynchronizationContext: false));
                pipes.Add(chunkPipe);
                producers.Add(ProduceChunk(trimmed, chunkPipe.Writer, ct));
                Channel<ChunkEntry> channel = Channel.CreateBounded<ChunkEntry>(ChannelCapacity);
                channels.Add(channel);
                materializers.Add(MaterializeChunk(chunkPipe.Reader, channel.Writer, isOrdinal, encoding, compareInfo, compareOptions, ct));
                sources.Add(channel.Reader.ReadAllAsync(ct));
            }

            await foreach (var group in sources.MergeSorted(
                ChunkEntryComparer.Instance,
                groupPool,
                ct).WithCancellation(ct).ConfigureAwait(false))
            {
                await writer.SendAsync(group, ct).ConfigureAwait(false);
            }
        }
        catch
        {
            // Ошибка мержа: завершаем читателей пайпов (энумераторы доиграют в finally мержа)
            // и читателей каналов — заблокированные на WriteAsync парсеры выйдут; ошибки задач
            // вторичны к исходной.
            writer.Complete();
            try { await writer.Completion.ConfigureAwait(false); }
            catch { /* причина — в исходной ошибке мержа */ }
            foreach (var pipe in pipes)
                await pipe.Reader.CompleteAsync().ConfigureAwait(false);
            foreach (var channel in channels)
                channel.Writer.TryComplete();
            try { await Task.WhenAll(producers.Concat(materializers)).ConfigureAwait(false); }
            catch { /* причина — в исходной ошибке мержа */ }
            throw;
        }
        writer.Complete();
        await writer.Completion.ConfigureAwait(false);
        await Task.WhenAll(producers.Concat(materializers)).ConfigureAwait(false);

        if (PreallocatedLength is not null)
            output.SetLength(output.Position);
        output.Flush();
        Console.WriteLine($"Merged: {Destination.FullName}");
        Console.WriteLine(sw.Elapsed);
        return chunks;
    }

    /// <summary>
    /// Продюсер одного чанка: FileStream → zstd → буфер пайпа. Сегмент пайпа — и есть буфер
    /// чтения (GetSpan из общего пула), ноль копий. Flush по порогу; backpressure — pause-порог.
    /// </summary>
    private async Task ProduceChunk(string path, PipeWriter writer, CancellationToken ct)
    {
        try
        {
            // Большой буфер FileStream: ОС получает последовательные чтения по 4МБ независимо
            // от чередования чтений сотен чанков — мелкие запросы роняют полосу HDD в разы.
            using FileStream fileStream = new(path, FileMode.Open, FileAccess.Read, FileShare.Read,
                bufferSize: 4 << 20, FileOptions.SequentialScan);
            // Окно сжатия чанков ограничено бюджетом (~2ГБ на все параллельные декодеры) —
            // лимит задаёт декодер.
            using ZstandardStream zstd = new(fileStream, new ZstandardDecoder(31));
            while (true)
            {
                var memory = writer.GetMemory(MinimumSegmentSize);
                var n = await zstd.ReadAsync(memory, ct).ConfigureAwait(false);
                if (n == 0)
                    break;
                writer.Advance(n);
                if (writer.UnflushedBytes >= MinimumSegmentSize)
                {
                    var flush = await writer.FlushAsync(ct).ConfigureAwait(false);
                    if (flush.IsCompleted)
                        throw new OperationCanceledException("Merge stopped consuming the chunk pipe");
                }
            }
            await writer.CompleteAsync();
        }
        catch (Exception ex)
        {
            await writer.CompleteAsync(ex);
            throw;
        }
    }

    /// <summary>
    /// Парсер чанка: разбирает пайп в записи, материализует каждую в ChunkEntry (аренда массива,
    /// текст + ключ + развёрнутые значения) и кладёт в ограниченный канал — сортировка ключей
    /// идёт параллельно по всем чанкам.
    /// </summary>
    private static async Task MaterializeChunk(PipeReader reader, ChannelWriter<ChunkEntry> channel,
        bool ordinal, Encoding encoding, CompareInfo compareInfo, CompareOptions compareOptions, CancellationToken ct)
    {
        try
        {
            await foreach (var raw in reader.ReadLines(ct).ConfigureAwait(false))
            {
                var entry = ChunkEntry.Create(raw.Text, raw.Values, raw.Count,
                    ordinal, encoding, compareInfo, compareOptions);
                await channel.WriteAsync(entry, ct).ConfigureAwait(false);
            }
            channel.TryComplete();
        }
        catch (Exception ex)
        {
            channel.TryComplete(ex);
        }
    }

    /// <summary>
    /// Пишет группу записей с равным ключом: значения каждой записи по возрастанию
    /// (культурно равные тексты, например канонически эквивалентные формы, сливаются в общий
    /// порядок). Одиночная запись пишется прямо из её сегментов, без копий.
    /// </summary>
    private static void WriteGroup(MergeGroup group, FileStream output, Encoding encoding, byte[] dotSpace, byte[] newLine)
    {
        foreach (var (text, value) in group)
            WriteEntry(value, text.Span, output, encoding, dotSpace, newLine);
    }

    private static void WriteEntry(long value, Span<byte> text, FileStream stream, Encoding encoding, byte[] dotSpace, byte[] newLine)
    {
        Span<char> number = stackalloc char[32];
        value.TryFormat(number, out var digits, null, null);

        Span<byte> numberBytes = stackalloc byte[encoding.GetMaxByteCount(digits)];
        var written = encoding.GetBytes(number[..digits], numberBytes);
        stream.Write(numberBytes[..written]);
        stream.Write(dotSpace);
        stream.Write(text);
        stream.Write(newLine);
    }
}
