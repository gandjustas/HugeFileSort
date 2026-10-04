using System.Buffers;
using System.Diagnostics;
using System.Globalization;
using System.IO.Compression;
using System.IO.Pipelines;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using DotMake.CommandLine;

/// <summary>
/// Команда merge и конвейер слияния: на каждый чанк — свой пайп и выделенный поток чтения
/// (обычный FileStream → zstd → вычисление sort key → запись записей в пайп одним GetSpan).
/// Мерж разбирает пайпы в IAsyncEnumerable&lt;ChunkLine&gt; и сливает их через хип по sort key;
/// тексты и значения берутся прямо из буферов пайпов. consumed пайпа двигается только по
/// обработанным записям (ChunkLine.Dispose → окно), поэтому байты невыписанных записей не
/// переиспользуются. Запись результата — через Pipe (глубокие пороги) в выделенный поток
/// с sync FileStream без буфера и с преаллокацией.
/// </summary>
[CliCommand(Parent = typeof(RootCliCommand), Description = "Merge sorted chunk files listed on stdin into one file")]
internal partial class MergeCliCommand
{
    // Буфер распаковки одного чанка и порог сброса его пайпа; рост под огромную запись — до 16МБ.
    private const int WorkBufferSize = 1 << 20;
    private const int MaxWorkBufferSize = 16 << 20;
    private const int PipeFlushThreshold = 1 << 20;

    // Границы stackalloc: значения записи и char-буфер декодирования текста.
    private const int StackallocLongThreshold = 512;
    private const int StackallocCharThreshold = 8 * 1024;

    [CliArgument(Description = "Destination file (will be overwritten)",
        ValidationRules = CliValidationRules.LegalPath)]
    public FileInfo Destination { get; set; } = null!;

    [CliOption(Name = "flush-mb", Description = "Unflushed bytes threshold for the output writer, MB (default: 32)")]
    public int FlushMb { get; set; } = 32;

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
        {
            line = line.Trim();
            if (line.Length != 0)
                yield return line;
        }
    }

    /// <summary>Мержит чанки в Destination с готовыми настройками; используется и командой sort. Возвращает список чанков.</summary>
    internal async Task<IReadOnlyList<string>> MergeAsync(Encoding encoding, CompareOptions compareOptions, IAsyncEnumerable<string> chunkFiles, CancellationToken ct)
    {
        Stopwatch sw = Stopwatch.StartNew();
        List<string> chunks = [];
        await foreach (var file in chunkFiles.WithCancellation(ct).ConfigureAwait(false))
        {
            var trimmed = file.Trim();
            if (trimmed.Length != 0)
                chunks.Add(trimmed);
        }

        var newLine = encoding.GetBytes(Environment.NewLine);
        var dotSpace = encoding.GetBytes(". ");
        var flushThreshold = FlushMb * 1024 * 1024;
        // Запись: Pipe позволяет циклу merge опережать диск на 2x flushThreshold.
        Pipe pipe = new(new PipeOptions(
            minimumSegmentSize: 1 << 20,
            pauseWriterThreshold: (long)flushThreshold * 2,
            resumeWriterThreshold: flushThreshold,
            useSynchronizationContext: false));
        using FileStream output = new(Destination.FullName, FileMode.Create, FileAccess.Write, FileShare.None,
            bufferSize: 0, FileOptions.SequentialScan);
        if (PreallocatedLength is { } size)
            output.SetLength(size);
        output.Seek(0, SeekOrigin.Begin);
        var drainer = Task.Factory.StartNew(() => DrainPipe(pipe.Reader, output, ct), ct, TaskCreationOptions.LongRunning, TaskScheduler.Default);

        var isOrdinal = compareOptions == CompareOptions.Ordinal;
        var compareInfo = CultureInfo.CurrentCulture.CompareInfo;
        // Пул на все пайпы чанков (сегменты 1МБ) и буферы распаковки; запись крупнее сегмента
        // аллоцируется вне пула. CPU-слот: не больше ProcessorCount чанков считают sort key сразу.
        ArrayPool<byte> pool = ArrayPool<byte>.Create(8 << 20, Math.Max(32, chunks.Count * 4));
        MemoryPool<byte> memoryPool = new ScratchMemoryPool(pool);
        SemaphoreSlim cpuSlots = new(Environment.ProcessorCount);

        // Пайп и выделенный поток чтения на каждый чанк; ошибки продюсера приходят в мерж через его пайп.
        List<IAsyncEnumerable<ChunkLine>> sources = new(chunks.Count);
        List<Task> producers = new(chunks.Count);
        for (var i = 0; i < chunks.Count; i++)
        {
            var path = chunks[i];
            Pipe chunkPipe = new(new PipeOptions(
                pool: memoryPool,
                minimumSegmentSize: 1 << 20,
                // Квант пробуждения продюсера = pause-resume: при мелких порогах сотни читателей
                // на HDD получают по ~3МБ на пробуждение и платят поиском за каждый — на 100ГБ
                // это вдвое-втро медленнее. 12МБ квант даёт ~4х меньше переключений головки.
                resumeWriterThreshold: 4 << 20,
                pauseWriterThreshold: 16 << 20,
                useSynchronizationContext: false));
            producers.Add(Task.Factory.StartNew(
                () => ProduceChunk(path, chunkPipe.Writer, pool, cpuSlots, encoding, compareInfo, compareOptions, isOrdinal, ct),
                ct, TaskCreationOptions.LongRunning, TaskScheduler.Default));
            sources.Add(chunkPipe.Reader.ReadLines(ct));
        }

        var number = new char[32];
        var writer = pipe.Writer;
        await foreach (var group in sources.MergeSorted(ChunkLineComparer.Instance, static group => group, ct).WithCancellation(ct).ConfigureAwait(false))
        {
            WriteGroup(group, writer, encoding, number, dotSpace, newLine);
            // Группа записана — записи отдают свои байты пайпам.
            foreach (var line in group)
                line.Dispose();
            if (writer.UnflushedBytes >= flushThreshold)
                await writer.FlushAsync(ct).ConfigureAwait(false);
        }
        await writer.FlushAsync(ct).ConfigureAwait(false);
        await pipe.Writer.CompleteAsync().ConfigureAwait(false);
        await drainer.ConfigureAwait(false);
        await Task.WhenAll(producers).ConfigureAwait(false);
        // Обрезаем преаллокацию по фактическому размеру.
        if (PreallocatedLength is not null)
            output.SetLength(output.Position);
        Console.WriteLine($"Merged: {Destination.FullName}");
        Console.WriteLine(sw.Elapsed);
        return chunks;
    }

    /// <summary>
    /// Продюсер одного чанка (выделенный поток): обычный FileStream → zstd → разбор записей →
    /// sort key → запись записи в пайп. Flush по порогу; backpressure — pause-порог пайпа.
    /// </summary>
    private static void ProduceChunk(
        string path,
        PipeWriter writer,
        ArrayPool<byte> pool,
        SemaphoreSlim cpuSlots,
        Encoding encoding,
        CompareInfo compareInfo,
        CompareOptions compareOptions,
        bool isOrdinal,
        CancellationToken ct)
    {
        byte[]? work = null;
        try
        {
            ct.ThrowIfCancellationRequested();
            // Большой буфер FileStream: ОС получает последовательные чтения по 4МБ независимо
            // от пинг-понга «чтение → sort key» — сотни чередующихся читателей с мелкими
            // запросами роняют полосу HDD в разы (см. trace: продюсеры висят в ZstandardStream.Read).
            using FileStream fileStream = new(path, FileMode.Open, FileAccess.Read, FileShare.Read,
                bufferSize: 4 << 20, FileOptions.SequentialScan);
            // Окно сжатия чанков ограничено бюджетом (~2ГБ на все параллельные декодеры) —
            // лимит задаёт декодер.
            using ZstandardStream zstd = new(fileStream, new ZstandardDecoder(31));
            work = pool.Rent(WorkBufferSize);

            var filled = 0;
            var eof = false;
            while (!eof)
            {
                // Дозаполняем буфер: хвост незавершённой записи уже в начале.
                while (filled < work.Length)
                {
                    var n = zstd.Read(work.AsSpan(filled));
                    if (n == 0)
                    {
                        eof = true;
                        break;
                    }
                    filled += n;
                }

                // Разбор и запись записей — под CPU-слотом.
                var pos = 0;
                cpuSlots.Wait(ct);
                try
                {
                    var span = work.AsSpan(0, filled);
                    while (TryWriteRecord(writer, span, ref pos, isOrdinal, encoding, compareInfo, compareOptions)) { }
                }
                finally
                {
                    cpuSlots.Release();
                }

                // Хвост незавершённой записи — в начало буфера.
                var tail = filled - pos;
                if (tail > 0)
                    Buffer.BlockCopy(work, pos, work, 0, tail);
                filled = tail;
                if (eof && filled != 0)
                    throw new FormatException("Truncated entry at end of chunk file");
                if (!eof && pos == 0)
                {
                    // Ни одна запись не влезла — растим буфер под неё.
                    if (work.Length >= MaxWorkBufferSize)
                        throw new FormatException("Entry does not fit into the work buffer");
                    var bigger = pool.Rent(work.Length * 2);
                    Buffer.BlockCopy(work, 0, bigger, 0, filled);
                    pool.Return(work);
                    work = bigger;
                    continue;
                }

                if (writer.UnflushedBytes >= PipeFlushThreshold)
                {
                    // Синхронное ожидание на выделенном потоке; pause-порог пайпа тормозит продюсера.
                    var flush = writer.FlushAsync(ct).AsTask().GetAwaiter().GetResult();
                    if (flush.IsCompleted)
                        throw new OperationCanceledException("Merge stopped consuming the chunk pipe");
                }
            }
            writer.Complete();
        }
        catch (Exception ex)
        {
            writer.Complete(ex);
            throw;
        }
        finally
        {
            if (work is not null)
                pool.Return(work);
        }
    }

    /// <summary>
    /// Разбирает одну запись формата чанка с позиции pos (варинт textLen, текст, варинт count,
    /// δ-варинты значений), вычисляет sort key и пишет запись в пайп одним GetSpan:
    /// [varint textLen][text][varint keyLen][key][varint count][count×long]. Одна запись —
    /// один сегмент пайпа. Неполная запись — false, pos восстанавливается.
    /// ordinal: ключ не вычисляется, в пайп пишется keyLength = 0.
    /// </summary>
    private static bool TryWriteRecord(
        PipeWriter writer,
        ReadOnlySpan<byte> span,
        ref int pos,
        bool isOrdinal,
        Encoding encoding,
        CompareInfo compareInfo,
        CompareOptions compareOptions)
    {
        var start = pos;
        if (!Extensions.TryReadVarint(span, ref pos, out var textLength)
            || textLength > (ulong)(span.Length - pos))
        {
            pos = start;
            return false;
        }
        var textStart = pos;
        pos += checked((int)textLength);
        if (!Extensions.TryReadVarint(span, ref pos, out var count))
        {
            pos = start;
            return false;
        }

        // Аренды и stackalloc — в объявлениях до try: stackalloc нельзя присваивать
        // вне области видимости переменной и внутри try (CS8353).
        long[]? rentedValues = null;
        char[]? rentedChars = null;
        Span<long> values = count <= StackallocLongThreshold
            ? stackalloc long[StackallocLongThreshold]
            : (rentedValues = ArrayPool<long>.Shared.Rent(checked((int)count)));

        var maxChars = isOrdinal ? 0 : encoding.GetMaxCharCount(checked((int)textLength));
        Span<char> chars = maxChars <= StackallocCharThreshold
            ? stackalloc char[StackallocCharThreshold]
            : (rentedChars = ArrayPool<char>.Shared.Rent(maxChars));

        try
        {
            var complete = true;
            long previous = 0;
            var deltaPos = pos;
            for (ulong i = 0; i < count; i++)
            {
                if (!Extensions.TryReadVarint(span, ref deltaPos, out var delta))
                {
                    complete = false;
                    break;
                }
                previous += checked((long)delta);
                values[(int)i] = previous;
            }
            if (!complete)
            {
                pos = start;
                return false;
            }
            pos = deltaPos;

            var textLengthInt = checked((int)textLength);
            var written = 0;
            int keyLength = 0;
            if (!isOrdinal)
            {
                written = encoding.GetChars(span.Slice(textStart, textLengthInt), chars);
                keyLength = compareInfo.GetSortKeyLength(chars[..written], compareOptions);
            }

            var size = VarintLength(textLength) + textLengthInt
                + VarintLength((ulong)keyLength) + keyLength
                + VarintLength(count) + checked((int)count * 8);
                var record = writer.GetSpan(size);
                var p = Extensions.WriteVarint(record, 0, textLength);
                span.Slice(textStart, textLengthInt).CopyTo(record.Slice(p, textLengthInt));
                p += textLengthInt;
            p = Extensions.WriteVarint(record, p, (ulong)keyLength);
            if (!isOrdinal)
                compareInfo.GetSortKey(chars[..written], record.Slice(p, keyLength), compareOptions);
            p += keyLength;
            p = Extensions.WriteVarint(record, p, count);
            values[..checked((int)count)].CopyTo(MemoryMarshal.Cast<byte, long>(record.Slice(p, checked((int)count * 8))));
            writer.Advance(size);
            return true;
        }
        finally
        {
            if (rentedValues is not null)
                ArrayPool<long>.Shared.Return(rentedValues);
            if (rentedChars is not null)
                ArrayPool<char>.Shared.Return(rentedChars);
        }
    }

    private static int VarintLength(ulong value)
    {
        var length = 1;
        while (value >= 0x80)
        {
            value >>= 7;
            length++;
        }
        return length;
    }

    /// <summary>Потребитель Pipe: пишет данные в файл синхронно (выделенный поток, буфер FileStream отключён).</summary>
    private static void DrainPipe(PipeReader reader, FileStream output, CancellationToken ct)
    {
        try
        {
            while (true)
            {
                // Синхронное завершение — забираем результат без Task; иначе блокируем выделенный поток.
                var vt = reader.ReadAsync(ct);
                ReadResult result = vt.IsCompleted ? vt.Result : vt.AsTask().GetAwaiter().GetResult();
                var buffer = result.Buffer;
                foreach (var segment in buffer)
                    output.Write(segment.Span);
                reader.AdvanceTo(buffer.End);
                if (result.IsCompleted)
                    break;
            }
            reader.Complete();
        }
        catch (Exception ex)
        {
            reader.Complete(ex);
            throw;
        }
    }

    private static readonly Comparer<(long Value, int Index)> ValueIndexComparer =
        Comparer<(long Value, int Index)>.Create((x, y) => x.Value.CompareTo(y.Value));

    /// <summary>
    /// Пишет группу записей с равным sort key: значения каждой записи по возрастанию
    /// (культурно равные тексты, например «ß» и «ss» в de-DE, сливаются в общий порядок).
    /// Штатный случай — одна запись в группе: значения отдаются напрямую, без хипа.
    /// </summary>
    private static void WriteGroup(IReadOnlyList<ChunkLine> group, PipeWriter writer, Encoding encoding, char[] number, byte[] dotSpace, byte[] newLine)
    {
        if (group.Count == 1)
        {
            var line = group[0];
            foreach (var value in line.Values)
                WriteEntry(value, line.Text, writer, encoding, number, dotSpace, newLine);
            return;
        }

        var pairs = group
            .Select((entry, index) => EnumerateValues(entry, index))
            .MergeSorted(ValueIndexComparer);
        foreach (var (value, index) in pairs)
            WriteEntry(value, group[index].Text, writer, encoding, number, dotSpace, newLine);

        static IEnumerable<(long Value, int Index)> EnumerateValues(ChunkLine line, int index)
        {
            for (var i = 0; i < line.ValuesLength; i++)
                yield return (line.Values[i], index);
        }
    }

    private static void WriteEntry(long value, ReadOnlySpan<byte> text, PipeWriter writer, Encoding encoding, char[] number, byte[] dotSpace, byte[] newLine)
    {
        value.TryFormat(number, out var digits, null, null);
        encoding.GetBytes(number.AsSpan(0, digits), writer);
        writer.Write(dotSpace);
        writer.Write(text);
        writer.Write(newLine);
    }
}
