using System.Buffers;
using System.Diagnostics;
using System.Text;
using System.Threading.Channels;
using WorkItem = System.Collections.Generic.List<(System.ReadOnlyMemory<char>, int)>;

internal class Program : IDisposable
{

    const int BufferSize = 1024 * 1024;

    private string file;
    private int chunkSize;
    private StringComparison currentCulture;
    Encoding detectedEncoding;
    int degreeOfParallelism;
    Comparer comparer;
    List<string> tempFiles = new();
    Random random = new();
    Channel<WorkItem> readToSort;
    Channel<WorkItem> sortToWrite;


    System.Diagnostics.Stopwatch timer = new();


    public Program(string file, int chunkSize, StringComparison currentCulture, int degreeOfParallelism)
    {
        this.file = file;
        this.chunkSize = chunkSize;
        this.currentCulture = currentCulture;
        this.degreeOfParallelism = degreeOfParallelism;
        this.comparer = new Comparer(StringComparison.CurrentCulture);

        readToSort = Channel.CreateBounded<WorkItem>(1);
        sortToWrite = Channel.CreateBounded<WorkItem>(1);

    }
    public void Dispose()
    {
        tempFiles.ForEach(File.Delete);
    }

    public async Task SplitSort()
    {
        timer.Restart();

        SemaphoreSlim semaphore = new(1);

        var sorterThreads = Enumerable.Range(0, degreeOfParallelism)
            .Select(_ => Task.Run(async () =>
            {
                await foreach (var chunk in readToSort.Reader.ReadAllAsync())
                {
                    chunk.Sort(comparer);
                    await sortToWrite.Writer.WriteAsync(chunk);
                }
                // Не можем так сделать, когда сортеров будет много
                //sortToWrite.Writer.Complete();
            }))
            .ToArray();

        var writerThread = Task.Run(async () =>
        {
            await foreach (var chunk in sortToWrite.Reader.ReadAllAsync())
            {
                await semaphore.WaitAsync();
                //Записываем чанки на диск
                await WriteChunk(chunk);
                semaphore.Release();
            }
        });

        using var reader = new StreamReader(file, Encoding.UTF8, true, BufferSize);
        detectedEncoding = reader.CurrentEncoding;

        var chunkBuffer = new char[chunkSize];
        var chunkReadPosition = 0;
        while (true)
        {
            await semaphore.WaitAsync();
            int charsRead = await reader.ReadBlockAsync(chunkBuffer, chunkReadPosition, chunkSize - chunkReadPosition);
            semaphore.Release();
            var eos = reader.EndOfStream;
            var m = chunkBuffer.AsMemory(0, chunkReadPosition + charsRead);

            // Заполняем список строк ReadOnlyMemory<char> для сортировки
            WorkItem chunk = new();
            int linePos;
            while ((linePos = m.Span.IndexOf(Environment.NewLine)) >= 0)
            {
                var line = m[..linePos];
                chunk.Add((line, line.Span.IndexOf('.')));
                m = m[(linePos + Environment.NewLine.Length)..];
            }

            // Если это был конец файла, то добавим в список последнюю строку, если она не пустая
            if (eos && m.Length > 0)
            {
                chunk.Add((m, m.Span.IndexOf('.')));
            }

            await readToSort.Writer.WriteAsync(chunk);
            //chunk.Sort(comparer);


            if (eos) break;
            //Остаток буфера переносим в начало нового массива
            chunkBuffer = new char[chunkSize];
            m.CopyTo(chunkBuffer);
            chunkReadPosition = m.Length;
        }
        readToSort.Writer.Complete();
        await Task.WhenAll(sorterThreads);
        sortToWrite.Writer.Complete();
        await writerThread;
        Console.WriteLine($"SplitSort done in {timer.Elapsed}");
    }



    public void Merge()
    {
        timer.Restart();

        var mergedLines = tempFiles
            .Select(f => File.ReadLines(f).Select(s => (s.AsMemory(), s.IndexOf('.')))) // Читаем построчно все файлы, находим в строках точку
            .Merge(comparer);  //Слияние итераторов IEnumerable<IEnumerable<T>> в IEnumerable<T>

        using var sortedFile = new StreamWriter(Path.ChangeExtension(file, ".sorted" + Path.GetExtension(file)), false, detectedEncoding, BufferSize);
        sortedFile.AutoFlush = false;
        foreach (var (l, _) in mergedLines)
        {
            sortedFile.WriteLine(l);
        }
        Console.WriteLine($"Merge done in {timer.Elapsed}");
    }

    async Task WriteChunk(WorkItem chunk)
    {
        // Записываем строки из отсортированного списка во временный файл
        var tempFileName = Path.ChangeExtension(file, $".part-{tempFiles.Count}" + Path.GetExtension(file));
        using (var tempFile = new StreamWriter(tempFileName, false, Encoding.UTF8, BufferSize))
        {
            tempFile.AutoFlush = false;

            foreach (var (l, _) in chunk)
            {
                await tempFile.WriteLineAsync(l);
            }
            await tempFile.FlushAsync();
        }
        tempFiles.Add(tempFileName);
    }

    private static async Task<int> Main(string[] args)
    {
        if ((args?.Length ?? 0) == 0)
        {
            Console.WriteLine($"Usage: {Path.GetFileNameWithoutExtension(Environment.ProcessPath)} <file path> [<approximate chunk size>]");
            return -1;
        }

        var file = args![0];
        var chunkSize = args?.Length > 1 ? int.Parse(args[1]) * 1000_000 / 2 : 10_000_000; //In characters

        using var app = new Program(file, chunkSize, StringComparison.CurrentCulture, 2);
        await app.SplitSort();
        app.Merge();

        return 0;
    }

}

