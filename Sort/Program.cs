using System.Buffers;
using System.Diagnostics;
using System.Text;


internal class Program : IDisposable
{

    const int BufferSize = 1024 * 1024;

    private string file;
    private int chunkSize;
    private StringComparison stringComparison;
    Encoding detectedEncoding;
    Comparer comparer;
    List<string> tempFiles = new();
    Random random = new();


    System.Diagnostics.Stopwatch timer = new();


    public Program(string file, int chunkSize, StringComparison currentCulture)
    {
        this.file = file;
        this.chunkSize = chunkSize;
        this.stringComparison = currentCulture;
        this.comparer = new Comparer(StringComparison.CurrentCulture);
    }
    public void Dispose()
    {
        tempFiles.ForEach(File.Delete);
    }

    public void SplitSort()
    {
        timer.Restart();

        using var reader = new StreamReader(file, Encoding.Default, true, BufferSize);
        detectedEncoding = reader.CurrentEncoding;

        List<(ReadOnlyMemory<char>, int)> chunk = new();
        var chunkBuffer = new char[chunkSize];
        var chunkReadPosition = 0;
        while (true)
        {
            var charsRead = reader.ReadBlock(chunkBuffer, chunkReadPosition, chunkSize - chunkReadPosition);
            var eos = reader.EndOfStream;
            var m = chunkBuffer.AsMemory(0, chunkReadPosition + charsRead);

            // Заполняем список строк ReadOnlyMemory<char> для сортировки
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

            chunk.Sort(comparer);

            //Записываем чанки на диск
            WriteChunk(chunk);

            if (eos) break;
            chunk.Clear();

            //Отсток буфера переносим в начало
            m.CopyTo(chunkBuffer);
            chunkReadPosition = m.Length;
        }
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



    void WriteChunk(List<(ReadOnlyMemory<char>, int)> chunk)
    {
        // Записываем строки из отсортированного списка во временный файл
        var tempFileName = Path.ChangeExtension(file, $".part-{tempFiles.Count}" + Path.GetExtension(file));
        using (var tempFile = new StreamWriter(tempFileName, false, Encoding.UTF8, BufferSize))
        {
            tempFile.AutoFlush = false;

            foreach (var (l, _) in chunk)
            {
                tempFile.WriteLine(l);
            }
        }
        tempFiles.Add(tempFileName);
    }

    private static int Main(string[] args)
    {
        if ((args?.Length ?? 0) == 0)
        {
            Console.WriteLine($"Usage: {Path.GetFileNameWithoutExtension(Environment.ProcessPath)} <file path> [<approximate chunk size>]");
            return -1;
        }

        var file = args![0];
        var chunkSize = args?.Length > 1 ? int.Parse(args[1]) * 1000_000 / 2 : 100_000_000; //In characters

        using var app = new Program(file, chunkSize, StringComparison.CurrentCulture);
        app.SplitSort();
        app.Merge();

        return 0;
    }

}

