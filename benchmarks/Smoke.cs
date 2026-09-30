using System.Diagnostics;

/// <summary>Одиночный прогон всех сценариев с проверкой совпадения результата: dotnet run -c Release -- smoke.</summary>
static class Smoke
{
    public static void Run()
    {
        InputFile.EnsureCreated();
        var file = new FileInfo(InputFile.FullPath);
        var sizeMb = file.Length / (1024.0 * 1024.0);
        Console.WriteLine($"Файл: {file.FullName} ({sizeMb:F0} МБ)");
        Console.WriteLine();

        var sw = Stopwatch.StartNew();
        var sync = FileReadingBenchmarks.CountLinesFileStreamSync();
        Report("FileStreamSync", sync, sw.ElapsedMilliseconds);

        sw.Restart();
        var pipeReader = FileReadingBenchmarks.CountLinesPipeReaderOverStreamAsync().GetAwaiter().GetResult();
        Report("PipeReaderOverStream", pipeReader, sw.ElapsedMilliseconds);

        sw.Restart();
        var producer = FileReadingBenchmarks.CountLinesPipeWithProducerThreadAsync().GetAwaiter().GetResult();
        Report("PipeWithProducerThread", producer, sw.ElapsedMilliseconds);

        Console.WriteLine();
        if (sync != pipeReader || sync != producer)
            throw new InvalidOperationException($"Сценарии посчитали разное число строк: {sync} / {pipeReader} / {producer}");
        Console.WriteLine($"Все сценарии посчитали одинаково: {sync:N0} строк");

        void Report(string name, long lines, long ms) =>
            Console.WriteLine($"{name,-22} {lines,15:N0} строк  {ms,8} мс  {sizeMb / Math.Max(ms, 1) * 1000,5:F0} МБ/с");
    }
}
