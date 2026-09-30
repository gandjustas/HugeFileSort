using BenchmarkDotNet.Running;

// Быстрый прогон всех сценариев по одному разу без BenchmarkDotNet: dotnet run -c Release --project benchmarks -- smoke
if (args is ["smoke", ..])
{
    Smoke.Run();
    return;
}

BenchmarkSwitcher.FromAssembly(typeof(FileReadingBenchmarks).Assembly).Run(args);
