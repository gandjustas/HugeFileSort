#if DEBUG
var k = new KeySort();
k.Setup();
k.CountingRadixSort();
#else
BenchmarkDotNet.Running.BenchmarkRunner.Run<Sort>(args: args);
BenchmarkDotNet.Running.BenchmarkRunner.Run<KeySort>(args: args);
BenchmarkDotNet.Running.BenchmarkRunner.Run<ReadLines>(args: args); 
#endif