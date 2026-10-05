using System.Globalization;
using System.Text;
using System.Threading.Channels;

namespace HugeFileSort.Tests;

public class SplitTests
{
    [Test]
    public async Task Stages_Are_At_Least_Quarter_Of_Cores()
    {
        await Assert.That(SplitCliCommand.ComputeStages(1))
            .IsEqualTo(Math.Max(1, Environment.ProcessorCount / 4));
    }

    [Test]
    public async Task Stages_Respect_Parallel_Lower_Bound()
    {
        await Assert.That(SplitCliCommand.ComputeStages(1000)).IsEqualTo(1000);
    }

    [Test]
    [Timeout(120_000)]
    public async Task Line_Longer_Than_Buffer_Throws_With_Clear_Message(CancellationToken ct)
    {
        var dir = Path.Combine(Path.GetTempPath(), "hugefile-tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        try
        {
            var input = Path.Combine(dir, "input.txt");
            // Буфер чанка — 1МБ (min = max = 1), текст строки 2МБ — не поместится ни в один чанк.
            // Без BOM: продуктовый парсер читает сырые байты.
            await File.WriteAllTextAsync(input, $"1. {new string('a', 2 * 1024 * 1024)}\r\n", new UTF8Encoding(false));

            var split = new SplitCliCommand
            {
                Source = new FileInfo(input),
                MinChunkMb = 1,
                MaxChunkMb = 1,
            };
            var exception = await Assert.That(async () =>
                    await split.SplitAsync(Encoding.UTF8, CompareOptions.None,
                        Channel.CreateUnbounded<string>().Writer, ct))
                .Throws<InvalidDataException>();
            await Assert.That(exception!.Message)
                .Contains("exceeds the chunk buffer")
                .And.Contains("Increase --min-chunk-mb");
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }
}
