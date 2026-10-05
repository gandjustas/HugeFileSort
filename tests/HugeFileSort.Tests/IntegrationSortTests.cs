using System.Globalization;
using System.Text;
using System.Threading.Channels;

namespace HugeFileSort.Tests;

/// <summary>
/// Интеграционный прогон конвейера sort (split + merge) на детерминированных 100МБ
/// с минимальным размером чанка: ~100 чанков по ~1МБ — много источников мержа.
/// </summary>
public class IntegrationSortTests
{
    [Test]
    [NotInParallel]
    [Timeout(600_000)]
    public async Task Sort_100Mb_MinimalChunks_PreservesMultisetAndOrder(CancellationToken ct)
    {
        var dir = Path.Combine(Path.GetTempPath(), "hugefile-tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        try
        {
            var input = Path.Combine(dir, "input.txt");
            var output = Path.Combine(dir, "input.sorted.txt");
            var length = await TestData.GenerateAsync(input, 100L * 1024 * 1024, ct);
            // Сплит с форсированным минимальным чанком: буфер 1МБ → ~100 чанков.
            var split = new SplitCliCommand
            {
                Source = new FileInfo(input),
                MinChunkMb = 1,
                MaxChunkMb = 1,
            };
            var files = Channel.CreateUnbounded<string>();
            await split.SplitAsync(Encoding.UTF8, CompareOptions.None, files.Writer, ct);
            files.Writer.TryComplete();
            var chunks = new List<string>();
            await foreach (var chunk in files.Reader.ReadAllAsync(ct))
                chunks.Add(chunk);
            await Assert.That(chunks.Count).IsGreaterThan(20);

            var merge = new MergeCliCommand
            {
                Destination = new FileInfo(output),
                PreallocatedLength = length,
            };
            await merge.MergeAsync(Encoding.UTF8, CompareOptions.None, chunks.ToAsyncEnumerable(), ct);

            // Побайтово тот же объём: формат строк и их размеры сохранены, изменился только порядок.
            await Assert.That(new FileInfo(output).Length).IsEqualTo(length);

            // Сортированность — продуктовая проверка.
            var check = new CheckSortedCliCommand { Source = new FileInfo(output) };
            var exit = await check.CheckAsync(Encoding.UTF8, CompareOptions.None, ct);
            await Assert.That(exit).IsEqualTo(0);

            // Мультимножество строк: выход — те же строки с теми же количествами, что и вход.
            await MultisetEquals(input, output);
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }

    static async Task MultisetEquals(string expectedPath, string actualPath)
    {
        var expected = await CountLines(expectedPath);
        var actual = await CountLines(actualPath);
        await Assert.That(actual.Count).IsEqualTo(expected.Count);
        foreach (var (line, count) in expected)
        {
            actual.TryGetValue(line, out var actualCount);
            await Assert.That(actualCount).IsEqualTo(count);
        }
    }

    static async Task<Dictionary<string, int>> CountLines(string path)
    {
        var counts = new Dictionary<string, int>(1 << 20);
        using var reader = new StreamReader(path, Encoding.UTF8);
        while (await reader.ReadLineAsync() is { } line)
        {
            counts.TryGetValue(line, out var count);
            counts[line] = count + 1;
        }
        return counts;
    }
}
