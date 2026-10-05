using System.Buffers;
using System.Globalization;
using System.Text;

namespace HugeFileSort.Tests;

/// <summary>
/// Группа равных ключей: одиночная запись без копий, материализация с memcpy значений,
/// рост буфера (мета в хвосте), k-way merge значений группы, культура, Reset/переиспользование.
/// </summary>
public class MergeGroupTests
{
    static readonly CompareInfo CompareInfo = CultureInfo.CurrentCulture.CompareInfo;

    [Test]
    public async Task Single_Entry_Iterates_Without_Copy_And_Reset_Allows_Reuse()
    {
        var group = new MergeGroup();
        await Assert.That(group.TryAdd(MakeEntry("hello", [7, 3, 10], ordinal: true))).IsTrue();

        var got = new List<(string Text, long Value)>();
        foreach (var (text, value) in group)
            got.Add((Encoding.UTF8.GetString(text.Span), value));

        // Порядок важен (значения по возрастанию) — SequenceEqual, не IsEquivalentTo.
        await Assert.That(got.Select(g => g.Value).SequenceEqual([3L, 7L, 10L])).IsTrue();
        await Assert.That(got.All(g => g.Text == "hello")).IsTrue();

        group.Reset();
        await Assert.That(group.TryAdd(MakeEntry("again", [1], ordinal: true))).IsTrue();
    }

    [Test]
    [Timeout(120_000)]
    public async Task Group_Of_Duplicates_Grows_Buffer_And_Merges_Values_Ascending(CancellationToken ct)
    {
        var group = new MergeGroup();
        var random = new Random(11);
        const int records = 200, valuesPerRecord = 10_000;
        for (var r = 0; r < records; r++)
        {
            var values = new long[valuesPerRecord];
            for (var v = 0; v < values.Length; v++)
                values[v] = random.Next(1_000_000_000);
            Array.Sort(values);
            await Assert.That(group.TryAdd(MakeEntry("same-key", values, ordinal: true))).IsTrue();
        }

        long last = long.MinValue;
        var count = 0;
        foreach (var (text, value) in group)
        {
            await Assert.That(value).IsGreaterThanOrEqualTo(last);
            await Assert.That(Encoding.UTF8.GetString(text.Span)).IsEqualTo("same-key");
            last = value;
            count++;
        }
        await Assert.That(count).IsEqualTo(records * valuesPerRecord);
        group.Reset();
    }

    [Test]
    public async Task Culture_Canonically_Equal_Keys_Are_Merged_Others_Stay_Separate()
    {
        var group = new MergeGroup();
        // «café» в NFC и NFD — канонически эквивалентные формы с одинаковым sort key.
        var nfc = group.TryAdd(MakeEntry("café", [5, 30], ordinal: false));
        var nfd = group.TryAdd(MakeEntry("café", [10], ordinal: false));
        var plain = group.TryAdd(MakeEntry("cafe", [1], ordinal: false)); // другой ключ

        await Assert.That(nfc).IsTrue();
        await Assert.That(nfd).IsTrue();
        await Assert.That(plain).IsFalse();

        var values = new List<long>();
        var texts = new List<string>();
        foreach (var (text, value) in group)
        {
            values.Add(value);
            texts.Add(Encoding.UTF8.GetString(text.Span));
        }
        await Assert.That(values.SequenceEqual([5L, 10L, 30L])).IsTrue();
        await Assert.That(texts.Count).IsEqualTo(3);
        group.Reset();
    }

    [Test]
    public async Task Different_Keys_Do_Not_Merge()
    {
        var group = new MergeGroup();
        var first = group.TryAdd(MakeEntry("aa", [1], ordinal: true));
        var different = group.TryAdd(MakeEntry("ab", [2], ordinal: true));
        var sameAgain = group.TryAdd(MakeEntry("aa", [2], ordinal: true));

        await Assert.That(first).IsTrue();
        await Assert.That(different).IsFalse();
        await Assert.That(sameAgain).IsTrue();

        var values = new List<long>();
        foreach (var (_, value) in group)
            values.Add(value);
        await Assert.That(values.SequenceEqual([1L, 2L])).IsTrue();
        group.Reset();
    }

    /// <summary>Собирает ChunkEntry: развёрнутые значения δ-варинтами, как пишет парсер чанка.</summary>
    static ChunkEntry MakeEntry(string text, long[] values, bool ordinal)
    {
        Array.Sort(values);
        var textBytes = Encoding.UTF8.GetBytes(text);
        var buf = new byte[8 * values.Length];
        var pos = 0;
        long previous = 0;
        foreach (var value in values)
        {
            pos = Extensions.WriteVarint(buf, pos, (ulong)(value - previous));
            previous = value;
        }
        return ChunkEntry.Create(new ReadOnlySequence<byte>(textBytes), new ReadOnlySequence<byte>(buf, 0, pos),
            values.Length, ordinal, Encoding.UTF8, CompareInfo, CompareOptions.None);
    }
}
