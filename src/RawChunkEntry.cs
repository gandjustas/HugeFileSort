using System.Buffers;
using System.Runtime.CompilerServices;

/// <summary>
/// Сырая запись чанка, разобранная из пайпа: последовательности байт текста и δ-варинтов
/// значений ссылаются в буфер пайпа и валидны до следующего MoveNext источника.
/// </summary>
readonly record struct RawChunkEntry(
    ReadOnlySequence<byte> Text,
    ReadOnlySequence<byte> Values,
    long Count)
{
    public long Size => Text.Length + Count * Unsafe.SizeOf<long>();
}

/// <summary>Порядок записей: побайтовое сравнение ключей (culture — sort key, ordinal — текст).</summary>
sealed class ChunkEntryComparer : IComparer<ChunkEntry>
{
    public static readonly ChunkEntryComparer Instance = new();

    public int Compare(ChunkEntry x, ChunkEntry y) =>
        x.Key.AsSpan().SequenceCompareTo(y.Key.AsSpan());
}
