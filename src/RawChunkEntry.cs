using System.Buffers;

/// <summary>
/// Сырая запись чанка, разобранная из пайпа: последовательности байт текста и δ-варинтов
/// значений ссылаются в буфер пайпа и валидны до следующего MoveNext источника.
/// </summary>
readonly record struct RawChunkEntry(
    ReadOnlySequence<byte> Text,
    ReadOnlySequence<byte> Values,
    long Count);
