using System.Buffers;
using System.IO.Hashing;
using System.IO.Pipelines;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

static class PipeExtensions
{
    extension(PipeReader reader)
    {
        public async Task ProcessLines(byte[] newLine, Action<ReadOnlySpan<byte>> processLine, CancellationToken ct)
        {
            await foreach (var line in reader.Split(newLine, ct).ConfigureAwait(false))
            {
                if (line.IsSingleSegment)
                {
                    processLine(line.FirstSpan);
                    continue;
                }

                var rented = ArrayPool<byte>.Shared.Rent((int)line.Length);
                try
                {
                    line.CopyTo(rented);
                    processLine(rented.AsSpan(0, (int)line.Length));
                }
                finally
                {
                    ArrayPool<byte>.Shared.Return(rented);
                }
            }
        }

        public async IAsyncEnumerable<ReadOnlySequence<byte>> Split(
            ReadOnlyMemory<byte> delimiter,
            [EnumeratorCancellation] CancellationToken ct = default)
        {
            if (delimiter.IsEmpty)
                throw new ArgumentException("Delimiter must not be empty", nameof(delimiter));

            try
            {
                while (true)
                {
                    var result = await reader.ReadAsync(ct).ConfigureAwait(false);
                    var buffer = result.Buffer;

                    var consumed = buffer.Start;
                    while (true)
                    {
                        var search = buffer.Slice(consumed);
                        SequenceReader<byte> sequenceReader = new(search);
                        if (!sequenceReader.TryReadTo(out ReadOnlySequence<byte> segment, delimiter.Span))
                            break;
                        consumed = search.GetPosition(sequenceReader.Consumed);
                        yield return segment;
                    }

                    var remaining = buffer.Slice(consumed);
                    if (result.IsCompleted)
                    {
                        if (!remaining.IsEmpty)
                            yield return remaining;
                        reader.AdvanceTo(buffer.End, buffer.End);
                        yield break;
                    }

                    reader.AdvanceTo(remaining.Start, buffer.End);
                }
            }
            finally
            {
                await reader.CompleteAsync().ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Разбирает формат чанка ([varint textLen][text][varint count][δ-varint значения]) в
        /// поток RawChunkEntry. Байты записи остаются валидными до следующего MoveNext
        /// источника: consumed двигается только когда предыдущая запись обработана.
        /// </summary>
        public async IAsyncEnumerable<RawChunkEntry> ReadLines([EnumeratorCancellation] CancellationToken ct = default)
        {
            try
            {
                while (true)
                {
                    var result = await reader.ReadAsync(ct).ConfigureAwait(false);
                    var buffer = result.Buffer;

                    var consumed = buffer.Start;
                    while (true)
                    {
                        var search = buffer.Slice(consumed);
                        SequenceReader<byte> sequenceReader = new(search);

                        if (!sequenceReader.TryReadVarint(out var textLength)) break;
                        var textSize = checked((long)textLength);
                        // Частичная запись в буфере: текст читаем только целиком, иначе Slice
                        // за конец буфера бросит исключение.
                        if (sequenceReader.Remaining < textSize) break;
                        var text = search.Slice(sequenceReader.Position, textSize);
                        // Срез не двигает ридер — текст проходим явно.
                        sequenceReader.Advance(textSize);

                        if (!sequenceReader.TryReadVarint(out var valuesCount)) break;
                        var valuesCountLong = checked((long)valuesCount);
                        var valuesStart = sequenceReader.Position;
                        long read = 0;
                        while (read < valuesCountLong && sequenceReader.TryReadVarint(out _))
                            read++;
                        if (read < valuesCountLong) break;
                        var values = search.Slice(valuesStart, sequenceReader.Position);

                        consumed = search.GetPosition(sequenceReader.Consumed);
                        yield return new(text, values, valuesCountLong);
                    }

                    var remaining = buffer.Slice(consumed);
                    if (result.IsCompleted)
                    {
                        if (!remaining.IsEmpty) throw new InvalidOperationException("Unexpected end of file");
                        reader.AdvanceTo(buffer.End, buffer.End);
                        yield break;
                    }

                    reader.AdvanceTo(remaining.Start, buffer.End);
                }
            }
            finally
            {
                await reader.CompleteAsync().ConfigureAwait(false);
            }
        }
    }
}

