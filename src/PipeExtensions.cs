using System.Buffers;
using System.IO.Pipelines;
using System.Runtime.CompilerServices;

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

    }
}

