using System.Buffers;
using System.IO.Pipelines;
using System.Runtime.CompilerServices;

static class PipeExtensions
{
    public delegate T ParseDelegate<T>(ref ReadOnlySequence<byte> buffer) where T: allows ref struct;

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
                    var sequenceReader = new SequenceReader<byte>(search);
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

        public IAsyncEnumerable<T> Parse<T>(ParseDelegate<T> parse) where T: allows ref struct
        {
            ArgumentNullException.ThrowIfNull(reader);
            ArgumentNullException.ThrowIfNull(parse);
            return new ParserAsyncEnumerable<T>(reader, parse);
        }

    
    }

    class ParserAsyncEnumerable<T>(PipeReader reader, ParseDelegate<T> parse) : IAsyncEnumerable<T>, IAsyncEnumerator<T> where T : allows ref struct
    {
        bool started = false;
        CancellationToken ct = default;
        ReadOnlySequence<byte> buffer;

        public T Current => parse(ref buffer);

        public ValueTask DisposeAsync()
        {
            return reader.CompleteAsync();            
        }

        public IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default)
        {
            if(started) throw new InvalidOperationException();
            started = true;
            ct = cancellationToken;
            return this;            
        }

        public async ValueTask<bool> MoveNextAsync()
        {
            reader.AdvanceTo(buffer.Start);
            var result = await reader.ReadAsync(ct);
            buffer = result.Buffer;
            return !result.IsCompleted || buffer.Length > 0;
        }

    }
}

