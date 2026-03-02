using System.Buffers;
using System.IO.Pipelines;

public static class PipeReaderExtensions
{
    public static async Task ProcessLines(this PipeReader reader, byte[] newLine, Action<ReadOnlySpan<byte>> processLine, CancellationToken ct)
    {
        while (true)
        {
            var result = await reader.ReadAsync(ct).ConfigureAwait(false);
            var buffer = result.Buffer;
            SequenceReader<byte> sequenceReader = new(buffer);
            while (!sequenceReader.End)
            {
                while (sequenceReader.TryReadTo(out ReadOnlySpan<byte> line, newLine))
                {
                    processLine(line);
                }

                buffer = buffer.Slice(sequenceReader.Position);
                sequenceReader.Advance(buffer.Length);
            }

            reader.AdvanceTo(buffer.Start, buffer.End);
            if (result.IsCompleted) break;
        }
    }

}