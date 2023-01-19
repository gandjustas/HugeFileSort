using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO.Pipelines;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Channels;

namespace Sort
{
    class PipeChunkReader
    {
        private readonly Stream stream;
        private readonly Encoding encoding;
        private readonly ChannelWriter<((string, int)[], bool)> output;
        private readonly int chunkSize;
        private readonly int bufferSize;

        public PipeChunkReader(Stream stream, Encoding encoding, int bufferSize, ChannelWriter<((string, int)[], bool)> output, int chunkSize)
        {
            this.stream = stream;
            this.encoding = encoding;
            this.output = output;
            this.chunkSize = chunkSize;
            this.bufferSize = bufferSize;
        }
        public async Task Process()
        {
            try
            {
                var reader = PipeReader.Create(stream, new StreamPipeReaderOptions(leaveOpen: false, bufferSize: bufferSize));
                var newLine = encoding.GetBytes(Environment.NewLine);
                while (true)
                {
                    var result = await reader.ReadAsync();
                    var buffer = result.Buffer;

                    await output.WaitToWriteAsync();
                    ProcessLines(ref buffer, newLine.AsSpan());

                    reader.AdvanceTo(buffer.Start, buffer.End);

                    if (result.IsCompleted) break;
                }
                await reader.CompleteAsync();
                if (chunk != null) 
                {
                    var c = chunk;
                    Array.Resize(ref c, chunkPosition);
                    ArrayPool<(string, int)>.Shared.Return(chunk);
                    await output.WriteAsync((c,false));
                }
            }
            finally
            {
                output.Complete();
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        void ProcessLines(ref ReadOnlySequence<byte> buffer, Span<byte> newLine)
        {
            if (buffer.IsSingleSegment)
            {
                var span = buffer.FirstSpan;
                int consumed;
                while (span.Length > 0)
                {
                    var idx = span.IndexOf(newLine);

                    if (idx == -1) break;

                    AddToChunk(span[..idx]);

                    consumed = idx + newLine.Length;
                    span = span[consumed..];
                    buffer = buffer.Slice(consumed);
                }
            }
            else
            {
                var sequenceReader = new SequenceReader<byte>(buffer);

                while (!sequenceReader.End)
                {
                    while (sequenceReader.TryReadTo(out ReadOnlySpan<byte> line, newLine))
                    {
                        AddToChunk(line);
                    }

                    buffer = buffer.Slice(sequenceReader.Position);
                    sequenceReader.Advance(buffer.Length);
                }
            }
        }

        private (string, int)[]? chunk;
        private int chunkPosition = 0;
        private void AddToChunk(ReadOnlySpan<byte> line)
        {
            chunk ??= ArrayPool<(string, int)>.Shared.Rent(chunkSize);
            var str = encoding.GetString(line);
            chunk[chunkPosition++] = (str, str.IndexOf('.'));
            if (chunkPosition >= chunk.Length)
            {
                while (!output.TryWrite((chunk, true)))
                {
                    Thread.Yield();
                };
                chunk = null;
                chunkPosition = 0;
            }
        }
    }
}
