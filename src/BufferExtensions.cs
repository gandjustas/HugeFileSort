using System.Runtime.CompilerServices;
using System.Text;

namespace System.Buffers;

public static class BufferExtensions
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void Write<T>(this IBufferWriter<byte> writer, T value, ReadOnlySpan<char> format) where T : IUtf8SpanFormattable
    {
        var destination = writer.GetSpan(32);
        int bytesWritten;
        while (!value.TryFormat(destination, out bytesWritten, format, null))
        {
            destination = writer.GetSpan(destination.Length * 2);
        }
        writer.Advance(bytesWritten);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void Write(this IBufferWriter<byte> writer, ReadOnlySpan<char> value, Encoding encoding)
    {
        var destination = writer.GetSpan(value.Length * 2);
        _ = encoding.GetBytes(value, writer);
    }
}
