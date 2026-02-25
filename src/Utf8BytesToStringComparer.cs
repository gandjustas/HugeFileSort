using System.Diagnostics.CodeAnalysis;
using System.Text;

class StringToUtf8BytesComparer(Encoding enc, int seed = 0) : IEqualityComparer<string>, IAlternateEqualityComparer<ReadOnlySpan<byte>, string>
{
    public string Create(ReadOnlySpan<byte> alternate)
    {
        return enc.GetString(alternate);
    }

    public bool Equals(ReadOnlySpan<byte> alternate, string other)
    {
        Span<byte> buffer = stackalloc byte[enc.GetByteCount(other)];
        enc.GetBytes(other, buffer);
        return alternate.SequenceCompareTo(buffer) == 0;
    }

    public bool Equals(string? x, string? y)
    {
        return string.Equals(x, y);
    }

    public int GetHashCode(ReadOnlySpan<byte> alternate)
    {
        return (int)System.IO.Hashing.XxHash32.HashToUInt32(alternate, seed);
    }

    public int GetHashCode([DisallowNull] string obj)
    {
        Span<byte> buffer = stackalloc byte[enc.GetByteCount(obj)];
        enc.GetBytes(obj, buffer);
        return (int)System.IO.Hashing.XxHash32.HashToUInt32(buffer, seed);
    }
}