using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading.Tasks;

record struct SortKey(ReadOnlyMemory<byte> OriginalString, ReadOnlyMemory<byte> Key)
{
    public int Length => OriginalString.Length + Key.Length + sizeof(int) * 2;
    public int Write(Span<byte> destination)
    {
        var offset = 0;
        BinaryPrimitives.WriteInt32LittleEndian(destination, OriginalString.Length);
        offset += sizeof(int);
        OriginalString.Span.CopyTo(destination[offset..]);
        offset += OriginalString.Length;

        BinaryPrimitives.WriteInt32LittleEndian(destination[offset..], Key.Length);
        offset += sizeof(int);
        Key.Span.CopyTo(destination[offset..]);
        offset += Key.Length;
        return offset;
    }

    public int Write(byte[] destination, int startIndex)
    {
        return Write(destination.AsSpan(startIndex));
    }
}

public class Comparer
    : IComparer<SortKey>
{
    int IComparer<SortKey>.Compare(SortKey x, SortKey y)
    {
        return x.Key.Span.SequenceCompareTo(y.Key.Span);
    }

}