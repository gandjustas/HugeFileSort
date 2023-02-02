using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

record struct SortKey(ReadOnlyMemory<byte> OriginalString, ReadOnlyMemory<byte> Key);

public record Comparer()
    : IComparer<SortKey>
{
    int IComparer<SortKey>.Compare(SortKey x, SortKey y)
    {
        return x.Key.Span.SequenceCompareTo(y.Key.Span);
    }

}