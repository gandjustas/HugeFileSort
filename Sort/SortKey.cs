using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

record struct SortKey(ReadOnlyMemory<byte> OriginalString, ReadOnlyMemory<byte> Key);

public class SortKeySelector : SortHelpers.ISortKeySelector<SortKey, byte>
{
    ReadOnlySpan<byte> SortHelpers.ISortKeySelector<SortKey, byte>.GetSortKey(SortKey source)
    {
        return source.Key.Span;
    }
}
