public record Comparer(StringComparison stringComparison) 
    : IComparer<SortKey> 
{
    int IComparer<SortKey>.Compare(SortKey x, SortKey y)
    {
        return x.Key.Span.SequenceCompareTo(y.Key.Span);
    }

}