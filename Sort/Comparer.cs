public record Comparer(StringComparison stringComparison) : IComparer<(ReadOnlyMemory<char>, int)>
{
    public int Compare((ReadOnlyMemory<char>, int) x, (ReadOnlyMemory<char>, int) y)
    {
        var spanX = x.Item1.Span;
        var spanY = y.Item1.Span;
        var xDot = x.Item2;
        var yDot = y.Item2;

        var cmp = spanX[(xDot + 2)..].CompareTo(spanY[(yDot + 2)..], stringComparison);
        if (cmp != 0) return cmp;
        return int.Parse(spanX[..xDot]) - int.Parse(spanY[..yDot]);
    }
}