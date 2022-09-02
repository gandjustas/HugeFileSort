namespace System.Linq
{
    public static class Exensions
    {
        public static IEnumerable<T> Merge<T>(this IEnumerable<IEnumerable<T>> sources, IComparer<T> comparer = default)
        {
            var enumerators = (from source in sources
                               let e = source.GetEnumerator()
                               where e.MoveNext()
                               select e).ToList();
            
            while (enumerators.Count > 0)
            {
                var min = enumerators.MinBy(e => e.Current, comparer)!;
                yield return min.Current;
                if (!min.MoveNext())
                {
                    min.Dispose();
                    enumerators.Remove(min);
                }
            }
        }

    }
}
