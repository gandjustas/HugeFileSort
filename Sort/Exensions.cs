using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Sort
{
    public static class Exensions
    {
        public static IEnumerable<string> EnumerateLines(this StreamReader reader)
        {            
            while (!reader.EndOfStream)
            {
                yield return reader.ReadLine()!;
            }
        }        

        public static IEnumerable<string> MergeLines(this IEnumerable<IEnumerable<string>> sources, IComparer<string> comparer)
        {
            var enumerators = from source in sources
                              let e = source.GetEnumerator()
                              where e.MoveNext()
                              select (e,e.Current);
            var heap = new PriorityQueue<IEnumerator<string>, string>(enumerators, comparer);

            while (heap.Count > 0)
            {
                var min = heap.Dequeue();
                yield return min.Current;
                if (min.MoveNext())
                {
                    heap.Enqueue(min, min.Current);
                }
                else
                { 
                    min.Dispose();
                }
            }
        }

    }
}
