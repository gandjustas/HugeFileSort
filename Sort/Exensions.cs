using System;
using System.Buffers;
using System.Collections.Generic;
using System.Drawing;
using System.IO;
using System.IO.Pipelines;
using System.Linq;
using System.Reflection.PortableExecutable;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Channels;
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

        public static IEnumerable<T> MergeLines<T>(this IEnumerable<IEnumerable<T>> sources, Comparison<T> comparer)
        {
            var heap = (from source in sources
                        let e = source.GetEnumerator()
                        where e.MoveNext()
                        select e).ToArray();
            BuildHeap(heap, e => e.Current, comparer);

            while (true)
            {
                var min = heap[0];
                yield return min.Current;
                if (!min.MoveNext())
                {
                    min.Dispose();
                    if (heap.Length == 1) yield break;
                    heap[0] = heap[^1];
                    Array.Resize(ref heap, heap.Length - 1);
                }
                Heapify(heap, e => e.Current, 0, comparer);
            }
        }

        private static void Heapify<T, V>(T[] heap, Func<T, V> selector, int index, Comparison<V> comparer)
        {
            var min = index;
            while (true)
            {
                var leftChild = 2 * index + 1;
                var rightChild = 2 * index + 2;
                var v = selector(heap[min]);

                if (leftChild < heap.Length && comparer(v, selector(heap[leftChild])) > 0)
                {
                    min = leftChild;
                    v = selector(heap[min]);
                }

                if (rightChild < heap.Length && comparer(v, selector(heap[rightChild])) > 0)
                {
                    min = rightChild;
                }


                if (min == index) break;

                var temp = heap[index];
                heap[index] = heap[min];
                heap[min] = temp;

                index = min;
            }
        }

        private static void BuildHeap<T, V>(T[] heap, Func<T, V> selector, Comparison<V> comparer)
        {
            for (int i = heap.Length / 2; i >= 0; i--)
            {
                Heapify(heap, selector, i, comparer);
            }
        }


    }
}
