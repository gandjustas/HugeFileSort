using System.Runtime.InteropServices;

namespace System.Collections.Generic
{
    public static class DictionaryExtensions
    {
        public static void AddItemToLookup<K, A, C, V>(this in Dictionary<K, C>.AlternateLookup<A> lookup, A alternateKey, V value)
            where K: notnull
            where A : notnull, allows ref struct
            where C: class, IList<V>, new()
        {
            ref var list = ref CollectionsMarshal.GetValueRefOrAddDefault(lookup, alternateKey, out bool exist);
            if (!exist)
            {
                list = [value];
            }
            else
            {
                list!.Add(value);
            }
        }
    }
}
