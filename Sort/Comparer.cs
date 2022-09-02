using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Sort
{    public class Comparer : IComparer<string>
    {
        private StringComparison stringComparison;

        public Comparer(StringComparison stringComparison)
        {
            this.stringComparison = stringComparison;
        }

#pragma warning disable CS8767 // Допустимость значений NULL для ссылочных типов в типе параметра не соответствует неявно реализованному элементу (возможно, из-за атрибутов допустимости значений NULL).
        public int Compare(string x, string y)
#pragma warning restore CS8767 // Допустимость значений NULL для ссылочных типов в типе параметра не соответствует неявно реализованному элементу (возможно, из-за атрибутов допустимости значений NULL).
        {
            var spanX = x.AsSpan();
            var spanY = y.AsSpan();
            var xDot = spanX.IndexOf('.');
            var yDot = spanY.IndexOf('.');

            var cmp = spanX.Slice(xDot + 2).CompareTo(spanY.Slice(yDot + 2), stringComparison);
            if (cmp != 0) return cmp;
            return int.Parse(spanX.Slice(0, xDot)) - int.Parse(spanY.Slice(0, yDot));
        }
    }
}
