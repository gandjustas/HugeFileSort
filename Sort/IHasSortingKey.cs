using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Sort
{
    public interface IHasSortKey<T> where T: IComparable<T>
    {
        Span<T> GetSortKey();
    }
}
