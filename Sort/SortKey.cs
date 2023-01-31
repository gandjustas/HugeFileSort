using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

record struct SortKey(ReadOnlyMemory<byte> OriginalString, ReadOnlyMemory<byte> Key);