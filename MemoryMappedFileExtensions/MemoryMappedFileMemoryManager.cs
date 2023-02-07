using System.Buffers;

namespace System.IO.MemoryMappedFiles
{
    internal unsafe class MemoryMappedFileMemoryManager : MemoryManager<byte>
    {
        private readonly MemoryMappedViewAccessor source;
        private readonly byte* ptr;
        private readonly int size;
        public MemoryMappedFileMemoryManager(MemoryMappedViewAccessor source)
        {
            this.source = source;
            checked //Throw if size larger than int.MaxValue
            {
                size = (int)source.Capacity;
            }
            source.SafeMemoryMappedViewHandle.AcquirePointer(ref this.ptr);
        }

        public override Span<byte> GetSpan()
        {
            return new Span<byte>(this.ptr, this.size);
        }

        public override MemoryHandle Pin(int elementIndex = 0)
        {
            return new MemoryHandle(this.ptr + elementIndex);
        }

        public override void Unpin()
        {
        }

        protected override void Dispose(bool disposing)
        {
            source.SafeMemoryMappedViewHandle.ReleasePointer();
        }
    }
}