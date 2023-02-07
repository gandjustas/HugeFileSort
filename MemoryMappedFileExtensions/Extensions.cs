namespace System.IO.MemoryMappedFiles
{
    public static class Extensions
    {
        public static Buffers.MemoryManager<byte> UnsafeGetMemory(this MemoryMappedViewAccessor source)
        {
            return new MemoryMappedFileMemoryManager(source);
        }
    }
}