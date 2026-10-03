
/// <summary>Сжатый чанк в памяти: сжатые данные (буфер возвращает писатель через Dispose).</summary>
sealed class CompressedChunk(ChunkPools pools, byte[] buffer, int length) : IDisposable
{
    public byte[] Buffer { get; } = buffer;

    public int Length { get; } = length;

    public void Dispose() => pools.Scratch.Return(Buffer);
}
