using System.Buffers;

/// <summary>
/// Запись промежуточного файла при merge: текст и sort key — сегменты буферов пакета (Batch),
/// значения — срез общего массива значений пакета. Dispose уменьшает счётчик пакета: последний
/// возврат возвращает буферы пакета в пулы (MergeSorted делает это после потребления группы).
/// </summary>
sealed class ReadChunkEntry(
    Batch batch,
    ArraySegment<byte> text,
    ArraySegment<byte> sortKey,
    long[] values,
    int valuesStart,
    int valuesCount) : IDisposable
{
    /// <summary>Байты текста в кодировке файла.</summary>
    public ArraySegment<byte> Text { get; } = text;

    /// <summary>Явный sort key текста (ordinal: алиас текста; культура: сегмент буфера пакета).</summary>
    public ArraySegment<byte> SortKey { get; } = sortKey;

    /// <summary>Общий массив значений пакета (срез — по ValuesStart/ValuesCount).</summary>
    public long[] ValuesArray { get; } = values;

    public int ValuesStart { get; } = valuesStart;

    public int ValuesCount { get; } = valuesCount;

    public void Dispose() => batch.Release();
}

/// <summary>
/// Пакет записей одного чанка при merge: арендованный буфер текстов (записи ссылаются в него
/// zero-copy), буфер sort key (в ordinal-режиме не выделяется) и общий массив значений.
/// Пакет живёт, пока merge не потребит все записи: каждая ReadChunkEntry.Dispose уменьшает
/// счётчик, последний возвращает массивы в пулы.
/// </summary>
sealed class Batch(byte[] buffer, byte[]? sortKeyBuffer, int entryCount)
{
    private readonly byte[] buffer = buffer;
    private readonly byte[]? sortKeyBuffer = sortKeyBuffer;
    private int remaining = entryCount;

    /// <summary>Записи пакета; заполняется после создания пакета.</summary>
    public ReadChunkEntry[] Entries { get; set; } = [];

    /// <summary>Вызывается при потреблении очередной записи; последний возврат — буферы в пулы.</summary>
    public void Release()
    {
        if (Interlocked.Decrement(ref remaining) == 0)
        {
            ArrayPool<byte>.Shared.Return(buffer);
            if (sortKeyBuffer is not null)
                ArrayPool<byte>.Shared.Return(sortKeyBuffer);
        }
    }
}

/// <summary>Метаданные записи пакета: смещение/длина текста, диапазон значений в общем массиве.</summary>
readonly record struct BatchEntryMeta(int TextOffset, int TextLength, int ValuesStart, int ValuesCount);

/// <summary>Сравнивает записи промежуточных файлов только по sort key (побайтово).</summary>
sealed class ChunkEntryComparer : IComparer<ReadChunkEntry>
{
    public static readonly ChunkEntryComparer Instance = new();

    public int Compare(ReadChunkEntry? x, ReadChunkEntry? y)
    {
        if (x is null) return y is null ? 0 : -1;
        if (y is null) return 1;
        return x.SortKey.AsSpan().SequenceCompareTo(y.SortKey.AsSpan());
    }
}
