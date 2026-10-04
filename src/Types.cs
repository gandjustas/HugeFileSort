using System.Runtime.InteropServices;

/// <summary>
/// Окно чтения пайпа одного чанка: фиксирует абсолютную позицию (в байтах потока чанка), до
/// которой записи обработаны мержем (Dispose последней обработанной ChunkLine). Разбор пайпа
/// двигает consumed только на неё, поэтому пайп не переиспользует байты записей, которые мерж
/// ещё держит. Позиции — long, а не SequencePosition: сегменты пайпа рециклируются (объект
/// сегмента переиспользуется следующей записью), и объекты позиций нельзя хранить через границы
/// ReadAsync. Обращения только из потока мержа, синхронизация не нужна.
/// </summary>
sealed class ChunkWindow
{
    public bool HasReleased { get; private set; }

    public long ReleasedEnd { get; private set; }

    /// <summary>Мерж вызывает после записи записи в результат; релизы идут в порядке записей.</summary>
    public void Release(long end)
    {
        ReleasedEnd = end;
        HasReleased = true;
    }
}

/// <summary>
/// Запись чанка при merge: текст, sort key и значения лежат в одном сегменте буфера пайпа
/// своего чанка (продюсер пишет запись одним GetSpan — запись целиком в одном сегменте).
/// Спаны нарезаются прямо из пайпа; Dispose возвращает байты пайпу после записи группы в мерже.
/// </summary>
readonly struct ChunkLine(
    ChunkWindow window,
    byte[] array,
    int textStart,
    int textLength,
    int keyStart,
    int keyLength,
    int valuesStart,
    int valuesCount,
    long end) : IDisposable
{
    /// <summary>Байты текста в кодировке файла.</summary>
    public ReadOnlySpan<byte> Text => array.AsSpan(textStart, textLength);

    /// <summary>Sort key текста; ordinal-режим пишет keyLength = 0 — ключ алиасит текст.</summary>
    public ReadOnlySpan<byte> SortKey => keyLength == 0 ? Text : array.AsSpan(keyStart, keyLength);

    /// <summary>Значения записи, развёрнутые из δ-варинтов продюсером чанка.</summary>
    public ReadOnlySpan<long> Values => MemoryMarshal.Cast<byte, long>(array.AsSpan(valuesStart, valuesCount * 8));

    /// <summary>Число значений записи — без нарезки спана (нужен в итераторах через yield).</summary>
    public int ValuesLength => valuesCount;

    /// <summary>Запись записана в результат — пайп может освободить её байты.</summary>
    public void Dispose() => window.Release(end);
}

/// <summary>Сравнивает записи чанков только по sort key (побайтово).</summary>
sealed class ChunkLineComparer : IComparer<ChunkLine>
{
    public static readonly ChunkLineComparer Instance = new();

    public int Compare(ChunkLine x, ChunkLine y) => x.SortKey.SequenceCompareTo(y.SortKey);
}
