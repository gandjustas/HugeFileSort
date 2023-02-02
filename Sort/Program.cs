using System.Buffers.Binary;
using System.Globalization;
using System.IO;
using System.Reflection.PortableExecutable;

if ((args?.Length ?? 0) == 0)
{
    Console.WriteLine($"Usage: {Path.GetFileNameWithoutExtension(Environment.ProcessPath)} <file path> [<approximate chunk size>]");
    return -1;
}

var file = args![0];
var chunkSize = args?.Length > 1 ? (int.Parse(args[1]) * 1000_000 / 2) : 100_000_000; //In characters

var comparer = new Comparer();
var culture = CultureInfo.CurrentCulture;

var sw = System.Diagnostics.Stopwatch.StartNew();
List<string> tempFiles = new();
List<Item> chunk = new();
using (var reader = File.OpenText(file))
{
    var keyBuffer = new byte[chunkSize * 2]; //Буфер для ключей
    var chunkBuffer = new char[chunkSize];
    var chunkReadPosition = 0;
    var eos = reader.EndOfStream;
    while (!eos)
    {
        // Читаем из файла весь буфер
        var charsRead = reader.ReadBlock(chunkBuffer.AsSpan(chunkReadPosition));
        eos = reader.EndOfStream;
        var m = chunkBuffer.AsMemory(0, chunkReadPosition + charsRead);
        var key = keyBuffer.AsMemory();

        // Заполняем список строк ReadOnlyMemory<char> для сортировки
        int linePos;
        while ((linePos = m.Span.IndexOf(Environment.NewLine)) >= 0 || (eos && m.Length > 0))
        {
            var line = linePos >= 0 ? m[..linePos] : m;
            var s = line.Span;
            var dot = line.Span.IndexOf('.');
            int x = int.Parse(s[..dot]);
            s = s[(dot + 2)..];
            var keyLen = culture.CompareInfo.GetSortKey(s, key.Span);    // Получаем ключ
            BinaryPrimitives.WriteInt32BigEndian(key[keyLen..].Span, x); // Добписываем число в конец ключа, чтобы старшый байт был с меньшим индексом
            keyLen += sizeof(int);

            chunk.Add(new Item(line, key[..keyLen]));
            m = m[(linePos + Environment.NewLine.Length)..];
            key = key[keyLen..];
        }

        chunk.Sort(comparer);

        // Записываем строки из отсортированного списка во временный файл
        var tempFileName = Path.ChangeExtension(file, $".part-{tempFiles.Count}" + Path.GetExtension(file));
        using (var tempFile = File.CreateText(tempFileName))
        {
            foreach (var (l, _) in chunk)
            {
                tempFile.WriteLine(l);
            }
        }
        tempFiles.Add(tempFileName);

        if (eos) break;
        chunk.Clear();

        //Отсток буфера переносим в начало
        m.CopyTo(chunkBuffer);
        chunkReadPosition = m.Length;
    }
}

Console.WriteLine($"SplitSort done in {sw.Elapsed}");
sw.Restart();

try
{
    var mergedLines = tempFiles
        .Select(f => File.ReadLines(f).Select(s => // Читаем построчно все файлы 
        {
            var m = s.AsMemory();
            var dot = s.IndexOf('.');              // Находим в строках точку
            int x = int.Parse(s.AsSpan(0, dot));
            var key = new byte[s.Length * 2 + sizeof(int)];
            var keyLen = culture.CompareInfo.GetSortKey(m[(dot+2)..].Span, key); // Получаем ключ того, что находится после точки с пробелом
            BinaryPrimitives.WriteInt32BigEndian(key.AsSpan(keyLen), x);         // Доисываем число в конец
            return new Item(m, key);
        }))
        .Merge(comparer);  //Слияние итераторов IEnumerable<IEnumerable<T>> в IEnumerable<T>
    using var sortedFile = File.CreateText(Path.ChangeExtension(file, ".sorted" + Path.GetExtension(file)));

    foreach (var (l, _) in mergedLines)
    {
        sortedFile.WriteLine(l);
    }
}
finally
{
    tempFiles.ForEach(File.Delete);
}
Console.WriteLine($"Merge done in {sw.Elapsed}");

return 0;

public record struct Item(ReadOnlyMemory<char> Line, ReadOnlyMemory<byte> Key);
public class Comparer : IComparer<Item>
{
    public int Compare(Item x, Item y)
    {
        return x.Key.Span.SequenceCompareTo(y.Key.Span);
    }
}