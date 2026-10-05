using System.Text;

namespace HugeFileSort.Tests;

/// <summary>
/// Детерминированный генератор входных файлов: строки «число. текст», завершённые
/// Environment.NewLine (как парсит split). Пул из 30 тысяч текстов с сильными дублями —
/// проверяет группы мержа; часть строк уникальна.
/// </summary>
static class TestData
{
    const string Alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-";

    /// <summary>Пишет файл до заданного размера, возвращает фактическую длину в байтах.</summary>
    public static async Task<long> GenerateAsync(string path, long sizeBytes, CancellationToken ct)
    {
        var random = new Random(20261005);
        var texts = new string[30_000];
        for (var i = 0; i < texts.Length; i++)
            texts[i] = RandomText(random, random.Next(1, 200));

        // Без BOM: продуктовый парсер читает сырые байты, как пишет команда generate.
        // Диспоз до чтения длины: у StreamWriter есть буфер, и без закрытия файла длина занижена.
        await using (var writer = new StreamWriter(path, append: false, new UTF8Encoding(encoderShouldEmitUTF8Identifier: false)))
        {
            long written = 0;
            while (written < sizeBytes)
            {
                var number = random.NextInt64(0, 1_000_000_000_000);
                var text = random.Next(10) < 7
                    ? texts[random.Next(texts.Length)]
                    : RandomText(random, random.Next(1, 200));
                var line = $"{number}. {text}";
                await writer.WriteLineAsync(line.AsMemory(), ct);
                written += Encoding.UTF8.GetByteCount(line) + Encoding.UTF8.GetByteCount(Environment.NewLine);
            }
        }
        return new FileInfo(path).Length;
    }

    static string RandomText(Random random, int length)
    {
        var chars = new char[length];
        for (var i = 0; i < length; i++)
            chars[i] = Alphabet[random.Next(Alphabet.Length)];
        return new string(chars);
    }
}
