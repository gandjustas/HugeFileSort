using DotMake.CommandLine;
using System.Globalization;
using System.Text;

Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);

return await Cli.RunAsync<RootCliCommand>(args);


[CliCommand(Description = "Huge file sorter")]
partial class RootCliCommand
{
    [CliOption(Name = "encoding", Recursive = true, Description = "Text encoding of the file(s) (default: utf-8)")]
    public string EncodingName { get; set; } = "utf-8";

    /// <summary>
    /// Разбирает общие настройки (кодировка — рекурсивная опция с root-команды, опции сравнения — из подкоманды)
    /// и запускает действие; при невалидных значениях печатает ошибку и возвращает 1.
    /// </summary>
    internal static Task<int> RunWithSettingsAsync(string compareOptionsName, Func<Encoding, CompareOptions, CancellationToken, Task<int>> run, CliContext context)
    {
        // Рекурсивная опция объявлена на RootCliCommand; значение достаём из дерева разбора —
        // оно совпадает в любом месте командной строки (root или подкоманда).
        var encodingName = context.Result.ParseResult.GetValue<string>("--encoding") ?? "utf-8";
        if (!TryGetEncoding(encodingName, out var encoding))
        {
            Console.Error.WriteLine($"Unknown encoding: '{encodingName}'");
            return Task.FromResult(1);
        }

        if (!Enum.TryParse(compareOptionsName, true, out CompareOptions compareOptions))
        {
            Console.Error.WriteLine($"Invalid compare options: '{compareOptionsName}'");
            return Task.FromResult(1);
        }

        return run(encoding, compareOptions, context.CancellationToken);
    }

    private static bool TryGetEncoding(string name, out Encoding encoding)
    {
        try
        {
            encoding = Encoding.GetEncoding(name);
            return true;
        }
        catch (ArgumentException)
        {
            encoding = null!;
            return false;
        }
    }
}
