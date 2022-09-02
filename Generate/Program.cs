
const string TextCharacters = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
const int DefaultCount = 10_000_000;
const int MinStringLength = 16;
const int MaxStringLength = 256;

if ((args?.Length ?? 0) == 0)
{
    Console.WriteLine($"Usage: {Path.GetFileNameWithoutExtension(Environment.ProcessPath)} <output file path> [<lines to generate>]");
    return -1;
}

var file = args![0];
var count = args.Length > 1 ? int.Parse(args[1]) : DefaultCount;
var maxStringLength = args.Length > 2 ? int.Parse(args[2]) : MaxStringLength;
var minStringLength = args.Length > 3 ? int.Parse(args[3]) : MinStringLength;

using (var f = File.CreateText(file))
    Generate(f, count, minStringLength, maxStringLength);
return 0;



void Generate(StreamWriter output, int count, int minStringLength, int maxStringLength)
{
    Random rand = new();
    for (int i = 0; i < count; i++)
    {
        var n = rand.Next();
        output.Write(n);
        output.Write(". ");
        var stringLength = rand.Next(minStringLength, maxStringLength);

        for (int j = 0; j < stringLength; j++)
        {
            if (j > 0 && j < stringLength-1 && rand.Next(8) == 0)
            {
                output.Write(' ');
            }
            else
            {
                output.Write(TextCharacters[rand.Next(TextCharacters.Length)]);
            }
        }
        output.WriteLine();
    }
}