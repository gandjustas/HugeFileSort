
if ((args?.Length ?? 0) == 0)
{
    Console.WriteLine($"Usage: {Path.GetFileNameWithoutExtension(Environment.ProcessPath)} <output file> [<source file> [<file size to generate>]]");
    return -1;
}

var file = args![0];
var sourceFile = args.Length > 1 ? args[1] : "source.txt";
var maxSize = args.Length > 2 ? long.Parse(args[2]) : (10L * 1024 * 1024 * 1024);

var source = (from l in File.ReadLines("source.txt")
              where !string.IsNullOrEmpty(l)
              from s in l.Split(new[] { '.', '?', '!', '[', ']' }, StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
              where s.Length > 10
              select s).ToList();

Random rand = new();

using (var f = File.CreateText(file))
{
    f.AutoFlush = false;
    while(f.BaseStream.Position < maxSize)
    {
        var n = rand.Next();
        f.Write(n);
        f.Write(". ");
        f.WriteLine(source[rand.Next(source.Count)]);
    }
}
return 0;
