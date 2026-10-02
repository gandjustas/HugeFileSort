using System.Text;
using DotMake.CommandLine;

Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);

return await Cli.RunAsync<RootCliCommand>(args);
