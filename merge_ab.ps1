param(
    [string]$ChunkList,
    [string]$OutFile,
    [string]$LogFile
)
# Один прогон merge: замер wall-time и пика числа потоков процесса.
$sw = [System.Diagnostics.Stopwatch]::StartNew()
$psi = [System.Diagnostics.ProcessStartInfo]::new()
$psi.FileName = "dotnet"
$psi.Arguments = "src\bin\Release\net10.0\huge-file.dll merge `"$OutFile`""
$psi.RedirectStandardInput = $true
$psi.UseShellExecute = $false
$p = [System.Diagnostics.Process]::Start($psi)
foreach ($line in Get-Content $ChunkList) { $p.StandardInput.WriteLine($line) }
$p.StandardInput.Close()

$peak = 0
while (-not $p.HasExited) {
    Start-Sleep -Milliseconds 500
    try { $peak = [Math]::Max($peak, (Get-Process -Id $p.Id).Threads.Count) } catch { }
}
$sw.Stop()
"$([int]$sw.Elapsed.TotalSeconds)s, peak threads: $peak, exit: $($p.ExitCode)" | Tee-Object -FilePath $LogFile
