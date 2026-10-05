param([string]$OutFile, [int]$IntervalSec = 2)
"timestamp,disk_read_bps,disk_write_bps,disk_idle_pct,disk_time_pct,cpu_pct" | Out-File -FilePath $OutFile -Encoding utf8
while ($true) {
    try {
        $t = (Get-Date).ToString('yyyy-MM-dd HH:mm:ss')
        $d = Get-CimInstance Win32_PerfFormattedData_PerfDisk_PhysicalDisk -ErrorAction Stop | Where-Object { $_.Name -like '*F:' } | Select-Object -First 1
        $c = Get-CimInstance Win32_PerfFormattedData_PerfOS_Processor -ErrorAction Stop | Where-Object { $_.Name -eq '_Total' } | Select-Object -First 1
        if ($d -and $c) {
            $line = "{0},{1},{2},{3},{4},{5}" -f $t, $d.DiskReadBytesPersec, $d.DiskWriteBytesPersec, $d.PercentIdleTime, $d.PercentDiskTime, $c.PercentProcessorTime
            Add-Content -Path $OutFile -Value $line -Encoding utf8
        }
    } catch { }
    Start-Sleep -Seconds $IntervalSec
}
