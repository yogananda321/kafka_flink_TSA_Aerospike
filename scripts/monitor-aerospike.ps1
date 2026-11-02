# Real-time Aerospike Read & Write Live Telemetry Monitor
param(
    [int]$IntervalSeconds = 1
)

Write-Host "==================================================================" -ForegroundColor Cyan
Write-Host "   AEROSPIKE REAL-TIME READ & WRITE LIVE MONITOR (1-sec refresh)  " -ForegroundColor Cyan
Write-Host "==================================================================" -ForegroundColor Cyan
Write-Host "Press Ctrl+C to stop monitoring.`n" -ForegroundColor DarkGray

function Get-AeroStats {
    $raw = docker exec aerospike asinfo -v "namespace/test" 2>$null
    $writes = 0
    $reads = 0
    $objects = 0
    if ($raw -match 'client_write_success=(\d+)') { $writes = [int64]$Matches[1] }
    if ($raw -match 'client_read_success=(\d+)')  { $reads  = [int64]$Matches[1] }
    if ($raw -match 'objects=(\d+)')              { $objects = [int64]$Matches[1] }
    return [PSCustomObject]@{
        Writes = $writes
        Reads = $reads
        Objects = $objects
        Timestamp = (Get-Date)
    }
}

$prev = Get-AeroStats

while ($true) {
    Start-Sleep -Seconds $IntervalSeconds
    $curr = Get-AeroStats
    $elapsed = ($curr.Timestamp - $prev.Timestamp).TotalSeconds
    if ($elapsed -le 0) { $elapsed = 1 }

    $writeRate = [math]::Round(($curr.Writes - $prev.Writes) / $elapsed)
    $readRate  = [math]::Round(($curr.Reads - $prev.Reads) / $elapsed)

    $timeStr = $curr.Timestamp.ToString("HH:mm:ss")

    $writeColor = if ($writeRate -gt 0) { "Green" } else { "DarkGray" }
    $readColor  = if ($readRate -gt 0) { "Yellow" } else { "DarkGray" }

    Write-Host "[$timeStr] " -NoNewline -ForegroundColor Cyan
    Write-Host "Writes/sec: " -NoNewline
    Write-Host ("{0,6:N0}" -f $writeRate) -NoNewline -ForegroundColor $writeColor
    Write-Host " ops/s | " -NoNewline
    Write-Host "Reads/sec: " -NoNewline
    Write-Host ("{0,6:N0}" -f $readRate) -NoNewline -ForegroundColor $readColor
    Write-Host " ops/s | " -NoNewline
    Write-Host "Total Writes: " -NoNewline
    Write-Host ("{0:N0}" -f $curr.Writes) -NoNewline -ForegroundColor Gray
    Write-Host " | Total Reads: " -NoNewline
    Write-Host ("{0:N0}" -f $curr.Reads) -NoNewline -ForegroundColor Gray
    Write-Host " | Stored Objects: " -NoNewline
    Write-Host ("{0:N0}" -f $curr.Objects) -ForegroundColor White

    $prev = $curr
}
