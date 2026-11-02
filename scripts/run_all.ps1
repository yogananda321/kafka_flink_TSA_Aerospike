param (
    [Parameter(Position=0)][int]$EPS = 0,
    [Parameter(Position=1)][int]$DurationSeconds = 0,
    [Parameter(Position=2)][int]$EventSizeBytes = 0,
    [Parameter(Position=3)][int]$LatencyMs = -1,
    [Parameter(Position=4)][string]$BootstrapServer = "kafka:29092"
)

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
& (Join-Path $scriptDir "run-all.ps1") $EPS $DurationSeconds $EventSizeBytes $LatencyMs $BootstrapServer
