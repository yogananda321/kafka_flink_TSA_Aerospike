param (
    [Parameter(Position=0)][int]$EPS = 0,
    [Parameter(Position=1)][int]$DurationSeconds = 0,
    [Parameter(Position=2)][int]$EventSizeBytes = 0,
    [Parameter(Position=3)][int]$LatencyMs = -1,
    [Parameter(Position=4)][string]$BootstrapServer = "kafka:29092"
)

Write-Host "======================================================================" -ForegroundColor Cyan
Write-Host "  ENTERPRISE FLINK PIPELINE - NATIVE HIGH-THROUGHPUT BENCHMARK RUNNER" -ForegroundColor Cyan
Write-Host "======================================================================" -ForegroundColor Cyan

if ($EPS -le 0) {
    $pEps = Read-Host 'Enter target EPS per producer [default: 5000]'
    if ([string]::IsNullOrWhiteSpace($pEps)) { $EPS = 5000 } else { $EPS = [int]($pEps -replace '[^\d]', '') }
}

if ($DurationSeconds -le 0) {
    $pDur = Read-Host 'Enter test duration in seconds [default: 30]'
    if ([string]::IsNullOrWhiteSpace($pDur)) { $DurationSeconds = 30 } else { $DurationSeconds = [int]($pDur -replace '[^\d]', '') }
}

if ($EventSizeBytes -le 0) {
    $pSize = Read-Host 'Enter event payload size in bytes [default: 1024]'
    if ([string]::IsNullOrWhiteSpace($pSize)) { $EventSizeBytes = 1024 } else { $EventSizeBytes = [int]($pSize -replace '[^\d]', '') }
}

if ($LatencyMs -lt 0) {
    $pLat = Read-Host 'Enter production latency in ms (randomized 10ms - N ms) [default: 0]'
    if ([string]::IsNullOrWhiteSpace($pLat)) { $LatencyMs = 0 } else { $LatencyMs = [int]($pLat -replace '[^\d]', '') }
}

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path

# Ensure Kafka topics exist
Write-Host "[*] Checking Kafka topics..." -ForegroundColor Yellow
$topicCheck = (docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list 2>&1 | Out-String)
if ($topicCheck -notmatch "merged_events") {
    Write-Host "[!] Kafka topics not found. Automatically provisioning topics..." -ForegroundColor Magenta
    & (Join-Path $scriptDir "create-topics.ps1")
} else {
    Write-Host "[OK] Kafka topics verified." -ForegroundColor Green
}

# Ensure Flink Job is running
Write-Host "[*] Checking Flink streaming application..." -ForegroundColor Yellow
$flinkList = (docker exec flink-jobmanager ./bin/flink list 2>&1 | Out-String)
if ($flinkList -notmatch "flink-dual-topic-event-correlation") {
    Write-Host "[!] Flink job not running. Submitting pipeline..." -ForegroundColor Magenta
    docker exec flink-jobmanager ./bin/flink run -d -c com.company.flink.Application /opt/flink/project/target/flink-event-correlation-1.0.0.jar --kafka.bootstrap.servers kafka:29092 --aerospike.hosts aerospike:3000
    Start-Sleep -Seconds 5
} else {
    Write-Host "[OK] Flink correlation pipeline is running." -ForegroundColor Green
}

Write-Host "`n[*] Starting Multi-Core Benchmark Runner ($EPS EPS, ${DurationSeconds}s, ${EventSizeBytes} bytes, ${LatencyMs}ms max random latency)..." -ForegroundColor Yellow

docker exec flink-jobmanager java -cp /opt/flink/project/target/flink-event-correlation-1.0.0.jar com.company.flink.util.HighThroughputBenchmarkProducer $EPS $DurationSeconds $EventSizeBytes $BootstrapServer $LatencyMs
