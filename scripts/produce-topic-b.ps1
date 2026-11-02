param (
    [int]$EPS = 10,
    [int]$DurationSeconds = 5,
    [int]$EventSizeBytes = 256,
    [int]$TotalCount = 0,
    [string]$BootstrapServer = "localhost:9092"
)

$topic = "topic_b"

if ($TotalCount -le 0) {
    $TotalCount = $EPS * $DurationSeconds
}

Write-Host "======================================================================" -ForegroundColor Cyan
Write-Host "  PRODUCER TOPIC B (MME Core Events)" -ForegroundColor Cyan
Write-Host "  Target EPS         : $EPS events/sec" -ForegroundColor Yellow
Write-Host "  Target Duration    : $DurationSeconds seconds" -ForegroundColor Yellow
Write-Host "  Target Event Size  : $EventSizeBytes bytes/event" -ForegroundColor Yellow
Write-Host "  Target Total Count : $TotalCount events" -ForegroundColor Yellow
Write-Host "======================================================================" -ForegroundColor Cyan

$startTime = [System.Diagnostics.Stopwatch]::StartNew()

$mmes = @("MME_01", "MME_02", "MME_03", "MME_04")
$tais = @("Chennai_125", "Mumbai_402", "Delhi_711", "Bangalore_330", "Hyderabad_821")
$procedures = @("Attach Request", "Service Request", "Tracking Area Update", "Handover Request")

$eventsList = [System.Collections.Generic.List[string]]::new()

for ($i = 0; $i -lt $TotalCount; $i++) {
    # Generate unique IMSI per event (404450000000001, 404450000000002...) for 1-to-1 state correlation
    $imsi = "40445" + ($i + 1).ToString("D10")
    $mme = $mmes[$i % $mmes.Count]
    $tai = $tais[$i % $tais.Count]
    $proc = $procedures[$i % $procedures.Count]
    $baseStr = "Timestamp: 10:15:33.$($i % 999) IMSI: $imsi Procedure: $proc MME_ID: $mme TAI: $tai Result: Success"
    
    # Pad payload to match requested EventSizeBytes
    $currentBytes = [System.Text.Encoding]::UTF8.GetByteCount($baseStr)
    if ($EventSizeBytes -gt $currentBytes) {
        $padLength = $EventSizeBytes - $currentBytes - 10
        if ($padLength -gt 0) {
            $padding = "B" * $padLength
            $baseStr += " Padding: $padding"
        }
    }
    
    $eventsList.Add($baseStr)

    # Inject 1 uncommon non-matching event for every 100 common events (100:1 ratio)
    if (($i + 1) % 100 -eq 0) {
        $uncommonImsi = "888888" + ($i + 1).ToString("D9")
        $uncommonStr = "Timestamp: 10:15:33.$($i % 999) IMSI: $uncommonImsi Procedure: $proc MME_ID: $mme TAI: $tai Result: Success"
        $eventsList.Add($uncommonStr)
    }
}

# Stream all events into Kafka console producer
$eventsList | docker exec -i kafka kafka-console-producer --bootstrap-server $BootstrapServer --topic $topic

if ($LASTEXITCODE -ne 0) {
    Write-Host "[ERROR] Failed to produce events to '$topic' (Exit Code: $LASTEXITCODE)." -ForegroundColor Red
    exit $LASTEXITCODE
}

$startTime.Stop()
$elapsedSec = [math]::Max(0.001, $startTime.Elapsed.TotalSeconds)
$effectiveEps = [math]::Round($TotalCount / $elapsedSec, 2)

Write-Host "[SUCCESS] Produced $TotalCount MME events (~$EventSizeBytes bytes each) to '$topic' in $([math]::Round($elapsedSec, 2))s (Effective: $effectiveEps EPS)" -ForegroundColor Green
