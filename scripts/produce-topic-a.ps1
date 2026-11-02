param (
    [int]$EPS = 10,
    [int]$DurationSeconds = 5,
    [int]$EventSizeBytes = 256,
    [int]$TotalCount = 0,
    [string]$BootstrapServer = "localhost:9092"
)

$topic = "topic_a"

if ($TotalCount -le 0) {
    $TotalCount = $EPS * $DurationSeconds
}

Write-Host "======================================================================" -ForegroundColor Cyan
Write-Host "  PRODUCER TOPIC A (RAN Measurement Reports)" -ForegroundColor Cyan
Write-Host "  Target EPS         : $EPS events/sec" -ForegroundColor Yellow
Write-Host "  Target Duration    : $DurationSeconds seconds" -ForegroundColor Yellow
Write-Host "  Target Event Size  : $EventSizeBytes bytes/event" -ForegroundColor Yellow
Write-Host "  Target Total Count : $TotalCount events" -ForegroundColor Yellow
Write-Host "======================================================================" -ForegroundColor Cyan

$startTime = [System.Diagnostics.Stopwatch]::StartNew()

$cells = @("LTE_5123", "LTE_8010", "LTE_3012", "LTE_4020", "LTE_5155", "LTE_5099", "LTE_5068", "LTE_5127")
$eventsList = [System.Collections.Generic.List[string]]::new()

for ($i = 0; $i -lt $TotalCount; $i++) {
    # Generate unique IMSI per event (404450000000001, 404450000000002...) for 1-to-1 state correlation
    $imsi = "40445" + ($i + 1).ToString("D10")
    $cell = $cells[$i % $cells.Count]
    $rsrp = -70 - ($i % 45)
    $rsrq = -8 - ($i % 12)
    $baseStr = "Timestamp: 10:15:34.$($i % 999) UE: IMSI $imsi Cell: $cell Event: Measurement Report RSRP: $rsrp dBm RSRQ: $rsrq dB Neighbor Cell: LTE_$($i + 5000)"
    
    # Pad payload to match requested EventSizeBytes
    $currentBytes = [System.Text.Encoding]::UTF8.GetByteCount($baseStr)
    if ($EventSizeBytes -gt $currentBytes) {
        $padLength = $EventSizeBytes - $currentBytes - 10
        if ($padLength -gt 0) {
            $padding = "A" * $padLength
            $baseStr += " Padding: $padding"
        }
    }
    
    $eventsList.Add($baseStr)

    # Inject 1 uncommon non-matching event for every 100 common events (100:1 ratio)
    if (($i + 1) % 100 -eq 0) {
        $uncommonImsi = "999999" + ($i + 1).ToString("D9")
        $uncommonStr = "Timestamp: 10:15:34.$($i % 999) UE: IMSI $uncommonImsi Cell: $cell Event: Measurement Report RSRP: $rsrp dBm RSRQ: $rsrq dB Neighbor Cell: LTE_$($i + 5000)"
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

Write-Host "[SUCCESS] Produced $TotalCount RAN events (~$EventSizeBytes bytes each) to '$topic' in $([math]::Round($elapsedSec, 2))s (Effective: $effectiveEps EPS)" -ForegroundColor Green
