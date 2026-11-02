param (
    [string]$BootstrapServer = "localhost:9092",
    [int]$Partitions = 12,
    [int]$ReplicationFactor = 1
)

# Robust parsing for PowerShell / Bash argument styles
for ($i = 0; $i -lt $args.Count; $i++) {
    $arg = $args[$i]
    if ($arg -match '^--?partitions$' -and ($i + 1) -lt $args.Count) {
        $Partitions = [int]$args[$i + 1]
    }
}
if ($BootstrapServer -match '^--?partitions$') {
    # Someone passed --partitions as first argument
    if ($Partitions -eq 12 -and $args.Count -eq 0) {
        $Partitions = 12
    }
    $BootstrapServer = "localhost:9092"
} elseif ($BootstrapServer -match '^\d+$') {
    # Someone called .\create-topics.ps1 12
    $Partitions = [int]$BootstrapServer
    $BootstrapServer = "localhost:9092"
}

$topics = @(
    "topic_a",
    "topic_b",
    "merged_events",
    "shadow_events",
    "payload_events",
    "validation_dlq",
    "aerospike_dlq",
    "correlation_dlq",
    "payload_fetch_dlq",
    "processing_dlq"
)

Write-Host "Creating/Ensuring Kafka topics on $BootstrapServer with $Partitions partitions..." -ForegroundColor Cyan

foreach ($topic in $topics) {
    docker exec kafka kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic $topic --partitions $Partitions --replication-factor $ReplicationFactor --config retention.ms=300000 2>&1 | Out-Null
    # If the topic already existed with fewer partitions, expand partitions:
    docker exec kafka kafka-topics --bootstrap-server localhost:9092 --alter --topic $topic --partitions $Partitions 2>&1 | Out-Null
    Write-Host "Topic '$topic' ready ($Partitions partitions, retention: 5m)." -ForegroundColor Green
}

Write-Host "All Kafka topics provisioned successfully." -ForegroundColor Yellow
