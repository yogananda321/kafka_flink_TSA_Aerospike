param (
    [string]$BootstrapServer = "localhost:9092",
    [switch]$LatestOnly
)

$topic = "merged_events"
Write-Host "Consuming joined merged events from $topic..." -ForegroundColor Cyan
if ($LatestOnly) {
    docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic $topic --property print.key=true
} else {
    docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic $topic --property print.key=true --from-beginning
}
