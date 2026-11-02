param (
    [string]$BootstrapServer = "localhost:9092"
)

$topic = "shadow_events"
Write-Host "Consuming lightweight shadow events from $topic..." -ForegroundColor Cyan
docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic $topic --property print.key=true
