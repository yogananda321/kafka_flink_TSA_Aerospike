param (
    [int]$EPS = 100,
    [int]$DurationSeconds = 60,
    [int]$EventSizeBytes = 1024,
    [string]$Latency = "random"
)

Write-Host "======================================================================" -ForegroundColor Cyan
Write-Host "  APPROACH 1: FLINK + ROCKSDB + AEROSPIKE BENCHMARK RUNNER" -ForegroundColor Cyan
Write-Host "======================================================================" -ForegroundColor Cyan

if ($EPS -gt 0) {
    # Run a single targeted benchmark run
    Write-Host "[*] Executing Single Benchmark Test: $EPS EPS/topic, Duration: ${DurationSeconds}s, Size: ${EventSizeBytes}B, Latency: $Latency" -ForegroundColor Yellow
    docker exec -i flink-jobmanager java -cp /opt/flink/project/target/flink-event-correlation-1.0.0.jar com.company.flink.util.HighThroughputBenchmarkProducer $EPS $DurationSeconds $EventSizeBytes $Latency kafka:29092
} else {
    # Run the complete automated empirical benchmark matrix
    Write-Host "[*] Executing Full Approach 1 Matrix (100, 500, 1000, 2000, 5000, 10000 EPS/topic)..." -ForegroundColor Yellow
    python scripts\run_approach1_benchmark.py
}
