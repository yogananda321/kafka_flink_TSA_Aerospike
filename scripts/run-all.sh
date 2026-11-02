#!/usr/bin/env bash
set -e

EPS=${1:-"100,500,1000,2000,5000"}
DURATION=${2:-30}
EVENT_SIZE=${3:-1024}
LATENCY=${4:-"random"}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUNNER_PATH="${SCRIPT_DIR}/benchmark_runner.py"

echo -e "\033[36m======================================================================\033[0m"
echo -e "\033[36m  APPROACH 1 BENCHMARK RUNNER (FLINK + ROCKSDB + AEROSPIKE)\033[0m"
echo -e "\033[36m  10 Core Metrics Telemetry Engine (100 - 5000 EPS | 30s Duration)\033[0m"
echo -e "\033[36m======================================================================\033[0m"
echo -e "\033[33mExecuting benchmark with: EPS=${EPS}, Duration=${DURATION}s, PayloadSize=${EVENT_SIZE}B, Latency=${LATENCY}\033[0m"

# Verify Kafka Topics
if ! docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list 2>/dev/null | grep -q "merged_events"; then
    echo "[!] Kafka topics not found. Provisioning..."
    "${SCRIPT_DIR}/create-topics.sh" localhost:9092 12 1
fi

# Verify Flink Job
if ! docker exec flink-jobmanager ./bin/flink list 2>/dev/null | grep -q "flink-dual-topic-event-correlation"; then
    echo "[!] Submitting Flink application..."
    docker exec flink-jobmanager ./bin/flink run -d -c com.company.flink.Application /opt/flink/project/target/flink-event-correlation-1.0.0.jar --kafka.bootstrap.servers kafka:29092 --aerospike.hosts aerospike:3000
    sleep 5
fi

PYTHON_CMD="python3"
if ! command -v python3 >/dev/null 2>&1; then
    if command -v python >/dev/null 2>&1; then
        PYTHON_CMD="python"
    else
        echo "Error: Python 3 is required." >&2
        exit 1
    fi
fi

${PYTHON_CMD} "${RUNNER_PATH}" --eps "${EPS}" --duration "${DURATION}" --size "${EVENT_SIZE}" --latency "${LATENCY}"
