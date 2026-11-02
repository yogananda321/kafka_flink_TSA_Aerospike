#!/usr/bin/env bash
BOOTSTRAP_SERVER=${1:-"localhost:9092"}
TOPIC="topic_a"

echo "Producing sample events to ${TOPIC}..."

# Detect docker executable (native docker or docker.exe for WSL)
DOCKER_CMD=""
if docker ps >/dev/null 2>&1; then
  DOCKER_CMD="docker"
elif command -v docker.exe >/dev/null 2>&1 && docker.exe ps >/dev/null 2>&1; then
  DOCKER_CMD="docker.exe"
fi

if command -v kafka-console-producer.sh >/dev/null 2>&1; then
  PRODUCER_CMD=(kafka-console-producer.sh --bootstrap-server "${BOOTSTRAP_SERVER}" --topic "${TOPIC}")
elif command -v kafka-console-producer >/dev/null 2>&1; then
  PRODUCER_CMD=(kafka-console-producer --bootstrap-server "${BOOTSTRAP_SERVER}" --topic "${TOPIC}")
elif [ -n "${DOCKER_CMD}" ]; then
  PRODUCER_CMD=(${DOCKER_CMD} exec -i kafka kafka-console-producer --bootstrap-server localhost:9092 --topic "${TOPIC}")
else
  echo "Error: kafka-console-producer not found locally and Docker container 'kafka' is not running." >&2
  exit 1
fi

cat <<EOF | "${PRODUCER_CMD[@]}"
{"event_id":"A-1001","common_id":"ORDER-101","timestamp":"2026-09-02T12:00:00Z","data":{"amount":120.50,"currency":"USD"}}
{"event_id":"A-1002","common_id":"ORDER-102","timestamp":"2026-09-02T12:01:00Z","data":{"amount":450.00,"currency":"EUR"}}
{"event_id":"A-1003","common_id":"ORDER-103","timestamp":"2026-09-02T12:02:00Z","data":{"amount":89.99,"currency":"USD"}}
EOF

echo "Topic A events produced."
