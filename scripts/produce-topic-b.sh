#!/usr/bin/env bash
BOOTSTRAP_SERVER=${1:-"localhost:9092"}
TOPIC="topic_b"

echo "Producing sample events to ${TOPIC}..."

# Detect docker executable (native docker or docker.exe for WSL)
DOCKER_CMD=""
if docker ps >/dev/null 2>&1; then
  DOCKER_CMD="docker"
elif command -v docker.exe >/dev/null 2>&1 && docker.exe ps >/dev/null 2>&1; then
  DOCKER_CMD="docker.exe"
fi

if command -v kafka-console-producer.sh >/dev/null 2>&1; then
  PRODUCER_CMD=(kafka-console-producer.sh --bootstrap-server "${BOOTSTRAP_SERVER}")
elif command -v kafka-console-producer >/dev/null 2>&1; then
  PRODUCER_CMD=(kafka-console-producer --bootstrap-server "${BOOTSTRAP_SERVER}")
elif [ -n "${DOCKER_CMD}" ]; then
  PRODUCER_CMD=(${DOCKER_CMD} exec -i kafka kafka-console-producer --bootstrap-server localhost:9092 --topic "${TOPIC}")
else
  echo "Error: kafka-console-producer not found locally and Docker container 'kafka' is not running." >&2
  exit 1
fi

cat <<EOF | "${PRODUCER_CMD[@]}"
{"event_id":"B-9001","common_id":"ORDER-101","created_at":"2026-09-02T12:00:30Z","details":{"status":"CONFIRMED","payment_method":"CREDIT_CARD"}}
{"event_id":"B-9002","common_id":"ORDER-102","created_at":"2026-09-02T12:01:15Z","details":{"status":"SHIPPED","carrier":"DHL"}}
{"event_id":"B-9003","common_id":"ORDER-103","created_at":"2026-09-02T12:02:10Z","details":{"status":"PENDING","note":"Rush delivery"}}
EOF

echo "Topic B events produced."
