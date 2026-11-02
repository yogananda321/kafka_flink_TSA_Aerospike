#!/usr/bin/env bash
set -e

BOOTSTRAP_SERVER=${1:-"localhost:9092"}
PARTITIONS=${2:-12}
REPLICATION=${3:-1}

TOPICS=(
  "topic_a"
  "topic_b"
  "merged_events"
  "shadow_events"
  "payload_events"
  "validation_dlq"
  "aerospike_dlq"
  "correlation_dlq"
  "payload_fetch_dlq"
  "processing_dlq"
)

# Detect docker executable (native docker or docker.exe for WSL)
DOCKER_CMD=""
if docker ps >/dev/null 2>&1; then
  DOCKER_CMD="docker"
elif command -v docker.exe >/dev/null 2>&1 && docker.exe ps >/dev/null 2>&1; then
  DOCKER_CMD="docker.exe"
fi

if command -v kafka-topics.sh >/dev/null 2>&1; then
  KAFKA_BIN=(kafka-topics.sh --bootstrap-server "${BOOTSTRAP_SERVER}")
elif command -v kafka-topics >/dev/null 2>&1; then
  KAFKA_BIN=(kafka-topics --bootstrap-server "${BOOTSTRAP_SERVER}")
elif [ -n "${DOCKER_CMD}" ]; then
  KAFKA_BIN=(${DOCKER_CMD} exec kafka kafka-topics --bootstrap-server localhost:9092)
else
  echo "Error: Neither 'kafka-topics.sh' nor a running Docker container named 'kafka' was found." >&2
  exit 1
fi

echo "Creating Kafka topics on ${BOOTSTRAP_SERVER} with ${PARTITIONS} partitions..."

for topic in "${TOPICS[@]}"; do
  "${KAFKA_BIN[@]}" \
    --create --if-not-exists \
    --topic "${topic}" \
    --partitions "${PARTITIONS}" \
    --replication-factor "${REPLICATION}" \
    --config retention.ms=300000
  echo "Topic '${topic}' ready (retention: 5m)."
done

echo "All topics successfully provisioned."
