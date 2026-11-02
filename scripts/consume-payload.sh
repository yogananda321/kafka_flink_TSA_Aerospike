#!/usr/bin/env bash
BOOTSTRAP_SERVER=${1:-"localhost:9092"}
TOPIC="payload_events"

echo "Reading full raw payload events from ${TOPIC}..."

# Detect docker executable (native docker or docker.exe for WSL)
DOCKER_CMD=""
if docker ps >/dev/null 2>&1; then
  DOCKER_CMD="docker"
elif command -v docker.exe >/dev/null 2>&1 && docker.exe ps >/dev/null 2>&1; then
  DOCKER_CMD="docker.exe"
fi

if command -v kafka-console-consumer.sh >/dev/null 2>&1; then
  kafka-console-consumer.sh --bootstrap-server "${BOOTSTRAP_SERVER}" --topic "${TOPIC}" --from-beginning --property print.key=true
elif command -v kafka-console-consumer >/dev/null 2>&1; then
  kafka-console-consumer --bootstrap-server "${BOOTSTRAP_SERVER}" --topic "${TOPIC}" --from-beginning --property print.key=true
elif [ -n "${DOCKER_CMD}" ]; then
  ${DOCKER_CMD} exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic "${TOPIC}" --from-beginning --property print.key=true
else
  echo "Error: kafka-console-consumer not found locally and Docker container 'kafka' is not running." >&2
  exit 1
fi
