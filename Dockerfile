# ==============================================================================
# Multi-Stage Production Dockerfile for Apache Flink Custom Job
# ==============================================================================
FROM maven:3.9.6-eclipse-temurin-17 AS builder

WORKDIR /app
COPY pom.xml .
RUN mvn dependency:go-offline -B

COPY src ./src
RUN mvn clean package -DskipTests -B

# ------------------------------------------------------------------------------
# Flink Runtime Image
# ------------------------------------------------------------------------------
FROM flink:1.19.1-java17

RUN mkdir -p /opt/flink/usrlib /tmp/flink-checkpoints /tmp/flink-savepoints

# Copy built Uber-JAR into Flink's user library directory
COPY --from=builder /app/target/flink-event-correlation-1.0.0.jar /opt/flink/usrlib/flink-event-correlation.jar

# Custom entrypoint or default flink command
USER flink
