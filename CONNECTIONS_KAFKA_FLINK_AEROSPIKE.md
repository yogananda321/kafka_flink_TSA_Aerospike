# Kafka-to-Flink & Flink-to-Aerospike Connection Architecture

## Detailed Interface, Protocol, Pooling, and Network Specification

---

## 1. Network Topology & Port Mappings

All services communicate over the Docker bridge network named `flink-net`. Containers resolve each other using Docker's internal embedded DNS server.

```
+-----------------------------------------------------------------------------------------------+
|                                    DOCKER BRIDGE: flink-net                                   |
|                                                                                               |
|   +--------------------------+                                  +-------------------------+   |
|   |          kafka           |                                  |        aerospike        |   |
|   |  (confluentinc/cp-kafka) |                                  | (aerospike/aerospike)   |   |
|   |                          |                                  |                         |   |
|   |  Internal: kafka:29092   |                                  |  Internal:              |   |
|   |  External: :9092         |                                  |  Service: aerospike:3000|   |
|   +------------▲-------------+                                  +------------▲------------+   |
|                │                                                             │                |
|                │ [1] Consume Raw (29092)                                     │ [3] Put Payload|
|                │ [5] Produce Merged & DLQs (29092)                           │ [4] Multi-Get  |
|                │                                                             │     Payload    |
|   +------------▼-------------------------------------------------------------▼------------+   |
|   |                                  FLINK CLUSTER                                        |   |
|   |                                                                                       |   |
|   |   flink-jobmanager                               flink-taskmanager (x2)               |   |
|   |   - RPC: flink-jobmanager:6123                   - Slots: 4 per TM (8 total)          |   |
|   |   - Blob: :6124                                  - Data Port: :6122                   |   |
|   |   - Web UI: :8081                                - AerospikeClientProvider (Singleton)|   |
|   +---------------------------------------------------------------------------------------+   |
+-----------------------------------------------------------------------------------------------+
```

### Port Matrix

| Host Port | Container Port | Protocol / Listener | Container Hostname | Traffic Flow |
| :--- | :--- | :--- | :--- | :--- |
| `9092` | `9092` | `PLAINTEXT_HOST` | `kafka` | External Producers / Consumers (Host OS PowerShell & Bash scripts) |
| *None* | `29092` | `PLAINTEXT` | `kafka` | Internal high-speed broker access for Flink TaskManagers |
| `9093` | `9093` | `CONTROLLER` | `kafka` | Internal KRaft Raft consensus quorum |
| `3000` | `3000` | Aerospike Service | `aerospike` | TCP client connections for Flink `PayloadWriter` and `PayloadFetcher` |
| `8081` | `8081` | HTTP / REST | `flink-jobmanager`| Flink Dashboard, Metrics API, Job submission |
| *None* | `6123` | Akka / Netty RPC | `flink-jobmanager`| TaskManager registration, heartbeat, checkpoint coordination |

---

## 2. Kafka to Flink (Ingestion Layer)

### 2.1 Connection Mechanics & Ingress Lifecycle
The connection between Kafka and Flink is established using Flink's modernized, non-blocking `KafkaSource` API ([TopicASource.java](file:///c:/Users/2857155/Desktop/EOL_Final/src/main/java/com/company/flink/source/TopicASource.java) and [TopicBSource.java](file:///c:/Users/2857155/Desktop/EOL_Final/src/main/java/com/company/flink/source/TopicBSource.java)).

```
  Kafka (topic_a, topic_b)
             │
             │ [TCP Connection: kafka:29092]
             ▼
  Flink KafkaSource (SplitEnumerator in JobManager)
             │  - Periodically checks for new partitions
             │  - Assigns partition splits to TaskManager slots
             ▼
  Flink SourceReader (in TaskManager Task Slots)
             │  - Polls records using KafkaConsumer.poll()
             │  - Disables auto-commit (managed by Flink checkpoints)
             ▼
  SimpleStringSchema (Deserializes raw bytes -> Java String)
             │
             ▼
  DataStream<String>.rebalance() (Round-robins splits across all 8 slots)
```

### 2.2 Client Configuration Parameters

```java
// Source building snippet from TopicASource.java
KafkaSource.<String>builder()
    .setBootstrapServers("kafka:29092")
    .setTopics("topic_a")
    .setGroupId("flink-event-join-cg-a")
    .setStartingOffsets(OffsetsInitializer.earliest())
    .setValueOnlyDeserializer(new SimpleStringSchema())
    .setProperties(kafkaConfig.toConsumerProperties())
    .build();
```

| Parameter Name | Value Set | System Design Purpose |
| :--- | :--- | :--- |
| `bootstrap.servers` | `kafka:29092` | Direct internal networking inside `flink-net`. |
| `group.id` | `flink-event-join-cg-a` / `-b` | Isolated consumer groups per topic; avoids partition assignment contention. |
| `enable.auto.commit` | `false` | Completely disables Kafka background offset commits; offsets are committed to Kafka **only upon successful Flink checkpoint completion**. |
| `auto.offset.reset` | `earliest` | Reads from the beginning of partitions if no committed offset is stored. |
| `isolation.level` | `read_committed` | Guarantees Flink never reads uncommitted or aborted transactional messages. |
| `partition.discovery.interval.ms` | `-1` (disabled) | Fixed partition set (12 partitions pre-created per topic). |

### 2.3 Partition Redistribution (`rebalance`)
Because Kafka topics have 12 partitions and the Flink cluster provides 8 task slots, default partition assignment would leave some subtasks with 2 partitions and others with 1 partition. Invoking `.rebalance()` immediately following `env.fromSource(...)` enforces a round-robin network shuffle across all 8 TaskManager slots, evening out CPU load.

---

## 3. Flink to Kafka (Egress & DLQ Layer)

### 3.1 Connection Mechanics & Sinks
Flink connects to Kafka as a high-reliability producer via `KafkaSink` ([KafkaMergedEventSink.java](file:///c:/Users/2857155/Desktop/EOL_Final/src/main/java/com/company/flink/sink/KafkaMergedEventSink.java), [KafkaShadowEventSink.java](file:///c:/Users/2857155/Desktop/EOL_Final/src/main/java/com/company/flink/sink/KafkaShadowEventSink.java), [KafkaPayloadEventSink.java](file:///c:/Users/2857155/Desktop/EOL_Final/src/main/java/com/company/flink/sink/KafkaPayloadEventSink.java), [DLQSink.java](file:///c:/Users/2857155/Desktop/EOL_Final/src/main/java/com/company/flink/sink/DLQSink.java)).

```
  Flink Pipeline Operators (MergeFunction / DLQ Side Outputs)
             │
             │ Record emitted
             ▼
  KafkaRecordSerializationSchema
             │  - Key Serializer: UTF-8 Bytes of common_id (IMSI)
             │  - Value Serializer: UTF-8 Bytes of JSON string
             ▼
  Flink KafkaWriter (Internal Producer Pool)
             │  - Buffers up to 64KB (batch.size) or waits 20ms (linger.ms)
             │  - Compresses batches using Snappy
             ▼
  TCP Socket Connection -> kafka:29092
             │
             ▼
  Kafka Topics:
    - merged_events      (Final correlated output)
    - shadow_events      (Lightweight metadata stream)
    - payload_events     (Full raw payload audit stream)
    - validation_dlq     (Malformed syntax / Missing IMSI)
    - aerospike_dlq      (Database write errors)
    - correlation_dlq    (Orphan correlation timeouts)
    - payload_fetch_dlq  (Missing records in Aerospike)
    - processing_dlq     (Merge calculation failures)
```

### 3.2 Producer Configuration & Guarantees

```java
// Serialization and partition key assignment in KafkaMergedEventSink.java
new KafkaRecordSerializationSchema<MergedEvent>() {
    @Override
    public ProducerRecord<byte[], byte[]> serialize(MergedEvent element, KafkaSinkContext context, Long timestamp) {
        byte[] key = element.getCommonId() != null ? element.getCommonId().getBytes(StandardCharsets.UTF_8) : null;
        byte[] value = JsonUtil.toJson(element).getBytes(StandardCharsets.UTF_8);
        return new ProducerRecord<>("merged_events", key, value);
    }
}
```

| Producer Setting | Configured Value | Operational Impact |
| :--- | :--- | :--- |
| `acks` | `all` (-1) | The leader broker waits for in-sync replicas to acknowledge before confirming success. |
| `retries` | `10` | Automatic retry on transient broker leader re-elections or socket disconnects. |
| `max.in.flight.requests.per.connection` | `1` | Strictly preserves message order per partition during producer retries. |
| `compression.type` | `snappy` | Fast, low-CPU compression reducing wire bandwidth and storage footprint. |
| `batch.size` | `65536` (64 KB) | Batches individual records together into larger TCP payloads. |
| `linger.ms` | `20` | Waits up to 20ms for batches to fill before dispatching to Kafka. |
| **Record Key Strategy** | `common_id` (IMSI) | **Deterministic Routing**: Every event for the same subscriber hashes to the exact same Kafka partition, ensuring strictly ordered consumption for downstream systems. |

---

## 4. Flink to Aerospike (Payload Offloading / Write)

### 4.1 Connection Management & Lifecycle
Direct NoSQL connectivity is managed by [AerospikeClientProvider.java](file:///c:/Users/2857155/Desktop/EOL_Final/src/main/java/com/company/flink/aerospike/AerospikeClientProvider.java).

```
   Flink TaskManager JVM
  ┌────────────────────────────────────────────────────────────────────────┐
  │  AerospikeClientProvider (Thread-Safe Singleton)                       │
  │                                                                        │
  │   ClientPolicy:                                                        │
  │     - maxConnsPerNode: 300 connections                                 │
  │     - timeout: 2000 ms                                                 │
  │                                                                        │
  │   Shared Connection Pool: [Socket 1, Socket 2, ... Socket 300]         │
  └───────────────────▲────────────────────────────────▲───────────────────┘
                      │                                │
       [Slot 1: PayloadWriter]          [Slot 2: PayloadWriter]
                      │                                │
                      └────────────────┬───────────────┘
                                       │
                                       │ TCP :3000 (Binary Aerospike Protocol)
                                       ▼
                       Aerospike Cluster Node (aerospike:3000)
                       - Namespace: `test`
                       - Set: `event_payloads`
                       - Primary Key: SHA-256(sourceType + "::" + eventId)
```

### 4.2 Thread-Safe Singleton Pattern
Because Flink TaskManagers execute multiple parallel subtasks (slots) within a single JVM process, creating a new `AerospikeClient` per subtask would exhaust file descriptors and socket handles. `AerospikeClientProvider` uses double-checked locking to maintain a **single client instance per JVM**:

```java
public AerospikeClient getClient() {
    if (instance == null || !instance.isConnected()) {
        synchronized (AerospikeClientProvider.class) {
            if (instance == null || !instance.isConnected()) {
                instance = createClient(config);
            }
        }
    }
    return instance;
}
```

### 4.3 Write Policy & Consistency Boundary

```java
// Configuration inside PayloadWriter.java
this.writePolicy = new WritePolicy();
this.writePolicy.recordExistsAction = RecordExistsAction.UPDATE;
this.writePolicy.totalTimeout = 3000;       // 3 seconds total timeout
this.writePolicy.socketTimeout = 1000;      // 1 second socket timeout
this.writePolicy.expiration = 604800;       // 7 days TTL (seconds)
```

```
                        [ CommonEvent Arrives at PayloadWriter ]
                                           │
                                           ▼
                       Compute Deterministic SHA-256 payload_id
                                           │
                                           ▼
                       Execute client.put(recordKey, bins...)
                                           │
                         ┌─────────────────┴─────────────────┐
                         │                                   │
                    [ SUCCESS ]                         [ FAILURE ]
                         │                                   │
                         ▼                                   ▼
             Emit ShadowEvent Downstream            Retry up to 3 times
           (Gated Consistency Boundary)         (Sleep 100ms * attemptCount)
                                                             │
                                                  ┌──────────┴──────────┐
                                                  │                     │
                                            [ RECOVERED ]        [ EXHAUSTED ]
                                                  │                     │
                                                  ▼                     ▼
                                          Emit ShadowEvent       Emit DLQEvent
                                          to State Stream     to aerospike_dlq
```

**Consistency Boundary Rule**:
Under no circumstances is a `ShadowEvent` permitted to enter Flink's RocksDB correlation state unless the raw payload has been written and acknowledged by Aerospike. If writes fail after 3 retries, the event is diverted to `aerospike_dlq` and no shadow event is generated.

---

## 5. Aerospike to Flink (Multi-Get Payload Fetching)

### 5.1 Retrieval Flow
When two `ShadowEvents` correlate in RocksDB, Flink must re-attach the original full payloads before merging. This is handled by [PayloadFetcher.java](file:///c:/Users/2857155/Desktop/EOL_Final/src/main/java/com/company/flink/aerospike/PayloadFetcher.java).

```
   JoinedShadowEvent arrives from CorrelationFunction
   (Contains: common_id, payloadIdA, payloadIdB)
                     │
                     ▼
   PayloadFetcher builds Aerospike Keys:
     - Key A = (namespace: "test", set: "event_payloads", key: payloadIdA)
     - Key B = (namespace: "test", set: "event_payloads", key: payloadIdB)
                     │
                     ▼
   client.get(readPolicy, keyA)  &&  client.get(readPolicy, keyB)
                     │
         ┌───────────┴───────────┐
         │                       │
   [ Both Found ]        [ One or Both Missing / Null ]
         │                       │
         ▼                       ▼
   Extract "payload" bin   Increment missing_payload_total
   Deserialize JSON to     Route diagnostic record to
   PayloadEvent            payload_fetch_dlq
         │
         ▼
   Emit Tuple2<PayloadEvent, PayloadEvent>
   to MergeFunction
```

### 5.2 Read Policy Configuration
- `readPolicy.totalTimeout = 3000ms`
- `readPolicy.socketTimeout = 1000ms`
- `readPolicy.maxRetries = 3`
- `readPolicy.sleepBetweenRetries = 100ms`

---

## 6. End-to-End Bidirectional Interaction Lifecycle

To illustrate how these four interfaces interact in real-world operation, the following sequence tracks a pair of matching events from arrival to final publication:

```
Producer (Host)     Kafka (topic_a/b)       Flink Pipeline           Aerospike Server         Kafka (merged/dlq)
      │                    │                      │                         │                         │
      │── 1. Publish ────►│                      │                         │                         │
      │   RAN & MME logs   │                      │                         │                         │
      │                    │── 2. Poll Record ───►│                         │                         │
      │                    │   (TopicASource)     │                         │                         │
      │                    │                      │── 3. Parse & Normalize ─│                         │
      │                    │                      │   Compute SHA-256 ID    │                         │
      │                    │                      │                         │                         │
      │                    │                      │── 4. Write Payload ────►│                         │
      │                    │                      │   (PayloadWriter)       │                         │
      │                    │                      │◄── 5. ACK Write ────────│                         │
      │                    │                      │                         │                         │
      │                    │                      │── 6. Correlate Shadow ──│                         │
      │                    │                      │   (RocksDB State Join)  │                         │
      │                    │                      │                         │                         │
      │                    │                      │── 7. Multi-Get Read ───►│                         │
      │                    │                      │   (PayloadFetcher)      │                         │
      │                    │                      │◄── 8. Return Payloads ──│                         │
      │                    │                      │                         │                         │
      │                    │                      │── 9. Deep Merge ────────│                         │
      │                    │                      │   Conflict Resolution   │                         │
      │                    │                      │                         │                         │
      │                    │                      │── 10. Publish Joined ────────────────────────────►│
      │                    │                      │   (KafkaMergedSink)                               │
      │                    │                      │                                                   │
      │                    │                      │   [If any failure occurs in 3, 4, 6, 7, or 9]     │
      │                    │                      │── 11. Route Failure to DLQ ──────────────────────►│
      │                    │                      │   (DLQSink -> validation/aerospike/etc.)          │
```

---

## 7. Network Failure Modes & Recovery Matrix

| Failure Scenario | Interface Affected | Detection Mechanism | System Behavior & Mitigation |
| :--- | :--- | :--- | :--- |
| **Kafka Broker Offline** | Kafka <-> Flink | TCP Connection refused on `kafka:29092` | `TopicASource` / `TopicBSource` retry reconnection. Sinks buffer up to `buffer.memory` (64MB) and block; Flink checkpoint triggers exponential restart strategy. |
| **Aerospike Node Offline** | Flink -> Aerospike | `AerospikeException: Connection refused` | `PayloadWriter` exhausts 3 retries (total 3s timeout), then diverts record to `aerospike_dlq`. Pipeline continues without crashing. |
| **Aerospike Timeout (High Load)** | Flink -> Aerospike | `socketTimeout` (1000ms) or `totalTimeout` (3000ms) exceeded | Retries with backoff (`100ms * attempt`). If exhausted, routes to `aerospike_dlq`. Metrics counter `aerospike_write_failure_total` increments. |
| **Aerospike Record Eviction** | Aerospike -> Flink | `client.get()` returns `null` for key | `PayloadFetcher` detects missing payload, routes to `payload_fetch_dlq` (`AerospikeRecordNotFoundException`). |
| **Out-of-Order Events (>600s delay)** | Flink Stateful State | Flink processing-time timer fires in `CorrelationFunction` | `onTimer()` identifies orphan event in `stateA` or `stateB`, routes it to `correlation_dlq`, and clears state to prevent memory leaks. |
| **Consumer Lag / Backpressure** | Kafka -> Flink | In-flight network buffer saturation | Flink's credit-based flow control slows down `SourceReader` polls. Unaligned checkpoints ensure state snapshots complete even under heavy backpressure. |
