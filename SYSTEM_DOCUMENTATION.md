# Enterprise Apache Flink Dual-Topic Event Correlation Pipeline
## Comprehensive System Design, Architecture, Docker Infrastructure & Operational Manual

---

## 1. Executive Summary & Telecom Domain Context

This project implements an enterprise-grade, high-throughput, low-latency stream processing and stateful event correlation pipeline built on **Apache Flink 1.19**, **Apache Kafka (KRaft mode)**, and **Aerospike NoSQL Database**.

The system correlates real-time telecommunication events arriving asynchronously across two distinct network domains:
1. **Radio Access Network (RAN / LTE)**: High-frequency telemetry and measurement reports capturing cellular radio link conditions (e.g., UE IMSI, Serving Cell, Event Type, RSRP in dBm, RSRQ in dB, Neighbor Cells).
2. **Evolved Packet Core (EPC / MME)**: Core network signaling transactions capturing mobility and session control procedures (e.g., UE IMSI, Procedure Type like Attach Request or Handover, MME ID, Tracking Area Identity / TAI, Procedure Result).

### 1.1 The Core Correlation Challenge
In high-load telecommunications networks, RAN telemetry and EPC core events for a given subscriber (identified by their **IMSI**) arrive over independent Kafka message queues with variable network jitter, arbitrary interleaving, and differing throughput rates. Joining these streams requires:
- **Order-Agnostic Stateful Correlation**: The join must produce the exact same correlated output whether the RAN event arrives milliseconds before or minutes after the corresponding MME event.
- **Payload Offloading (Claim-Check Pattern)**: Telemetry records can reach several kilobytes or megabytes. Retaining heavy payloads directly in Flink's in-memory / RocksDB state induces severe serialization overhead, memory exhaustion, and checkpoint bloat. Instead, raw payloads are offloaded to Aerospike, while lightweight **ShadowEvents** traverse Flink's state backend.
- **Deterministic Replay Safety & Idempotence**: In the event of a TaskManager failure or network disconnect, replaying events from Kafka offsets must not produce duplicate state or duplicate database writes.
- **Zero Data Loss via Multi-Tier Dead Letter Queues (DLQs)**: Malformed payloads, storage write timeouts, join timeouts, and data corruption must never crash the streaming pipeline; they must be deterministically classified and routed to isolated DLQ topics.

---

## 2. End-to-End Architecture & Data Flow

```
                                  [ TELECOM INGESTION ]
                                             │
               ┌─────────────────────────────┴─────────────────────────────┐
               ▼                                                           ▼
       [ Kafka: topic_a ]                                          [ Kafka: topic_b ]
  (RAN Measurement Reports: RSRP, RSRQ)                         (MME Core Network Events: Attach, TAI)
               │                                                           │
               ▼                                                           ▼
     TopicASource (Flink)                                        TopicBSource (Flink)
               │                                                           │
               ▼                                                           ▼
     NormalizeAFunction                                          NormalizeBFunction
  (Regex KV / JSON Parser, IMSI)                              (Regex KV / JSON Parser, IMSI)
               │                                                           │
               └─────────────────────────────┬─────────────────────────────┘
                                             ▼
                                     [ Union Streams ]
                                             │
                                             ▼
                                     ValidationFunction ───────────────► [ Side Output: validation_dlq ]
                              (Check IMSI, Payload ID, Source)              (Missing IMSI, Malformed Records)
                                             │
                                             ▼
                                     keyBy(common_id)  [IMSI]
                                             │
                        ┌────────────────────┴────────────────────┐
                        ▼                                         ▼
               MapToPayloadEvent                         PayloadWriter (Aerospike)
                        │                                         │
                        ▼                                         ├──► [ Side Output: aerospike_dlq ]
              [ Kafka: payload_events ]                           │      (Write Failures after 3 Retries)
                                                                  ▼
                                                      Emit ShadowEvent on ACK
                                                                  │
                                      ┌───────────────────────────┴───────────────────────────┐
                                      ▼                                                       ▼
                            [ Kafka: shadow_events ]                                   keyBy(common_id)
                                                                                              │
                                                                                              ▼
                                                                                     CorrelationFunction
                                                                                   (Embedded RocksDB State)
                                                                                   (A-State, B-State, Timer)
                                                                                              │
                                                                  ┌───────────────────────────┴───────────────────────────┐
                                                                  ▼                                                       ▼
                                                         [ Timeout Fired (600s) ]                                  [ Match Detected ]
                                                                  │                                                       │
                                                                  ▼                                                       ▼
                                                     [ Side Output: correlation_dlq ]                             JoinedShadowEvent
                                                                                                                          │
                                                                                                                          ▼
                                                                                                                   PayloadFetcher
                                                                                                               (Aerospike Multi-Get)
                                                                                                                          │
                                                                                                  ┌───────────────────────┴───────────────────────┐
                                                                                                  ▼                                               ▼
                                                                                   [ Side Output: payload_fetch_dlq ]               Tuple2<PayloadA, PayloadB>
                                                                                   (Missing Aerospike Record)                             │
                                                                                                                                          ▼
                                                                                                                                    MergeFunction
                                                                                                                                  (Deep Merge Engine)
                                                                                                                                          │
                                                                                                  ┌───────────────────────────────────────┴───────────────────────────────────────┐
                                                                                                  ▼                                                                               ▼
                                                                                   [ Side Output: processing_dlq ]                                                          MergedEvent
                                                                                   (Field Conflict / Merge Error)                                                                 │
                                                                                                                                                                                  ▼
                                                                                                                                                                       KafkaMergedEventSink
                                                                                                                                                                                  │
                                                                                                                                                                                  ▼
                                                                                                                                                                        [ Kafka: merged_events ]
```

### 2.1 Stage-by-Stage Flow Description

#### Stage 1: Connection & Parallel Ingestion
- Ingestion occurs via two independent `KafkaSource` instances: `TopicASource` (`topic_a`) and `TopicBSource` (`topic_b`).
- Sources run with independent consumer group IDs (`flink-event-join-cg-a` and `flink-event-join-cg-b`), isolation level `read_committed`, and configurable offset initializers (`earliest` by default).
- `.rebalance()` is invoked on each raw stream to evenly distribute Kafka partitions across all available Flink TaskManager slots.

#### Stage 2: Dual-Format Normalization & Deterministic Hashing
- [NormalizeAFunction.java](file:///c:/Users/2857155/Desktop/EOL_Final/src/main/java/com/company/flink/normalize/NormalizeAFunction.java) and [NormalizeBFunction.java](file:///c:/Users/2857155/Desktop/EOL_Final/src/main/java/com/company/flink/normalize/NormalizeBFunction.java) process raw string inputs.
- **Parsing Fallback**: Tries standard JSON parsing first via Jackson into `RawEventA` / `RawEventB`. If the input is not JSON, it falls back to compiled regular expression patterns (`KV_PATTERN`) to extract fields from telecom log lines.
- **IMSI Resolution**: Resolves the subscriber's IMSI as the canonical `common_id`.
- **Deterministic ID Derivation**: Computes `payload_id = SHA-256(sourceType + "::" + eventId)` via `HashUtil`. This ensures deterministic idempotency on failure recovery.
- Emits a unified `CommonEvent` containing the normalized data map.

#### Stage 3: Global Validation & Workload Partitioning
- Streams from both normalizers are unified via `normalizedA.union(normalizedB)`.
- [ValidationFunction.java](file:///c:/Users/2857155/Desktop/EOL_Final/src/main/java/com/company/flink/validation/ValidationFunction.java) checks:
  - Non-null event.
  - Presence of non-empty `common_id` (IMSI).
  - Presence of non-empty `payload_id`.
  - Source type validation (`"A"` or `"B"`).
- Failing records are emitted to the `dlq-validation` side-output.
- The stream is partitioned using `.keyBy(CommonEvent::getCommonId)`, ensuring that all events for a given subscriber are routed to the same TaskManager subtask.

#### Stage 4: Payload Offloading & Raw Egress
- The keyed stream branches into two parallel paths:
  1. **Audit Egress**: Maps each `CommonEvent` to a `PayloadEvent` and publishes it to the Kafka topic `payload_events` via `KafkaPayloadEventSink`.
  2. **Aerospike Offload**: [PayloadWriter.java](file:///c:/Users/2857155/Desktop/EOL_Final/src/main/java/com/company/flink/aerospike/PayloadWriter.java) writes the complete payload JSON to Aerospike namespace `test`, set `event_payloads`, using `payload_id` as the primary key.
- **Consistency Boundary**: `PayloadWriter` emits a lightweight `ShadowEvent` downstream **only after** Aerospike confirms the write with an ACK. If writes fail after 3 retries, the record is routed to `dlq-aerospike` and no shadow event enters the correlation state.
- Emitted shadow events are mirrored to Kafka topic `shadow_events` via `KafkaShadowEventSink`.

#### Stage 5: Stateful Correlation in RocksDB
- [CorrelationFunction.java](file:///c:/Users/2857155/Desktop/EOL_Final/src/main/java/com/company/flink/state/CorrelationFunction.java) is keyed on `common_id` (IMSI) and backed by `EmbeddedRocksDBStateBackend`.
- Maintains three state handles:
  - `ValueState<ShadowEvent> stateA`
  - `ValueState<ShadowEvent> stateB`
  - `ValueState<Long> timerState`
- **Order-Agnostic Correlation**:
  - When Topic A arrives: If `stateB` exists, a match is made. Otherwise, `stateA` is updated and a processing-time timer (default 600s) is registered.
  - When Topic B arrives: If `stateA` exists, a match is made. Otherwise, `stateB` is updated and a timer is registered.
- **Upon Match**:
  - Emits `JoinedShadowEvent` containing `(common_id, payloadIdA, payloadIdB, joinTimestamp, latencyMs)`.
  - Deletes the registered timer from Flink's timer service.
  - Clears `stateA`, `stateB`, and `timerState` to prevent memory leaks.
- **Upon Timeout (`onTimer`)**:
  - The remaining orphan shadow event is encapsulated into a `DLQEvent` with type `CORRELATION_TIMEOUT` and emitted to `dlq-correlation`.
  - All state for that IMSI key is cleared.

#### Stage 6: Payload Multi-Get & Deep Merge
- [PayloadFetcher.java](file:///c:/Users/2857155/Desktop/EOL_Final/src/main/java/com/company/flink/aerospike/PayloadFetcher.java) queries Aerospike using batch / multi-get policies for `payloadIdA` and `payloadIdB`.
- If either payload is missing (e.g. evicted or missing), routes to `dlq-payload-fetch`.
- On success, outputs `Tuple2<PayloadEvent, PayloadEvent>`.
- [MergeFunction.java](file:///c:/Users/2857155/Desktop/EOL_Final/src/main/java/com/company/flink/merge/MergeFunction.java) applies `ConflictResolutionStrategy.DEEP_MERGE`. All fields from RAN and MME are combined. In case of key collisions, the Topic B attribute is namespaced as `b_<key>`.
- Emits the final `MergedEvent`.

#### Stage 7: Final Egress & Multi-Tier DLQ Sinks
- [KafkaMergedEventSink.java](file:///c:/Users/2857155/Desktop/EOL_Final/src/main/java/com/company/flink/sink/KafkaMergedEventSink.java) writes `MergedEvent` to `merged_events` keyed on IMSI for deterministic partition ordering.
- [DLQSink.java](file:///c:/Users/2857155/Desktop/EOL_Final/src/main/java/com/company/flink/sink/DLQSink.java) establishes 5 dedicated Kafka sinks for each side-output:
  1. `validation_dlq`
  2. `aerospike_dlq`
  3. `correlation_dlq`
  4. `payload_fetch_dlq`
  5. `processing_dlq`

---

## 3. System Design Decisions & Architectural Patterns

### 3.1 Claim-Check Pattern (Payload Offloading)
- **Problem**: Inbound telecom events range from 256 bytes to several kilobytes/megabytes. Storing full payloads in RocksDB state for hundreds of thousands of active subscribers during a 10-minute correlation window leads to extreme write amplification, large checkpoint sizes (gigabytes/terabytes), and high GC pauses.
- **Solution**: The complete raw JSON payload is offloaded to Aerospike immediately following normalization. Only an ultra-lightweight `ShadowEvent` (~100 bytes) containing metadata (`sourceType`, `commonId`, `payloadId`, timestamps) is stored in RocksDB state. Upon correlation match, the full payloads are fetched via primary key lookup.

### 3.2 Embedded RocksDB State Backend
- **Configuration**: `EmbeddedRocksDBStateBackend(incremental = true)`.
- **Memory Management**: Off-heap memory is managed through Flink's managed memory allocator (`state.backend.rocksdb.memory.managed: true`) with `1024MB` fixed allocation per slot.
- **Optimization**:
  - Bloom filters enabled with `10.0` bits per key to eliminate point-lookup disk I/O.
  - 4 write buffers of `128MB` each to smooth out write spikes.
  - Level-style compaction (`LEVEL`) to minimize disk space amplification.

### 3.3 Consistency Boundaries & Idempotency
- **Deterministic Identity**: `HashUtil.generateDeterministicPayloadId(sourceType, eventId)` generates a SHA-256 hash. If Flink crashes and replays from Kafka offsets, the exact same `payload_id` is recomputed, ensuring idempotent upserts in Aerospike (`RecordExistsAction.UPDATE`).
- **Synchronous ACK Gating**: `ShadowEvent` is emitted to the correlation stream **only after** Aerospike returns an ACK. If Aerospike is unreachable, the event goes to DLQ and never enters RocksDB, avoiding orphan state that could never be merged.

### 3.4 Fault Tolerance & Checkpointing
- **Interval**: 30,000 ms (30 seconds) with `EXACTLY_ONCE` consistency mode.
- **Unaligned Checkpoints**: `execution.checkpointing.unaligned.enabled: true` prevents checkpoint timeouts during downstream backpressure by snapshotting in-flight network buffers.
- **Restart Strategy**: Exponential delay restart with 1s initial backoff, 30s max backoff, multiplier 2.0, and 10% jitter to prevent thundering-herd issues on downstream systems.
- **State Retention**: Externalized checkpoints are configured with `RETAIN_ON_CANCELLATION`.

---

## 4. Docker Infrastructure & Container Breakdown

### 4.1 Topology Diagram

```
                              ┌─────────────────────────────────────────────────────────┐
                              │                    Host Machine                         │
                              │                 (Ports Exposed)                         │
                              └───────┬────────────┬─────────────┬────────────┬─────────┘
                                      │            │             │            │
                                 :9092│       :3000│        :8081│       :8089│
                                      ▼            ▼             ▼            ▼
   ┌──────────────────────────────────┼────────────┼─────────────┼────────────┼─────────────────────────────────┐
   │ flink-net (Bridge Network)       │            │             │            │                                 │
   │                                  ▼            ▼             ▼            ▼                                 │
   │                          ┌──────────────┐ ┌───────────┐ ┌──────────────────────┐                           │
   │                          │    kafka     │ │ aerospike │ │   flink-jobmanager   │                           │
   │                          │ (KRaft Mode) │ │  Server   │ │ (Coordination & UI)  │                           │
   │                          └──────────────┘ └───────────┘ └──────────┬───────────┘                           │
   │                                                                    │                                       │
   │                                                        RPC :6123   │                                       │
   │                                                                    ▼                                       │
   │                                                     ┌──────────────────────────────┐                       │
   │                                                     │   flink-taskmanager (x2)     │                       │
   │                                                     │   (Worker Nodes: 8 Slots)    │                       │
   │                                                     └──────────────────────────────┘                       │
   └────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

### 4.2 Detailed Container Specifications

| Container Name | Base Docker Image | Network / Hostname | Exposed Ports | Storage / Volumes | Role & Key Settings |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **`kafka`** | `confluentinc/cp-kafka:7.6.0` | `flink-net` / `kafka` | `9092`, `9093` | `/tmp/kraft-combined-logs` | Apache Kafka broker running in KRaft mode (no ZooKeeper). Listens on `29092` internally and `9092` externally. Log retention 5m. |
| **`aerospike`** | `aerospike/aerospike-server:latest` | `flink-net` / `aerospike` | `3000` | `./aerospike.conf/aerospike.conf:/etc/aerospike/aerospike.conf` | Aerospike NoSQL storage. Single-node cluster, namespace `test`, backed by 4GB dat file with kernel page cache enabled. |
| **`flink-jobmanager`** | `flink:1.19.1-java17` | `flink-net` / `flink-jobmanager` | `8081` | `.:/opt/flink/project`, `./checkpoints:/tmp/flink-checkpoints` | Flink master node managing job graphs, task slot allocations, and web dashboard at http://localhost:8081. |
| **`flink-taskmanager`** | `flink:1.19.1-java17` | `flink-net` / dynamic | None | `.:/opt/flink/project`, `./checkpoints:/tmp/flink-checkpoints` | Flink worker nodes (`scale: 2`). Each TM provides 4 task slots (8 slots total). Runs Embedded RocksDB state backend. |
| **Maven Builder** (Ephemeral) | `maven:3.9.6-eclipse-temurin-17` | Host / default | None | `${PWD}:/app`, `maven-repo:/root/.m2` | Compiles and packages the shaded Uber-JAR without requiring local Maven or JDK on the host OS. |

---

## 5. Granular File-by-File Breakdown

### 5.1 Root Configuration & Build Files

- [docker-compose.yml](file:///c:/Users/2857155/Desktop/EOL_Final/docker-compose.yml): Declares the four core services (`kafka`, `aerospike`, `jobmanager`, `taskmanager`), defines environment variables, port mappings, health checks, volume bind-mounts, and the `flink-net` bridge network.
- [Dockerfile](file:///c:/Users/2857155/Desktop/EOL_Final/Dockerfile): Multi-stage container build definition. First stage compiles source with `maven:3.9.6-eclipse-temurin-17` with offline dependency caching; second stage copies the shaded JAR into `flink:1.19.1-java17` at `/opt/flink/usrlib/` under user `flink`.
- [pom.xml](file:///c:/Users/2857155/Desktop/EOL_Final/pom.xml): Maven Project Object Model file specifying dependencies (`flink-streaming-java`, `flink-clients`, `flink-statebackend-rocksdb`, `flink-connector-kafka`, `aerospike-client`, `jackson-databind`), compiler configurations for Java 17, and the `maven-shade-plugin` packaging configuration.
- [flink-conf.yaml](file:///c:/Users/2857155/Desktop/EOL_Final/flink-conf.yaml): Hardware-tuned Flink cluster configuration tailored for high-throughput single-machine workloads (32GB RAM / 8 CPU cores). Sets memory budgets, RocksDB compaction and cache rules, and unaligned checkpoint settings.
- [HOW_TO_EXECUTE.txt](file:///c:/Users/2857155/Desktop/EOL_Final/HOW_TO_EXECUTE.txt): Operational step-by-step execution guide detailing startup, build commands, topic creation, job submission, stress testing, and teardown.
- [dlq_exec.txt](file:///c:/Users/2857155/Desktop/EOL_Final/dlq_exec.txt): Reference commands for inspecting each of the 5 DLQ topics individually or tailing all simultaneously with regex patterns.
- [README.md](file:///c:/Users/2857155/Desktop/EOL_Final/README.md): High-level architectural summary, schema definitions, and quickstart documentation.
- [aerospike.conf/aerospike.conf](file:///c:/Users/2857155/Desktop/EOL_Final/aerospike.conf/aerospike.conf): Aerospike server configuration defining service ports, fabric ports, mesh heartbeat, and the `test` namespace storage engine.

---

### 5.2 Java Source Code (`src/main/java/com/company/flink/`)

#### Application Entrypoint
- [Application.java](file:///c:/Users/2857155/Desktop/EOL_Final/src/main/java/com/company/flink/Application.java): Main execution entrypoint. Orchestrates configuration loading, Flink environment initialization, RocksDB backend instantiation, DAG graph construction, side-output routing, and job execution.

#### Configuration Layer (`config/`)
- [AppConfig.java](file:///c:/Users/2857155/Desktop/EOL_Final/src/main/java/com/company/flink/config/AppConfig.java): Hierarchical configuration loader combining classpath `application.properties`, environment variable overrides, and CLI parameters.
- [FlinkConfig.java](file:///c:/Users/2857155/Desktop/EOL_Final/src/main/java/com/company/flink/config/FlinkConfig.java): Immutable model holding Flink runtime settings (parallelism, checkpoint interval/timeout, correlation timeout).
- [KafkaConfig.java](file:///c:/Users/2857155/Desktop/EOL_Final/src/main/java/com/company/flink/config/KafkaConfig.java): Immutable model holding Kafka connection details, consumer group properties, producer batching/compression rules, and topic names.
- [AerospikeConfig.java](file:///c:/Users/2857155/Desktop/EOL_Final/src/main/java/com/company/flink/config/AerospikeConfig.java): Immutable model for Aerospike connection parameters, retry counts, timeout settings, and payload TTL.

#### Data Models (`model/`)
- [RawEventA.java](file:///c:/Users/2857155/Desktop/EOL_Final/src/main/java/com/company/flink/model/RawEventA.java): Inbound model for Topic A (RAN). Extracts `UE`, `IMSI`, `Cell`, `Event`, `RSRP`, `RSRQ`, `Neighbor Cell`, and supports dynamic JSON properties.
- [RawEventB.java](file:///c:/Users/2857155/Desktop/EOL_Final/src/main/java/com/company/flink/model/RawEventB.java): Inbound model for Topic B (MME). Extracts `IMSI`, `Procedure`, `MME_ID`, `TAI`, `Result`, and dynamic properties.
- [CommonEvent.java](file:///c:/Users/2857155/Desktop/EOL_Final/src/main/java/com/company/flink/model/CommonEvent.java): Unified normalized internal event representing standardized records from either stream.
- [ShadowEvent.java](file:///c:/Users/2857155/Desktop/EOL_Final/src/main/java/com/company/flink/model/ShadowEvent.java): Lightweight correlation metadata event containing only identifiers and timestamps for RocksDB storage.
- [JoinedShadowEvent.java](file:///c:/Users/2857155/Desktop/EOL_Final/src/main/java/com/company/flink/model/JoinedShadowEvent.java): Correlation join output containing `payloadIdA`, `payloadIdB`, join timestamp, and correlation latency.
- [PayloadEvent.java](file:///c:/Users/2857155/Desktop/EOL_Final/src/main/java/com/company/flink/model/PayloadEvent.java): Full payload representation offloaded to Aerospike and published to `payload_events`.
- [MergedEvent.java](file:///c:/Users/2857155/Desktop/EOL_Final/src/main/java/com/company/flink/model/MergedEvent.java): Canonical final joined record published to `merged_events`.
- [DLQEvent.java](file:///c:/Users/2857155/Desktop/EOL_Final/src/main/java/com/company/flink/model/DLQEvent.java): Standardized error event envelope capturing diagnostics, stage names, exception classes, retry counts, and the original raw payload.

#### Ingestion & Normalization (`source/`, `normalize/`, `validation/`)
- [TopicASource.java](file:///c:/Users/2857155/Desktop/EOL_Final/src/main/java/com/company/flink/source/TopicASource.java): Builds the `KafkaSource` for Topic A.
- [TopicBSource.java](file:///c:/Users/2857155/Desktop/EOL_Final/src/main/java/com/company/flink/source/TopicBSource.java): Builds the `KafkaSource` for Topic B.
- [NormalizeAFunction.java](file:///c:/Users/2857155/Desktop/EOL_Final/src/main/java/com/company/flink/normalize/NormalizeAFunction.java): Normalizes Topic A strings (JSON or KV regex), extracts IMSI, calculates SHA-256 payload IDs, and emits `CommonEvent` or DLQ.
- [NormalizeBFunction.java](file:///c:/Users/2857155/Desktop/EOL_Final/src/main/java/com/company/flink/normalize/NormalizeBFunction.java): Normalizes Topic B strings (JSON or KV regex), extracts IMSI, calculates SHA-256 payload IDs, and emits `CommonEvent` or DLQ.
- [ValidationFunction.java](file:///c:/Users/2857155/Desktop/EOL_Final/src/main/java/com/company/flink/validation/ValidationFunction.java): Enforces mandatory field constraints prior to storage offload.

#### Database Offload Layer (`aerospike/`)
- [AerospikeClientProvider.java](file:///c:/Users/2857155/Desktop/EOL_Final/src/main/java/com/company/flink/aerospike/AerospikeClientProvider.java): Thread-safe singleton provider managing connection pools per JVM.
- [PayloadWriter.java](file:///c:/Users/2857155/Desktop/EOL_Final/src/main/java/com/company/flink/aerospike/PayloadWriter.java): Idempotently writes payloads to Aerospike with retries and emits `ShadowEvent` upon write confirmation.
- [PayloadFetcher.java](file:///c:/Users/2857155/Desktop/EOL_Final/src/main/java/com/company/flink/aerospike/PayloadFetcher.java): Fetches both Payload A and Payload B from Aerospike via primary keys for correlated shadow pairs.

#### Stateful Correlation Layer (`state/`)
- [CorrelationFunction.java](file:///c:/Users/2857155/Desktop/EOL_Final/src/main/java/com/company/flink/state/CorrelationFunction.java): Keyed process function managing RocksDB state (`stateA`, `stateB`, `timerState`). Performs order-agnostic matching, state purging upon join, and timeout routing to `dlq-correlation`.

#### Merge Engine (`merge/`)
- [ConflictResolutionStrategy.java](file:///c:/Users/2857155/Desktop/EOL_Final/src/main/java/com/company/flink/merge/ConflictResolutionStrategy.java): Resolves key collisions during merge (`DEEP_MERGE`, `PREFER_A`, `PREFER_B`, `SEPARATE_NAMESPACES`).
- [MergeFunction.java](file:///c:/Users/2857155/Desktop/EOL_Final/src/main/java/com/company/flink/merge/MergeFunction.java): Combines fetched payload pairs into canonical `MergedEvent`.

#### Sinks (`sink/`)
- [KafkaMergedEventSink.java](file:///c:/Users/2857155/Desktop/EOL_Final/src/main/java/com/company/flink/sink/KafkaMergedEventSink.java): Publishes final joined events to `merged_events` keyed on IMSI.
- [KafkaShadowEventSink.java](file:///c:/Users/2857155/Desktop/EOL_Final/src/main/java/com/company/flink/sink/KafkaShadowEventSink.java): Publishes lightweight shadow events to `shadow_events`.
- [KafkaPayloadEventSink.java](file:///c:/Users/2857155/Desktop/EOL_Final/src/main/java/com/company/flink/sink/KafkaPayloadEventSink.java): Publishes full raw payloads to `payload_events`.
- [DLQSink.java](file:///c:/Users/2857155/Desktop/EOL_Final/src/main/java/com/company/flink/sink/DLQSink.java): Generic factory creating Kafka sinks for all 5 DLQ topics.

#### Utilities & Benchmarks (`util/`, `metrics/`)
- [HashUtil.java](file:///c:/Users/2857155/Desktop/EOL_Final/src/main/java/com/company/flink/util/HashUtil.java): Thread-safe SHA-256 hashing for deterministic ID generation.
- [JsonUtil.java](file:///c:/Users/2857155/Desktop/EOL_Final/src/main/java/com/company/flink/util/JsonUtil.java): Thread-safe Jackson ObjectMapper configured with JSR310 date support.
- [PipelineMetrics.java](file:///c:/Users/2857155/Desktop/EOL_Final/src/main/java/com/company/flink/metrics/PipelineMetrics.java): Metric constants used across the pipeline.
- [HighThroughputBenchmarkProducer.java](file:///c:/Users/2857155/Desktop/EOL_Final/src/main/java/com/company/flink/util/HighThroughputBenchmarkProducer.java): Native high-speed Java load generator capable of producing 50,000+ EPS with padded payloads, measuring end-to-end latency and verifying DLQ routing.

---

### 5.3 Scripts (`scripts/`)

- [create-topics.ps1](file:///c:/Users/2857155/Desktop/EOL_Final/scripts/create-topics.ps1) / [create-topics.sh](file:///c:/Users/2857155/Desktop/EOL_Final/scripts/create-topics.sh): Provisions all 10 topics (12 partitions each, 5-minute retention).
- [produce-topic-a.ps1](file:///c:/Users/2857155/Desktop/EOL_Final/scripts/produce-topic-a.ps1) / [produce-topic-a.sh](file:///c:/Users/2857155/Desktop/EOL_Final/scripts/produce-topic-a.sh): Generates RAN Measurement Reports with custom EPS, duration, payload size, and 100:1 orphan injection.
- [produce-topic-b.ps1](file:///c:/Users/2857155/Desktop/EOL_Final/scripts/produce-topic-b.ps1) / [produce-topic-b.sh](file:///c:/Users/2857155/Desktop/EOL_Final/scripts/produce-topic-b.sh): Generates MME Core Network events matching Topic A IMSIs.
- [consume-merged.ps1](file:///c:/Users/2857155/Desktop/EOL_Final/scripts/consume-merged.ps1) / [consume-merged.sh](file:///c:/Users/2857155/Desktop/EOL_Final/scripts/consume-merged.sh): Real-time consumer for joined events from `merged_events`.
- [consume-shadow.ps1](file:///c:/Users/2857155/Desktop/EOL_Final/scripts/consume-shadow.ps1) / [consume-shadow.sh](file:///c:/Users/2857155/Desktop/EOL_Final/scripts/consume-shadow.sh): Real-time consumer for `shadow_events`.
- [consume-payload.ps1](file:///c:/Users/2857155/Desktop/EOL_Final/scripts/consume-payload.ps1) / [consume-payload.sh](file:///c:/Users/2857155/Desktop/EOL_Final/scripts/consume-payload.sh): Real-time consumer for `payload_events`.
- [run-all.ps1](file:///c:/Users/2857155/Desktop/EOL_Final/scripts/run-all.ps1): Interactive PowerShell wrapper triggering the containerized native Java benchmark.
- [test_1000_async_joins.py](file:///c:/Users/2857155/Desktop/EOL_Final/scripts/test_1000_async_joins.py): Python load generator executing 10,000 asynchronous shuffled joins and validating output schema.
- [run_benchmark.py](file:///c:/Users/2857155/Desktop/EOL_Final/scripts/run_benchmark.py): Configurable Python benchmark runner with orphan injection and latency percentiles.

---

### 5.4 Kubernetes Manifests (`k8s/`)

- [flink-configmap.yaml](file:///c:/Users/2857155/Desktop/EOL_Final/k8s/flink-configmap.yaml): Production Kubernetes ConfigMap configuring S3 checkpointing (`s3://flink-checkpoints-prod/event-correlation`), Kubernetes High Availability, and RocksDB memory settings.
- [flink-jobmanager.yaml](file:///c:/Users/2857155/Desktop/EOL_Final/k8s/flink-jobmanager.yaml): Deployment and ClusterIP service for Flink JobManager exposing RPC (6123), Blob (6124), and Web UI (8081).
- [flink-taskmanager.yaml](file:///c:/Users/2857155/Desktop/EOL_Final/k8s/flink-taskmanager.yaml): Deployment for 2 TaskManager pods with pod anti-affinity across hostnames.
- [flink-job.yaml](file:///c:/Users/2857155/Desktop/EOL_Final/k8s/flink-job.yaml): Kubernetes Batch Job for automated pipeline submission to JobManager.

---

## 6. Executable Operational Playbook

### Step 1: Start Infrastructure
```powershell
docker compose up -d
docker compose ps
```

### Step 2: Build Application JAR
```powershell
docker run --rm -v "${PWD}:/app" -v maven-repo:/root/.m2 -w /app maven:3.9.6-eclipse-temurin-17 mvn clean package -DskipTests
```

### Step 3: Provision Kafka Topics
```powershell
.\scripts\create-topics.ps1
```

### Step 4: Submit Pipeline to Flink
```powershell
docker exec flink-jobmanager ./bin/flink run -d -c com.company.flink.Application /opt/flink/project/target/flink-event-correlation-1.0.0.jar --kafka.bootstrap.servers kafka:29092 --aerospike.hosts aerospike:3000
```
Check Web UI at: `http://localhost:8081`

### Step 5: Execute Native High-Throughput Stress Test
```powershell
docker exec -it flink-jobmanager java -cp /opt/flink/project/target/flink-event-correlation-1.0.0.jar com.company.flink.util.HighThroughputBenchmarkProducer 5000 30 5128 kafka:29092
```

### Step 6: Manual Ingestion & Sink Verification
- **Terminal 1: Consume Merged Events**:
  ```powershell
  .\scripts\consume-merged.ps1
  ```
- **Terminal 2: Ingest RAN Measurement Event (Topic A)**:
  ```powershell
  .\scripts\produce-topic-a.ps1 -EPS 10 -DurationSeconds 5 -EventSizeBytes 512
  ```
- **Terminal 3: Ingest MME Core Event (Topic B)**:
  ```powershell
  .\scripts\produce-topic-b.ps1 -EPS 10 -DurationSeconds 5 -EventSizeBytes 512
  ```

### Step 7: Inspect Dead Letter Queues (DLQ)
Stream all DLQs simultaneously:
```powershell
docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 --whitelist ".*_dlq" --from-beginning
```

### Step 8: Teardown
```powershell
docker compose down -v
```
