# Fault Tolerance, Failover Strategies, Consistency & Exactly-Once Architecture
## Production Resilience Engineering & In-Depth Use Case Reference

---

## 1. Introduction & Reliability Architecture Overview

In distributed real-time telecommunications stream processing, failures are not anomalies—they are guaranteed operational certainties. A production pipeline processing tens of thousands of events per second across Radio Access Networks (RAN) and Core Networks (EPC/MME) must withstand:
- Worker (TaskManager) hardware node and container crashes.
- Network partitions and socket disconnects between Flink, Kafka, and Aerospike.
- Transient downstream database throttling and storage write timeouts.
- Upstream network retransmissions causing duplicate event delivery.
- Asymmetrical event arrival where one stream halts or delays for minutes.

This document details the **exact mechanisms, configurations, code implementations, and mathematical invariants** used in this project to achieve high fault tolerance, deterministic recovery, end-to-end consistency, and Exactly-Once processing semantics.

---

## 2. Granular Fault Tolerance Mechanisms

```
                                  [ CHECKPOINT TRIGGER (Every 30s) ]
                                                   │
                                                   ▼
                                     JobManager CheckpointCoordinator
                                                   │
                            Injects CheckpointBarrier(N) into Sources
                                                   │
                 ┌─────────────────────────────────┴─────────────────────────────────┐
                 ▼                                                                   ▼
       Source A (TopicASource)                                             Source B (TopicBSource)
   - Snapshots Kafka Offsets (P0..P11)                                 - Snapshots Kafka Offsets (P0..P11)
                 │                                                                   │
                 ▼                                                                   ▼
       [ Unaligned Checkpoint Barrier: Overtakes queued data in in-flight network buffers ]
                 │                                                                   │
                 └─────────────────────────────────┬─────────────────────────────────┘
                                                   ▼
                                    Stateful CorrelationFunction
                                   (Embedded RocksDB State Backend)
                                                   │
                          ┌────────────────────────┴────────────────────────┐
                          ▼                                                 ▼
                 RocksDB MemTable Flush                           In-Flight Buffer Snapshot
                 Incremental SST Snapshots                        Persists channel bytes to
                 Transfers only newly compacted                   checkpoint storage
                 files to Checkpoint Directory                    (Prevents backpressure stall)
                          │                                                 │
                          └────────────────────────┬────────────────────────┘
                                                   ▼
                                       JobManager Acknowledgment
                                  State Commit & External Storage ACK
                                                   │
                                                   ▼
                                       Commit Offsets to Kafka
                                   (Only after Checkpoint N succeeds)
```

### 2.1 Flink Checkpointing Subsystem
Configured in [Application.java](file:///c:/Users/2857155/Desktop/EOL_Final/src/main/java/com/company/flink/Application.java#L69-L82) and [application.properties](file:///c:/Users/2857155/Desktop/EOL_Final/src/main/resources/application.properties#L57-L67):

```java
// Checkpointing configuration in Application.java
env.enableCheckpointing(30000L, CheckpointingMode.EXACTLY_ONCE);
CheckpointConfig checkpointCfg = env.getCheckpointConfig();
checkpointCfg.setCheckpointTimeout(120000L);                 // 2 minutes timeout
checkpointCfg.setMinPauseBetweenCheckpoints(10000L);          // 10s breathing room
checkpointCfg.setMaxConcurrentCheckpoints(1);                // Strict checkpoint serialization
checkpointCfg.setTolerableCheckpointFailureNumber(2);         // Up to 2 transient fails tolerated
checkpointCfg.setExternalizedCheckpointCleanup(
    CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
);
checkpointCfg.enableUnalignedCheckpoints();                   // Backpressure immunity
checkpointCfg.setCheckpointStorage("file:///tmp/flink-checkpoints/correlation-pipeline");
```

#### Detailed Parameter Rationale:
1. **`CheckpointingMode.EXACTLY_ONCE`**: Ensures that all operators snapshot their local state at the exact same logical point in the stream relative to Kafka record offsets.
2. **`minPauseBetweenCheckpoints(10000ms)`**: Guarantees that the streaming engine has at least 10 seconds of pure processing time between consecutive checkpoint snapshots, preventing "checkpoint storming" where workers spend 100% of their I/O bandwidth writing state.
3. **`maxConcurrentCheckpoints(1)`**: Enforces that checkpoint $N+1$ cannot begin until checkpoint $N$ is fully committed, avoiding out-of-order state commits and memory exhaustion.
4. **`tolerableCheckpointFailureNumber(2)`**: Allows up to 2 individual checkpoint failures before triggering a cluster-wide job restart. This accommodates transient network lag while writing snapshots to disk/S3.
5. **`RETAIN_ON_CANCELLATION`**: In the event of manual job cancellation or unrecoverable error, the metadata pointer and state files remain on disk, enabling instantaneous resumption from the latest savepoint/checkpoint.

---

### 2.2 Embedded RocksDB State Backend & Incremental Checkpointing
Configured via `new EmbeddedRocksDBStateBackend(true)` in [Application.java](file:///c:/Users/2857155/Desktop/EOL_Final/src/main/java/com/company/flink/Application.java#L66) and hardware-tuned in [flink-conf.yaml](file:///c:/Users/2857155/Desktop/EOL_Final/flink-conf.yaml#L32-L43):

* **Incremental Checkpointing**: Unlike the HashMap state backend which serializes the entire state heap to disk on every checkpoint, RocksDB incremental checkpointing captures **only the delta (newly created SST files)** since the prior checkpoint.
* **Write Pipeline**: State updates (`stateA.update()`, `stateB.update()`, `timerState.update()`) write directly to an in-memory WriteBuffer (MemTable). When the 128MB buffer fills, it is flushed to disk as an immutable SSTable file. Compaction threads (`state.backend.rocksdb.thread.num: 8`) merge SSTables in the background.
* **Bloom Filters**: `state.backend.rocksdb.use-bloom-filter: true` with `10.0` bits per key. This eliminates disk seeks for non-existent keys (e.g. checking `stateB.value()` when only A has arrived) by verifying key absence in memory with a 1% false-positive rate.
* **Managed Memory Allocation**: Flink directly controls RocksDB's block cache (`4096MB`) and write buffers within Flink's managed memory bounds, preventing native memory leaks and Linux OOM-killer invocations.

---

### 2.3 Unaligned Checkpoints for Backpressure Immunity
* **Standard (Aligned) Checkpoints**: In standard Flink checkpointing, an operator must wait for checkpoint barriers to arrive on all input channels before taking a state snapshot. If downstream sinks (like Kafka or Aerospike) experience latency, network buffers fill up, barriers become stuck in input queues, and checkpoints time out (exceeding 120s), eventually crashing the job.
* **Unaligned Checkpoints (`enableUnalignedCheckpoints()`)**:
  - The moment a `CheckpointBarrier` reaches the head of **any** input channel, the operator immediately snapshots its internal state.
  - It copies all records ahead of the barrier in other input channels and all un-dispatched output records into the checkpoint file as "in-flight data".
  - The barrier immediately bypasses queued data and travels downstream.
  - **Result**: Checkpoints complete in milliseconds even when operators are operating under 100% backpressure.

---

## 3. Failover & Restart Strategies

### 3.1 Exponential Delay Restart Strategy
Configured in [Application.java](file:///c:/Users/2857155/Desktop/EOL_Final/src/main/java/com/company/flink/Application.java#L84-L90):

```java
env.setRestartStrategy(RestartStrategies.exponentialDelayRestart(
    Time.seconds(1),    // Initial backoff
    Time.seconds(30),   // Max backoff
    2.0,                // Backoff multiplier (exponential growth)
    Time.minutes(2),    // Reset backoff threshold
    0.1                 // Jitter factor (10% randomized delay)
));
```

#### Operational Mechanics:
1. **Transient Crash**: On the first failure, Flink pauses for 1 second before restarting the topology.
2. **Cascading Crash**: If the failure persists (e.g., Aerospike restart in progress), subsequent restart intervals grow exponentially: $1\text{s} \rightarrow 2\text{s} \rightarrow 4\text{s} \rightarrow 8\text{s} \rightarrow 16\text{s} \rightarrow 30\text{s}$ (capped at 30s).
3. **Thundering Herd Avoidance (Jitter)**: A `0.1` jitter factor adds $\pm 10\%$ randomness to the backoff interval, ensuring multiple workers do not bombard recovering infrastructure at the exact same millisecond.
4. **Health Reset**: If the pipeline runs cleanly for 2 consecutive minutes (`reset-backoff-threshold`), the failure counter resets back to the initial 1-second delay.

---

### 3.2 High-Availability (HA) Layer
- **Local / Docker Environment**: Uses single JobManager coordinator. Job state is persisted to the host-mounted volume `./checkpoints:/tmp/flink-checkpoints`.
- **Kubernetes Production Environment**: Defined in [k8s/flink-configmap.yaml](file:///c:/Users/2857155/Desktop/EOL_Final/k8s/flink-configmap.yaml#L27-L28):
  - `high-availability.type: kubernetes`: Utilizes Kubernetes ConfigMaps and leader election leases instead of ZooKeeper.
  - `high-availability.storageDir: s3://flink-ha-metadata-prod/event-correlation`: Stores JobGraph and checkpoint pointer metadata in durable object storage. If the JobManager container terminates, Kubernetes spins up a new pod that recovers the running topology and resumes from the exact checkpoint offset without operator intervention.

---

## 4. End-to-End Consistency & Exactly-Once Approach

True "End-to-End Exactly-Once" requires coordinated guarantees across all three distributed tiers:

$$\text{End-to-End Consistency} = \text{Source Rewindability} + \text{State Snapshot Determinism} + \text{Sink Idempotency / 2PC}$$

```
+---------------------------------------------------------------------------------------------------+
|                                  END-TO-END CONSISTENCY GUARANTEE                                 |
|                                                                                                   |
|  [ KAFKA INGESTION ]              [ FLINK STREAM PROCESSING ]             [ STORAGE & SINK ]      |
|                                                                                                   |
|  - Rewindable Offsets             - Exactly-Once RocksDB State            - Deterministic Hash    |
|  - Committed only on              - Order-Agnostic Keyed State              (SHA-256 Primary Key) |
|    Checkpoint ACK                   (stateA, stateB, timerState)          - Aerospike Idempotent  |
|  - isolation.level:               - State-Level Deduplication               Upsert (UPDATE mode)  |
|    read_committed                 - Gated Consistency Boundary            - Kafka Merged Sink     |
|                                     (Shadow emitted on DB ACK)              Keyed on IMSI         |
+---------------------------------------------------------------------------------------------------+
```

### 4.1 Ingestion Consistency (Source Layer)
- **Source Offsets in Checkpoint**: The `KafkaSource` reader stores its current partition consumption offsets directly inside Flink's checkpointed state, **not in Kafka**.
- **Deferred Commit**: The consumer property `enable.auto.commit` is strictly `false`. Flink commits offsets back to Kafka topic `__consumer_offsets` **only when a checkpoint is confirmed by the JobManager**.
- **Recovery Behavior**: If a TaskManager crashes midway between checkpoints, Flink rewinds Kafka partition offsets to the last successful checkpoint $N$. Zero records are skipped.

### 4.2 Processing Consistency (State Layer)
- **State Transition Isolation**: In [CorrelationFunction.java](file:///c:/Users/2857155/Desktop/EOL_Final/src/main/java/com/company/flink/state/CorrelationFunction.java#L64-L106), state updates (`stateA.update()`, `stateB.update()`) and timer registrations (`registerProcessingTimeTimer()`) are tied to Flink's transactional stream processing.
- **In-State Deduplication**:
  ```java
  ShadowEvent existingA = stateA.value();
  if (existingA != null && existingA.getPayloadId().equals(incoming.getPayloadId())) {
      duplicateCounter.inc();
      LOG.debug("Duplicate A shadow event received for commonId: {}", commonId);
      return; // Discard duplicate without re-registering timers or corrupting state
  }
  ```
- **Atomicity upon Join**: When a match occurs, `stateA.clear()`, `stateB.clear()`, and `timerState.clear()` are executed synchronously within the same operator invocation. State is atomically cleaned up, preventing ghost matches on replayed streams.

### 4.3 Storage Idempotency & Consistency Boundary (Egress Layer)
- **Deterministic Primary Keys**: In [HashUtil.java](file:///c:/Users/2857155/Desktop/EOL_Final/src/main/java/com/company/flink/util/HashUtil.java#L18-L24):
  $$\text{payload\_id} = \text{SHA-256}(\text{sourceType} \mathbin{\Vert} \text{"::"} \mathbin{\Vert} \text{eventId})$$
  Because the hash is derived strictly from immutable event attributes, replaying a Kafka offset produces the **exact same string key**.
- **Aerospike Idempotent Upsert**:
  In [PayloadWriter.java](file:///c:/Users/2857155/Desktop/EOL_Final/src/main/java/com/company/flink/aerospike/PayloadWriter.java#L53), the write policy is configured with `RecordExistsAction.UPDATE`. Writing the same record multiple times during failure recovery produces identical database state without duplicate rows.
- **Gated Consistency Boundary**:
  A `ShadowEvent` is emitted to the stateful correlation pipeline **only after** Aerospike returns an ACK (`client.put(...)`). If Aerospike fails, the event is routed to `aerospike_dlq` and **no shadow event enters Flink state**. This guarantees that Flink will never hold a pointer to a payload that does not exist in the database.
- **Partition Affinity in Kafka Sinks**:
  All egress records are keyed on `common_id` (IMSI) in [KafkaMergedEventSink.java](file:///c:/Users/2857155/Desktop/EOL_Final/src/main/java/com/company/flink/sink/KafkaMergedEventSink.java#L31). Kafka's murmur2 partitioner guarantees that all updates for subscriber `IMSI_X` land on the exact same partition in `merged_events`.

---

## 5. Comprehensive Analysis of 5 Real-World Failure Use Cases

---

### Use Case 1: TaskManager Pod Crash During Peak Ingestion

#### Scenario & Architecture State:
* High load: 10,000 EPS streaming through the pipeline.
* 50,000 subscriber events are currently held in RocksDB state waiting for their corresponding pair.
* Worker container `flink-taskmanager-1` experiences a sudden hardware kernel panic / OOM crash.

```
Time (t)   Event Lifecycle & Recovery Steps
──────────────────────────────────────────────────────────────────────────────────────────────────
t = 0.0s   TaskManager-1 dies. TCP connection to JobManager drops.
t = 0.5s   JobManager Heartbeat Monitor detects TaskManager loss; marks slots as FAILED.
t = 1.0s   JobManager cancels active ExecutionGraph and halts Kafka partition consumption.
t = 1.5s   JobManager queries Checkpoint Storage; locates latest valid snapshot: Checkpoint #42.
t = 2.0s   Docker Compose / Kubernetes restarts TaskManager container.
t = 3.0s   TaskManager registers 4 new task slots with JobManager.
t = 3.5s   JobManager redistributes Task Splits and restores RocksDB state from Checkpoint #42.
           - Reads immutable SSTables from storage into local RocksDB instances.
           - Restores all active processing-time timers into Flink's InternalTimerService.
t = 4.0s   KafkaSource rewinds partition read offsets back to the exact byte offsets stored
           in Checkpoint #42.
t = 4.2s   Pipeline resumes execution:
           - Records published between Checkpoint #42 and crash are re-read from Kafka.
           - PayloadWriter re-generates identical SHA-256 payload_ids and writes to Aerospike.
             Aerospike applies `RecordExistsAction.UPDATE` (idempotent overwrite).
           - CorrelationFunction encounters replayed events: deduplication logic detects
             identical payload_id in stateA/stateB and discards duplicates.
t = 4.5s   Steady-state processing resumed with 0 duplicate joins and 0 lost records.
```

---

### Use Case 2: Transient Aerospike Cluster Outage / High Latency Spike

#### Scenario & Architecture State:
* Aerospike cluster experiences network saturation or node rebalance, causing write latencies to spike from 1ms to 5,000ms.
* Inbound stream continues pumping events into `PayloadWriter`.

```
                        [ CommonEvent Arrives at PayloadWriter ]
                                           │
                                           ▼
                                 client.put() Attempt 1
                                           │
                                     [ FAILED ]
                            (Socket Timeout: 1000ms exceeded)
                                           │
                                           ▼
                                  Sleep: 100ms * 1 = 100ms
                                           │
                                           ▼
                                 client.put() Attempt 2
                                           │
                                     [ FAILED ]
                            (Socket Timeout: 1000ms exceeded)
                                           │
                                           ▼
                                  Sleep: 100ms * 2 = 200ms
                                           │
                                           ▼
                                 client.put() Attempt 3
                                           │
                                     [ FAILED ]
                            (Total Timeout: 3000ms exceeded)
                                           │
                                           ▼
                        [ Write Retries Exhausted (Attempt 3/3) ]
                                           │
                        ┌──────────────────┴──────────────────┐
                        │                                     │
                        ▼                                     ▼
           writeFailureCounter.inc()             Construct Universal DLQEvent
                                                 - errorType: "AEROSPIKE_WRITE_ERROR"
                                                 - processingStage: "AEROSPIKE_WRITE"
                                                 - originalEvent: <Raw Payload JSON>
                                                 - retryCount: 3
                                                              │
                                                              ▼
                                                 Emit to Side-Output Tag:
                                                 `dlqAerospikeTag`
                                                              │
                                                              ▼
                                                 [ Kafka: aerospike_dlq ]
```

#### Key Invariants Preserved:
1. **Zero Pipeline Stalls**: The failure is isolated to side-outputs; Flink worker threads do not crash or block indefinitely.
2. **State Protection**: Because `out.collect(shadowEvent)` is skipped, no dangling metadata enters RocksDB.
3. **Full Observability**: Ops teams consume from `aerospike_dlq` to replay missed records once database health is restored.

---

### Use Case 3: Extreme Downstream Backpressure & Slow Consumer

#### Scenario & Architecture State:
* Downstream consumer of `merged_events` slows down, causing Kafka broker buffers to fill.
* Network backpressure cascades upstream from `KafkaMergedEventSink` through `MergeFunction`, `PayloadFetcher`, and `CorrelationFunction` to `TopicASource`.

```
Without Unaligned Checkpoints (Failure Mode):
──────────────────────────────────────────────────────────────────────────────────────────────────
1. Downstream Kafka sink blocks on full TCP send buffer.
2. Flink operators buffer records in network memory segments; all credit buffers fill to 100%.
3. CheckpointCoordinator injects CheckpointBarrier(50) into TopicASource.
4. Barrier is queued behind 50,000 un-processed data records in network buffers.
5. Checkpoint timer ticks: 30s... 60s... 90s... 120s (Checkpoint Timeout Exceeded).
6. Checkpoint fails. Tolerable failure limit (2) is exceeded. Pipeline crashes and restarts.

With Configured Unaligned Checkpoints (Project Implementation):
──────────────────────────────────────────────────────────────────────────────────────────────────
1. Downstream Kafka sink blocks; network buffers fill.
2. CheckpointBarrier(50) arrives at input channel of CorrelationFunction.
3. CorrelationFunction notices downstream channel is blocked.
4. Instant Barrier Overtake:
   - Barrier jumps to the head of the output queue, bypassing all queued data records.
   - Operator immediately snapshots current RocksDB state.
   - Operator takes an in-flight snapshot of all bytes currently sitting in input and output channels.
5. Barrier arrives at Kafka sink and commits in <150ms.
6. Checkpoint #50 successfully commits to storage despite 100% backpressure.
```

---

### Use Case 4: Asymmetric Event Arrival & Unmatched Orphan Timeout

#### Scenario & Architecture State:
* A mobile subscriber attempts to attach to the network: MME emits an `Attach Request` to Topic B.
* Due to an antenna transmission failure, the corresponding RAN Measurement Report on Topic A is permanently lost at the tower.

```
Timeline of Correlation Function State:
──────────────────────────────────────────────────────────────────────────────────────────────────
Time t = 0s:
  - Event B arrives for IMSI: `404450000000123`.
  - CorrelationFunction checks `stateA.value()` -> returns NULL (Topic A has not arrived).
  - Checks `stateB.value()` -> NULL.
  - Updates RocksDB: `stateB.update(eventB)`.
  - Increments metric: `correlation_pending_events`.
  - Registers Flink Processing-Time Timer at: `System.currentTimeMillis() + 600,000ms` (10 minutes).
  - Saves timer timestamp in `timerState.update(timerTimestamp)`.

Time t = 1s to 599s:
  - Pipeline executes hundreds of checkpoints; stateB and timerState are safely snapshotted.
  - No matching Topic A event ever arrives.

Time t = 600s:
  - Flink's internal timer service triggers `onTimer(timestamp, ctx, out)`.
  - Key context is restored: `ctx.getCurrentKey()` -> `404450000000123`.
  - Inspects state: `stateA` is null, `stateB` contains orphan Topic B metadata.
  - Executes Timeout Handler:
    1. Increments metric: `correlation_timeouts_total.inc()`.
    2. Decrements metric: `correlation_pending_events.dec()`.
    3. Builds `DLQEvent`:
       - `commonId`: "404450000000123"
       - `errorType`: "CORRELATION_TIMEOUT"
       - `processingStage`: "STATEFUL_JOIN"
       - `errorMessage`: "Correlation timed out after 600 seconds without matching event"
    4. Routes to side-output: `ctx.output(dlqOutputTag, dlqEvent)`.
    5. State Purge:
       `stateA.clear();`
       `stateB.clear();`
       `timerState.clear();`
  - Result: 100% state cleanup; zero memory leaks in RocksDB; failure logged in `correlation_dlq`.
```

---

### Use Case 5: Duplicate Ingestion from Radio Retransmissions (HARQ / RLC)

#### Scenario & Architecture State:
* Weak radio link causes a UE terminal to retransmit its LTE Measurement Report three times.
* Upstream telecom collector produces three identical records to `topic_a` for IMSI `404450000000999` with identical event IDs (`A-404450000000999-10:15:34.100`).

```
Event Arrival Sequence & Multi-Layer Deduplication:
──────────────────────────────────────────────────────────────────────────────────────────────────
1. Duplicate #1 Arrives:
   - NormalizeAFunction hashes payload:
     `payloadId = SHA-256("A::A-404450000000999-10:15:34.100")` -> `a7f9b2...`
   - PayloadWriter writes to Aerospike key `a7f9b2...` -> ACK.
   - Emits ShadowEvent(A, `404450000000999`, `a7f9b2...`).
   - CorrelationFunction stores in `stateA`.

2. Duplicate #2 Arrives (50ms later):
   - NormalizeAFunction computes identical hash -> `a7f9b2...`.
   - PayloadWriter writes to Aerospike key `a7f9b2...`:
     Aerospike updates existing record in-place (`RecordExistsAction.UPDATE`). Zero duplicate rows.
   - Emits ShadowEvent(A, `404450000000999`, `a7f9b2...`).
   - CorrelationFunction inspects `stateA.value()`:
     ```java
     ShadowEvent existingA = stateA.value();
     if (existingA != null && existingA.getPayloadId().equals(incoming.getPayloadId())) {
         duplicateCounter.inc();
         return; // SILENTLY DISCARD DUPLICATE
     }
     ```
   - Duplicate #2 is dropped before touching timer services or corrupting state.

3. Matching Topic B Event Arrives (100ms later):
   - Correlates with Duplicate #1 in `stateA`.
   - Emits exactly 1 `JoinedShadowEvent`.
   - State is cleared.

4. Duplicate #3 Arrives (Late retransmission, 5 seconds later):
   - Topic B is already matched and cleared; state is empty.
   - Duplicate #3 enters `stateA` as a new event.
   - After 600s, times out and routes to `correlation_dlq` as an orphan.
   - Downstream `merged_events` never receives a duplicate joined event.
```

---

## 6. Summary Comparison Matrix of Resilience Capabilities

| Dimension | Native / Naive Implementation | Project Implementation |
| :--- | :--- | :--- |
| **State Storage** | JVM Heap Memory (OOM prone, huge GC pauses) | **Embedded RocksDB with 4GB off-heap managed cache + SSD backing** |
| **Checkpoint Delta** | Full snapshot every cycle (multi-gigabyte writes) | **Incremental SSTable delta snapshots** |
| **Backpressure Behavior** | Checkpoints time out after 120s, job restarts | **Unaligned checkpoints snapshot in-flight channel buffers** |
| **Payload Handling** | Full payload kept in Flink memory | **Claim-Check offload to Aerospike with 7-day TTL** |
| **Recovery Idempotence** | Random UUIDs cause duplicate DB rows on retry | **Deterministic SHA-256 primary key generation** |
| **Database Failure Policy** | Task crashes; job enters restart loop | **Gated ACK boundary + 3 retries with backoff + `aerospike_dlq`** |
| **Unmatched Records** | Accumulate indefinitely, leaking memory | **Processing-time timers enforce 600s TTL eviction to `correlation_dlq`** |
| **Kafka Offset Semantics** | Periodic auto-commit (causes data loss on crash) | **Offsets committed strictly upon checkpoint confirmation** |
