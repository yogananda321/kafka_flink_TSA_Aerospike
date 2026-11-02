# Chapter 7 — TaskManager, Task Slots, Subtasks and Parallelism

## Detailed Execution Model

---

## Table of Contents

- [7A. Execution Hierarchy](#7a-execution-hierarchy)
- [7B. What Happens When Parallelism Is Set](#7b-what-happens-when-parallelism-is-set)
- [7C. What Is a Subtask](#7c-what-is-a-subtask)
- [7D. Record Routing and Partitioning](#7d-record-routing-and-partitioning)
- [7E. What Is a Task Slot](#7e-what-is-a-task-slot)
- [7F. Subtask-to-Slot Assignment](#7f-subtask-to-slot-assignment)
- [7G. Subtask ≠ Slot](#7g-subtask--slot)
- [7H. Operator Chaining](#7h-operator-chaining)
- [7I. Complete Example — 2 TMs × 2 Slots](#7i-complete-example--2-tms--2-slots)
- [7J. Example With Two Kafka Topics](#7j-example-with-two-kafka-topics)
- [7K. Effect of Increasing Slots](#7k-effect-of-increasing-slots)
- [7L. Effect of Increasing Parallelism](#7l-effect-of-increasing-parallelism)
- [7M. CPU Cores vs Task Slots](#7m-cpu-cores-vs-task-slots)
- [7N. Why Slots Can Exceed CPU Cores](#7n-why-slots-can-exceed-cpu-cores)
- [7O. Practical Slot Sizing](#7o-practical-slot-sizing)
- [7P. TaskManager Sizing](#7p-taskmanager-sizing)
- [7Q. Parallelism vs Slots — The Key Relationship](#7q-parallelism-vs-slots--the-key-relationship)
- [7R. Complete End-to-End Example](#7r-complete-end-to-end-example)
- [7S. TaskManager Failure Recovery](#7s-taskmanager-failure-recovery)
- [7T. TaskManager Failure vs Server Failure](#7t-taskmanager-failure-vs-server-failure)
- [7U. Recommended Mental Model](#7u-recommended-mental-model)
- [7V. The Five Equations to Remember](#7v-the-five-equations-to-remember)

---

## 7A. Execution Hierarchy

The relationship between **TaskManager**, **Task Slot**, **Operator**, **Subtask**, and **Parallelism** is one of the most important concepts in understanding how a Flink application actually executes.

```
Flink Cluster
│
├── JobManager
│
├── TaskManager 1
│   ├── Slot 1
│   ├── Slot 2
│   ├── Slot 3
│   └── Slot 4
│
├── TaskManager 2
│   ├── Slot 1
│   ├── Slot 2
│   ├── Slot 3
│   └── Slot 4
│
└── TaskManager 3
    ├── Slot 1
    ├── Slot 2
    ├── Slot 3
    └── Slot 4
```

> **Core Principle:**
> TaskManagers provide **execution resources**, slots provide **scheduling capacity**, and subtasks are the **actual parallel instances** of operators that execute using those resources.

---

## 7B. What Happens When Parallelism Is Set

Consider an application with this pipeline:

```
Kafka Source → Normalize → Validate → Sink
```

When the application parallelism is set to **4**, Flink creates four parallel instances of **each** operator:

| Operator    | Subtask Instances                              |
|-------------|------------------------------------------------|
| Source      | `Source-0`, `Source-1`, `Source-2`, `Source-3`  |
| Normalize   | `Normalize-0`, `Normalize-1`, `Normalize-2`, `Normalize-3` |
| Validate    | `Validate-0`, `Validate-1`, `Validate-2`, `Validate-3` |
| Sink        | `Sink-0`, `Sink-1`, `Sink-2`, `Sink-3`        |

**Formula:**

```
Subtasks per Operator = Operator Parallelism
```

For example, `P = 4` means **4 subtasks** for that operator.

> **⚠️ Common Misconceptions**
>
> | Incorrect Interpretation            | Correct Interpretation                              |
> |-------------------------------------|-----------------------------------------------------|
> | Parallelism 4 = 4 CPU cores         | Parallelism determines the number of parallel operator instances |
> | Parallelism 4 = 4 Task Slots        | The scheduler determines where subtasks execute     |

---

## 7C. What Is a Subtask

- An **operator** is the logical processing stage.
- A **subtask** is one **parallel execution instance** of that operator.

**Example:** Operator `Normalize` with `Parallelism = 4` creates:

```
Normalize
 ├── Normalize-0
 ├── Normalize-1
 ├── Normalize-2
 └── Normalize-3
```

Each subtask processes a **portion** of the incoming data. Instead of a single processor handling all records, Flink distributes the work:

```
Without parallelism:           With parallelism (P=4):

    Normalize                       Normalize
        |                               |
   1 processor                ┌────┬────┼────┐
                              ↓    ↓    ↓    ↓
                             N-0  N-1  N-2  N-3
```

Four subtasks can process records **concurrently**.

---

## 7D. Record Routing and Partitioning

What determines which subtask receives a record depends on the **operator type** and the **partitioning strategy**.

### Kafka Source Partitioning

For a Kafka source, **Kafka partitions** determine which source subtask receives records.

| Kafka Partition | Source Subtask |
|-----------------|---------------|
| Partition 0     | `Source-0`    |
| Partition 1     | `Source-1`    |
| Partition 2     | `Source-2`    |
| Partition 3     | `Source-3`    |

```
Kafka
 ├── P0 ─────→ Source-0
 ├── P1 ─────→ Source-1
 ├── P2 ─────→ Source-2
 └── P3 ─────→ Source-3
```

> **Note:** This assignment is **not permanent**. Kafka/Flink consumer assignment can change during rebalancing or recovery.

### Source Parallelism Upper Bound

```
Useful Source Parallelism ≤ Kafka Partition Count
```

If Kafka has only **4 partitions** but source parallelism is set to **8**:

| Subtask    | Status                 |
|------------|------------------------|
| Source-0   | ✅ Active (partition assigned) |
| Source-1   | ✅ Active (partition assigned) |
| Source-2   | ✅ Active (partition assigned) |
| Source-3   | ✅ Active (partition assigned) |
| Source-4   | ❌ Idle (no partition)  |
| Source-5   | ❌ Idle (no partition)  |
| Source-6   | ❌ Idle (no partition)  |
| Source-7   | ❌ Idle (no partition)  |

> **Key Takeaway:** Increasing source parallelism beyond the Kafka partition count **does not** create additional Kafka ingestion parallelism.

---

## 7E. What Is a Task Slot

A **Task Slot** is a **resource/scheduling unit** inside a TaskManager.

```
TaskManager
│
├── Slot 0
├── Slot 1
├── Slot 2
└── Slot 3
```

This means the TaskManager has **4 slots** available for scheduling work.

**Total Slot Calculation:**

```
Total Slots = TaskManager Count × Slots per TaskManager
```

**Example:**

| TaskManagers | Slots per TM | Total Slots |
|:------------:|:------------:|:-----------:|
| 2            | 4            | **8**       |

> **⚠️ Critical Distinction — A slot is NOT:**
>
> | Not a…          | A slot is… |
> |-----------------|------------|
> | CPU core        | A Flink **scheduling/resource capacity** unit |
> | Kafka partition | |
> | Operator        | |
> | Subtask         | |
> | Thread          | |

---

## 7F. Subtask-to-Slot Assignment

The **JobManager/Scheduler** assigns subtasks to available slots.

**Setup:**

```
2 TaskManagers × 2 Slots = 4 Total Slots
```

```
TM1                    TM2
├── Slot 1             ├── Slot 1
└── Slot 2             └── Slot 2
```

For `Operator N` with `Parallelism = 4`, Flink creates subtasks `N0`, `N1`, `N2`, `N3`. The scheduler places them:

```
TM1                        TM2
├── Slot 1 → N0            ├── Slot 1 → N2
└── Slot 2 → N1            └── Slot 2 → N3
```

**Result:** 4 subtasks → 4 available slots → distributed across 2 TaskManagers.

---

## 7G. Subtask ≠ Slot

> **This is the most frequently misunderstood concept.**

**Setup:**

| Operator   | Parallelism | Subtasks |
|------------|:-----------:|:--------:|
| Source     | 2           | 2        |
| Normalize  | 2           | 2        |
| Validate   | 2           | 2        |
| **Total**  |             | **6**    |

This does **not** automatically mean 6 Task Slots are required.

**Why?** Because compatible operators can be **chained**, and **slot sharing** allows multiple operators to execute within the same scheduling allocation.

```
Chain 0 (Slot 1):                Chain 1 (Slot 2):

  Source-0                         Source-1
     ↓                                ↓
  Normalize-0                      Normalize-1
     ↓                                ↓
  Validate-0                       Validate-1
```

**Result:**

```
6 operator subtasks  →  2 execution chains  →  2 slots
```

> **Key Insight:** Counting operator subtasks and counting slots are **not the same thing**.

---

## 7H. Operator Chaining

Consider `Source → Normalize → Validate` with `Parallelism = 2` for all operators.

**Without chaining** — each operator passes data over the network:

```
Source
 ↓ ← network serialization/deserialization
Normalize
 ↓ ← network serialization/deserialization
Validate
```

**With chaining** — operators execute within the same task, passing data via method calls:

```
Chain 0:                          Chain 1:
Source-0 → Normalize-0 → Validate-0    Source-1 → Normalize-1 → Validate-1
```

Chaining **reduces unnecessary network communication and serialization** between compatible operators, executing them as a single fused task.

---

## 7I. Complete Example — 2 TMs × 2 Slots

**Infrastructure:**

```
TaskManager Count = 2
Slots per TM     = 2
Total Slots      = 2 × 2 = 4
```

```
                 JobManager
                      |
          ┌───────────┴───────────┐
          ↓                       ↓
       TaskManager 1          TaskManager 2
       ┌───────────┐          ┌───────────┐
       │ Slot 1    │          │ Slot 1    │
       │ Slot 2    │          │ Slot 2    │
       └───────────┘          └───────────┘
```

**Pipeline:** `N → M → O` with `Parallelism = 2`

Flink creates: `N0`, `N1`, `M0`, `M1`, `O0`, `O1`

**With chaining:**

```
TM1
├── Slot 1:  N0 → M0 → O0
└── Slot 2:  N1 → M1 → O1

TM2
├── Slot 1:  (empty)
└── Slot 2:  (empty)
```

> **Note:** Flink does **not** have to use every available slot. TM2 might have no work assigned for this particular job.

---

## 7J. Example With Two Kafka Topics

**Kafka Setup:**

```
Topic A                    Topic B
├── Partition A0           ├── Partition B0
└── Partition A1           └── Partition B1
```

**Flink Setup:**

| Source   | Parallelism | Subtasks |
|----------|:-----------:|:--------:|
| Source A | 2           | 2        |
| Source B | 2           | 2        |
| **Total**|             | **4**    |

**With 2 TaskManagers × 2 Slots = 4 Slots:**

```
TM1                                TM2
├── Slot 1 → A-Source-0            ├── Slot 1 → A-Source-1
└── Slot 2 → B-Source-0            └── Slot 2 → B-Source-1
```

> This is a conceptual placement. The actual scheduler determines the final assignment.

---

## 7K. Effect of Increasing Slots

**Before:**

```
2 TMs × 2 Slots = 4 Total Slots
```

**After:**

```
2 TMs × 4 Slots = 8 Total Slots
```

But if Kafka still has:
- Topic A → 2 partitions
- Topic B → 2 partitions
- Source parallelism → A=2, B=2

Then there are still only **4 active source subtasks**.

> **Key Insight:**
> 8 slots does **not** automatically mean 8 Kafka consumers processing 8 partitions. There are only 4 Kafka partitions. The additional slots provide **capacity/headroom** for other parallel operators, jobs, or downstream work.

---

## 7L. Effect of Increasing Parallelism

Now increase Kafka partitions **and** source parallelism:

```
Topic A: A0, A1, A2, A3    (4 partitions)
Topic B: B0, B1, B2, B3    (4 partitions)

Source A Parallelism = 4
Source B Parallelism = 4
Total Source Subtasks = 4 + 4 = 8
```

**With 2 TMs × 4 Slots = 8 Slots**, resources and parallelism are now aligned:

```
TM1                            TM2
├── Slot 1 → A0                ├── Slot 1 → A2
├── Slot 2 → A1                ├── Slot 2 → A3
├── Slot 3 → B0                ├── Slot 3 → B2
└── Slot 4 → B1                └── Slot 4 → B3
```

> This is an illustrative placement, not a permanent assignment.

---

## 7M. CPU Cores vs Task Slots

Suppose one TaskManager has **8 CPU cores** and is configured with **8 slots**.

A reasonable starting point for CPU-heavy workloads:

```
Slots ≈ CPU Cores
```

But this is **not** a Flink law.

> **⚠️ Slots do NOT map 1:1 to CPU cores:**
>
> | Incorrect Model            | Correct Model                         |
> |---------------------------|---------------------------------------|
> | Slot 1 = CPU Core 1       | Slots = Flink scheduling capacity     |
> | Slot 2 = CPU Core 2       | OS/JVM executes work on CPU threads   |
> | …                         | Thread scheduling is independent      |

---

## 7N. Why Slots Can Exceed CPU Cores

Consider an I/O-heavy pipeline:

```
Kafka → Normalize → Validate → Aerospike Async I/O → Merge
```

The Aerospike operation spends significant time **waiting for network I/O**:

```
CPU work
  ↓
Send request to Aerospike
  ↓
  WAIT  ← CPU is idle during this phase
  ↓
Response arrives
  ↓
CPU work
```

During the wait, the CPU does **not** need to remain busy executing that operation. Therefore, I/O-heavy workloads may benefit from **more slots than CPU cores**.

**Benchmarking approach for an 8-core machine:**

| Configuration | Metrics to Monitor                                    |
|:-------------:|-------------------------------------------------------|
| 8 slots       | CPU utilization, throughput, latency                  |
| 12 slots      | Kafka consumer lag, Flink backpressure                |
| 16 slots      | Aerospike latency, network utilization, memory, GC    |

The optimal value is determined **experimentally**.

---

## 7O. Practical Slot Sizing

There is **no universal formula**. A useful starting model:

```
S ≈ C / R
```

| Symbol | Meaning                                    |
|:------:|--------------------------------------------|
| S      | Number of slots                            |
| C      | CPU cores allocated to the TaskManager     |
| R      | Desired CPU capacity per slot              |

**Examples:**

| Cores | CPU per Slot (R) | Calculated Slots |
|:-----:|:----------------:|:----------------:|
| 16    | 1.0              | 16               |
| 16    | 0.5              | 32               |

> **Caution:** A higher slot count is not always better. Memory, network, external-system concurrency, and workload characteristics constrain the practical value.

---

## 7P. TaskManager Sizing

**Formula:**

```
Total Slots = TM Count × Slots per TM
```

**Example:**

| TM Count | Slots per TM | Total Slots |
|:--------:|:------------:|:-----------:|
| 4        | 8            | **32**      |

If the application requires approximately **24 execution slots**:

```
TM1 → 8 slots
TM2 → 8 slots
TM3 → 8 slots
TM4 → 8 slots
─────────────
Total: 32 slots (4 spare slots of capacity)
```

---

## 7Q. Parallelism vs Slots — The Key Relationship

**Setup:**

| Operator  | Parallelism | Subtasks |
|-----------|:-----------:|:--------:|
| Source    | 4           | 4        |
| Normalize | 4           | 4        |
| Validate  | 4           | 4        |
| Sink      | 4           | 4        |
| **Total** |             | **16**   |

If all four operators can be **chained**:

```
Chain 0: Source-0 → Normalize-0 → Validate-0 → Sink-0
Chain 1: Source-1 → Normalize-1 → Validate-1 → Sink-1
Chain 2: Source-2 → Normalize-2 → Validate-2 → Sink-2
Chain 3: Source-3 → Normalize-3 → Validate-3 → Sink-3
```

**Result:** ~4 parallel execution chains rather than 16 independent slots.

**The correct mental model:**

```
Parallelism
     ↓
Number of subtasks
     ↓
Chaining / slot sharing
     ↓
Execution chains / scheduling requirements
     ↓
TaskManager slots
```

> This is far more accurate than the naive model: `1 subtask = 1 slot`.

---

## 7R. Complete End-to-End Example

### Infrastructure

```
TaskManagers: 2
Slots per TM: 2
Total Slots:  2 × 2 = 4
```

### Kafka

```
Topic A → 2 partitions (A0, A1)
Topic B → 2 partitions (B0, B1)
```

### Flink Pipeline

| Operator      | Parallelism |
|---------------|:-----------:|
| Source A      | 2           |
| Source B      | 2           |
| Normalize     | 2           |
| Validate      | 2           |
| Correlation   | 2           |
| Sink          | 2           |

### Logical Graph

```
      Topic A                 Topic B
       A0 A1                   B0 B1
        ↓  ↓                    ↓  ↓
      Source A                Source B
       P=2                      P=2
          \                     /
           \                   /
            Normalize  P=2
                  ↓
            Validate   P=2
                  ↓
           Correlation P=2
                  ↓
               Sink    P=2
```

### Subtask Count

| Operator    | Subtasks |
|-------------|:--------:|
| Source A    | 2        |
| Source B    | 2        |
| Normalize   | 2        |
| Validate    | 2        |
| Correlation | 2        |
| Sink        | 2        |
| **Total**   | **12**   |

### With Chaining and Slot Sharing

These 12 logical subtasks do **not** require 12 slots. A possible execution structure:

```
Slot 1:                        Slot 2:
  Source-A-0                     Source-A-1
     ↓                              ↓
  Normalize-0                    Normalize-1
     ↓                              ↓
  Validate-0                     Validate-1
     ↓                              ↓
  Correlation-0                  Correlation-1
     ↓                              ↓
  Sink-0                          Sink-1
```

Source B's processing is placed according to the actual execution graph, slot-sharing groups, and scheduling constraints. The **JobManager's scheduler** determines the actual placement.

---

## 7S. TaskManager Failure Recovery

**Setup:**

```
TM1                           TM2
├── Slot 1 → Chain 0          ├── Slot 1 → Chain 2
└── Slot 2 → Chain 1          └── Slot 2 → Chain 3
```

**When TM1 fails (❌):**

The subtasks running on TM1 are lost. Flink's recovery process:

```
Checkpoint (consistent state snapshot)
     ↓
TM1 failure detected
     ↓
Job restart / recovery triggered
     ↓
State restored from checkpoint
     ↓
Subtasks recreated on available TaskManagers
     ↓
Processing continues
```

> This is one reason why **slots are not permanent homes** for subtasks.

---

## 7T. TaskManager Failure vs Server Failure

This distinction is critical for **production architecture**.

### Single Server, Single TaskManager

```
Server 1
└── TM1
```

- If `TM1` crashes → Flink can potentially restart/redeploy it.
- If `Server 1` crashes → TaskManager is also gone.

### Multiple Servers (Recommended)

```
Server 1 → TM1
Server 2 → TM2
Server 3 → TM3
```

If `Server 2` fails:

| Component | Status |
|-----------|--------|
| TM1       | ✅ Available |
| TM2       | ❌ Lost      |
| TM3       | ✅ Available |

This provides actual **machine-level failure isolation**.

---

## 7U. Recommended Mental Model

```
                   JOB
                    │
                    ▼
               PARALLELISM
                    │
                    ▼
             creates SUBTASKS
                    │
                    ▼
         ┌──────────┴──────────┐
         │                     │
      Subtask 0             Subtask 1
         │                     │
         ▼                     ▼
    Scheduler              Scheduler
         │                     │
         └──────────┬──────────┘
                    ▼
                TASK SLOTS
                    │
         ┌──────────┴──────────┐
         ▼                     ▼
     TaskManager 1         TaskManager 2
```

> **In one sentence:**
> Parallelism determines how many subtasks Flink creates; the scheduler places those subtasks into available TaskManager slots; TaskManagers provide the actual runtime resources needed to execute them.

---

## 7V. The Five Equations to Remember

### 1. Total Slots

```
S_total = TM_count × S_per_TM
```

*Example: 4 × 8 = 32 total slots*

### 2. Subtasks per Operator

```
N_subtasks = P_operator
```

*Example: P = 8 → 8 subtasks*

### 3. Source Parallelism Bound

```
P_source ≤ N_kafka_partitions
```

*Kafka partitions bound the number of simultaneously active source partition assignments in a consumer group.*

### 4. Approximate Slot Sizing

```
S ≈ C / R
```

*Where C = CPU capacity, R = desired CPU capacity per slot.*
*This is a starting point, not a mandatory formula.*

### 5. Logical Subtask Count

```
N_logical_subtasks = Σ P_i    (for operators O₁, O₂, …, Oₙ)
```

> **But critically:**
> ```
> N_logical_subtasks ≠ N_required_slots
> ```
> Because operator chaining and slot sharing affect how execution is scheduled.

---

## Final Practical Picture

For the **event-correlation architecture**, the full hierarchy:

```
                    FLINK JOB
                         │
                         ▼
                 Execution Graph
                         │
              ┌──────────┴──────────┐
              ▼                     ▼
         Parallelism            Parallelism
              │                     │
              ▼                     ▼
        Source subtasks       Correlation subtasks
              │                     │
              └──────────┬──────────┘
                         ▼
                    Scheduler
                         │
              assigns execution to
                         │
                         ▼
                   TASK SLOTS
                         │
             ┌───────────┼───────────┐
             ▼           ▼           ▼
            TM1         TM2         TM3
             │           │           │
          CPU/RAM     CPU/RAM     CPU/RAM
             │           │           │
             └───────────┼───────────┘
                         ▼
                  Actual processing
                         │
                         ▼
              Kafka / Aerospike / Sink
```

**The key conceptual chain:**

```
Kafka partitions → Source parallelism → Subtasks → Scheduler → TaskManager slots → CPU/Memory/Network → Actual processing
```

> [!CAUTION]
> **Do not** design a Flink cluster by saying *"I have 16 CPU cores, therefore I need 16 slots."*
>
> Instead:
> 1. Treat slots as Flink **scheduling capacity**
> 2. Choose an initial slot/CPU configuration
> 3. **Validate experimentally** using: throughput, CPU utilization, backpressure, latency, memory, network, Kafka lag, and — particularly for this architecture — **Aerospike I/O concurrency**

---

*End of Chapter 7*
