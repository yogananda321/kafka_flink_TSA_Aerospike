#!/usr/bin/env python3
import json
import math
import random
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from kafka import KafkaConsumer, KafkaProducer

BOOTSTRAP_SERVERS = 'localhost:9092'
TOPIC_A = 'topic_a'
TOPIC_B = 'topic_b'
TOPIC_MERGED = 'merged_events'
DLQ_TOPICS = [
    'validation_dlq',
    'aerospike_dlq',
    'correlation_dlq',
    'payload_fetch_dlq',
    'processing_dlq'
]

def main():
    print("=" * 70)
    print("      ENTERPRISE FLINK CORRELATION PIPELINE — BENCHMARK RUNNER      ")
    print("=" * 70)

    # CLI arguments or interactive prompts
    def parse_int(val, default):
        import re
        clean = re.sub(r'[^\d]', '', str(val))
        return int(clean) if clean else default

    if len(sys.argv) >= 4:
        eps = parse_int(sys.argv[1], 1000)
        duration_sec = parse_int(sys.argv[2], 30)
        event_size_bytes = parse_int(sys.argv[3], 1024)
    else:
        try:
            eps_input = input("Enter target EPS per producer [default: 1000]: ").strip()
            eps = parse_int(eps_input, 1000)

            duration_input = input("Enter test duration in seconds [default: 30]: ").strip()
            duration_sec = parse_int(duration_input, 30)

            size_input = input("Enter event payload size in bytes [default: 1024]: ").strip()
            event_size_bytes = parse_int(size_input, 1024)
        except (KeyboardInterrupt, EOFError):
            print("\nBenchmark cancelled.")
            sys.exit(0)

    total_events_per_topic = eps * duration_sec
    orphan_count_per_topic = max(1, total_events_per_topic // 100)
    total_orphans_injected = orphan_count_per_topic * 2

    print("\n" + "-" * 70)
    print(f"[*] Benchmark Configuration:")
    print(f"    - Target EPS per Producer  : {eps} events/sec")
    print(f"    - Duration                 : {duration_sec} seconds")
    print(f"    - Payload Size per Event   : {event_size_bytes} bytes")
    print(f"    - Target Matching Event Pairs: {total_events_per_topic} pairs")
    print(f"    - Injected Orphan IDs (100:1): {total_orphans_injected} non-common events ({orphan_count_per_topic} A + {orphan_count_per_topic} B)")
    print(f"    - Total Input Events       : {total_events_per_topic * 2 + total_orphans_injected} events")
    print("-" * 70 + "\n")

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: v.encode('utf-8') if isinstance(v, str) else json.dumps(v).encode('utf-8'),
        acks=1,
        linger_ms=5,
        batch_size=131072
    )

    # Unique IMSI pool for 1-to-1 exact correlation matching
    session_prefix = f"404{int(time.time()) % 100000:05d}"
    imsis = [f"{session_prefix}{i+1:08d}" for i in range(total_events_per_topic)]
    
    # Generate non-matching orphan IMSIs (999... for A, 888... for B)
    orphan_ids_a = [f"999999{i+1:08d}" for i in range(orphan_count_per_topic)]
    orphan_ids_b = [f"888888{i+1:08d}" for i in range(orphan_count_per_topic)]

    # Store publish timestamps for E2E latency calculation
    ingest_timestamps = {}

    ids_a = imsis.copy()
    ids_b = imsis.copy()

    # Inject 100:1 orphans into list
    for idx, orphan in enumerate(orphan_ids_a):
        insert_pos = min(len(ids_a), (idx + 1) * 100)
        ids_a.insert(insert_pos, orphan)

    for idx, orphan in enumerate(orphan_ids_b):
        insert_pos = min(len(ids_b), (idx + 1) * 100)
        ids_b.insert(insert_pos, orphan)

    cells = [f"LTE_{i:04d}" for i in range(5000, 5200)]
    mmes = ["MME_01", "MME_02", "MME_03", "MME_04"]
    tais = ["Chennai_125", "Mumbai_402", "Delhi_711", "Bangalore_330", "Hyderabad_821"]
    procedures = ["Attach Request", "Service Request", "Tracking Area Update", "Handover Request"]

    # Pre-allocate padding string once outside the loop to eliminate Python GC string allocation overhead
    base_sample_a = f"Timestamp: 10:15:34.999 UE: IMSI 404450000000000 Cell: LTE_5123 Event: Measurement Report RSRP: -95 dBm RSRQ: -12 dB Neighbor Cell: LTE_5124"
    sample_len_a = len(base_sample_a.encode('utf-8'))
    pad_len_a = max(0, event_size_bytes - sample_len_a - 10)
    padding_a = "A" * pad_len_a

    base_sample_b = f"Timestamp: 10:15:33.999 IMSI: 404450000000000 Procedure: Attach Request MME_ID: MME_01 TAI: Chennai_125 Result: Success"
    sample_len_b = len(base_sample_b.encode('utf-8'))
    pad_len_b = max(0, event_size_bytes - sample_len_b - 10)
    padding_b = "B" * pad_len_b

    def produce_topic_parallel(topic, id_list, padding_char, prefix_filter):
        start = time.time()
        total_items = len(id_list)
        num_workers = 4
        chunk_size = math.ceil(total_items / num_workers)

        if padding_char == 'A':
            pad_str = padding_a
        else:
            pad_str = padding_b

        def worker(worker_id, items):
            worker_producer = KafkaProducer(
                bootstrap_servers=BOOTSTRAP_SERVERS,
                value_serializer=lambda v: v.encode('utf-8') if isinstance(v, str) else json.dumps(v).encode('utf-8'),
                acks=1,
                linger_ms=5,
                batch_size=131072
            )
            worker_start = time.time()
            interval = 1.0 / (eps / num_workers) if eps > 0 else 0
            for idx, imsi in enumerate(items, start=1):
                now_ms = int(time.time() * 1000)
                if imsi not in ingest_timestamps and not imsi.startswith(prefix_filter):
                    ingest_timestamps[imsi] = now_ms

                if padding_char == 'A':
                    base_str = f"Timestamp: 10:15:34.{idx % 999:03d} UE: IMSI {imsi} Cell: LTE_5123 Event: Measurement Report RSRP: -95 dBm RSRQ: -12 dB Neighbor Cell: LTE_5124 Padding: {pad_str}" if pad_str else f"Timestamp: 10:15:34.{idx % 999:03d} UE: IMSI {imsi} Cell: LTE_5123 Event: Measurement Report RSRP: -95 dBm RSRQ: -12 dB Neighbor Cell: LTE_5124"
                else:
                    base_str = f"Timestamp: 10:15:33.{idx % 999:03d} IMSI: {imsi} Procedure: Attach Request MME_ID: MME_01 TAI: Chennai_125 Result: Success Padding: {pad_str}" if pad_str else f"Timestamp: 10:15:33.{idx % 999:03d} IMSI: {imsi} Procedure: Attach Request MME_ID: MME_01 TAI: Chennai_125 Result: Success"

                worker_producer.send(topic, key=imsi.encode('utf-8'), value=base_str)

                if idx % 100 == 0 and interval > 0:
                    expected_elapsed = idx * interval
                    actual_elapsed = time.time() - worker_start
                    if expected_elapsed > actual_elapsed:
                        time.sleep(expected_elapsed - actual_elapsed)

            worker_producer.flush()
            worker_producer.close()
            return len(items)

        chunks = [id_list[i:i + chunk_size] for i in range(0, total_items, chunk_size)]
        with ThreadPoolExecutor(max_workers=num_workers) as pool:
            futures = [pool.submit(worker, i, chunk) for i, chunk in enumerate(chunks)]
            results = [f.result() for f in futures]

        elapsed = time.time() - start
        return sum(results), elapsed

    def produce_a():
        return produce_topic_parallel(TOPIC_A, ids_a, 'A', "999999")

    def produce_b():
        return produce_topic_parallel(TOPIC_B, ids_b, 'B', "888888")

    # Instantiate Consumer BEFORE starting producers to avoid race conditions
    consumer = KafkaConsumer(
        TOPIC_MERGED,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id=f'benchmark-consumer-{int(time.time() * 1000)}',
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        consumer_timeout_ms=8000
    )

    print(f"[*] Launching parallel producers for Topic A and Topic B...")
    bench_start_time = time.time()

    with ThreadPoolExecutor(max_workers=2) as executor:
        fut_a = executor.submit(produce_a)
        fut_b = executor.submit(produce_b)
        count_a, time_a = fut_a.result()
        count_b, time_b = fut_b.result()

    eps_a = count_a / time_a if time_a > 0 else 0
    eps_b = count_b / time_b if time_b > 0 else 0

    print(f"[OK] Source A Produced: {count_a} events in {time_a:.2f}s ({eps_a:.2f} EPS)")
    print(f"[OK] Source B Produced: {count_b} events in {time_b:.2f}s ({eps_b:.2f} EPS)")
    print(f"[*] Waiting for Flink correlation pipeline to process & sink joined events...")

    joined_records = {}
    latencies_ms = []

    for msg in consumer:
        val = msg.value
        cid = val.get("common_id")
        if cid and cid in ingest_timestamps:
            merged_ts = val.get("merged_timestamp", int(time.time() * 1000))
            ingest_ts = ingest_timestamps[cid]
            latency = max(0, merged_ts - ingest_ts)
            
            joined_records[cid] = val
            latencies_ms.append(latency)

            if len(joined_records) >= total_events_per_topic:
                break

    bench_total_time = time.time() - bench_start_time
    consumer.close()
    producer.close()

    # Check DLQ counts across all DLQ topics
    dlq_total_count = 0
    try:
        dlq_consumer = KafkaConsumer(
            *DLQ_TOPICS,
            bootstrap_servers=BOOTSTRAP_SERVERS,
            auto_offset_reset='earliest',
            consumer_timeout_ms=1500,
            group_id=f'dlq-checker-{int(time.time())}'
        )
        for _ in dlq_consumer:
            dlq_total_count += 1
        dlq_consumer.close()
    except Exception:
        pass

    joined_count = len(joined_records)
    joins_per_sec = joined_count / bench_total_time if bench_total_time > 0 else 0
    avg_latency = (sum(latencies_ms) / len(latencies_ms)) if latencies_ms else 0.0

    print("\n" + "=" * 70)
    print("          BENCHMARK VERIFICATION & METRICS SUMMARY REPORT          ")
    print("=" * 70)
    print(f"  Target Event Pairs               : {total_events_per_topic}")
    print(f"  Source A Events Produced         : {count_a} ({eps_a:.2f} EPS)")
    print(f"  Source B Events Produced         : {count_b} ({eps_b:.2f} EPS)")
    print(f"  Joined & Merged Events Received  : {joined_count}")
    print(f"  Effective Joins Per Second       : {joins_per_sec:.2f} joins/sec")
    print(f"  Avg E2E Producer-to-Sink Latency : {avg_latency:.2f} ms")
    print(f"  DLQ Failure Count                : {dlq_total_count}")
    print("=" * 70)

    if joined_count >= total_events_per_topic:
        print("[SUCCESS] 100% of events successfully joined and merged with zero data loss!")
    else:
        loss = total_events_per_topic - joined_count
        print(f"[WARNING] Processed {joined_count}/{total_events_per_topic} (Missing: {loss} events).")
    print("=" * 70)

if __name__ == '__main__':
    main()
