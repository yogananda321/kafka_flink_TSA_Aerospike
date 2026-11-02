#!/usr/bin/env python3
import json
import random
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from kafka import KafkaProducer, KafkaConsumer

BOOTSTRAP_SERVERS = '127.0.0.1:9092'
TOPIC_A = 'topic_a'
TOPIC_B = 'topic_b'
TOPIC_SINK = 'merged_events'
TOTAL_EVENTS = 10000  # Configurable event count (e.g. 1000, 5000, 50000)

print(f"======================================================================")
print(f"[*] High-Throughput Telecom Async Join Stress Test ({TOTAL_EVENTS} Events)")
print(f"======================================================================")

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: v.encode('utf-8') if isinstance(v, str) else json.dumps(v).encode('utf-8'),
    acks='all'
)

# 1. Prepare Telecom IMSI common IDs (e.g. 404450000000001 to 404450000001000)
base_imsi = 404450000000000
imsis = [str(base_imsi + i) for i in range(1, TOTAL_EVENTS + 1)]

# Shuffle the order of emission independently for Topic A and Topic B
# to test order-invariance (A first vs B first arrival)
ids_for_a = imsis.copy()
ids_for_b = imsis.copy()
random.shuffle(ids_for_a)
random.shuffle(ids_for_b)

tai_list = ["Chennai_125", "Mumbai_402", "Delhi_711", "Bangalore_330", "Hyderabad_821"]
mme_list = ["MME_01", "MME_02", "MME_03", "MME_04"]
cell_list = [f"LTE_{i:04d}" for i in range(5000, 5200)]
procedure_list = ["Attach Request", "Service Request", "Tracking Area Update", "Handover Request"]

def produce_topic_a():
    print(f"[*] Thread A: Starting asynchronous production of {TOTAL_EVENTS} RAN events to '{TOPIC_A}'...")
    start_time = time.time()
    for idx, imsi in enumerate(ids_for_a, start=1):
        cell = random.choice(cell_list)
        neighbor = random.choice(cell_list)
        rsrp = f"-{random.randint(75, 120)} dBm"
        rsrq = f"-{random.randint(8, 20)} dB"
        ts = f"10:15:{random.randint(10, 59)}.{random.randint(100, 999)}"
        
        # Exact Telecom RAN Measurement Report string format:
        event_a_str = f"Timestamp: {ts} UE: IMSI {imsi} Cell: {cell} Event: Measurement Report RSRP: {rsrp} RSRQ: {rsrq} Neighbor Cell: {neighbor}"
        
        producer.send(TOPIC_A, key=imsi.encode('utf-8'), value=event_a_str)
        if idx % 500 == 0 or idx == TOTAL_EVENTS:
            print(f"    -> [Topic A] Produced {idx}/{TOTAL_EVENTS} records")
        time.sleep(0.001)  # sub-millisecond network jitter
    producer.flush()
    print(f"[OK] Thread A completed in {time.time() - start_time:.2f}s")

def produce_topic_b():
    print(f"[*] Thread B: Starting asynchronous production of {TOTAL_EVENTS} MME events to '{TOPIC_B}'...")
    start_time = time.time()
    for idx, imsi in enumerate(ids_for_b, start=1):
        proc = random.choice(procedure_list)
        mme = random.choice(mme_list)
        tai = random.choice(tai_list)
        ts = f"10:15:{random.randint(10, 59)}.{random.randint(100, 999)}"
        
        # Exact Telecom MME Core Network string format:
        event_b_str = f"Timestamp: {ts} IMSI: {imsi} Procedure: {proc} MME_ID: {mme} TAI: {tai} Result: Success"
        
        producer.send(TOPIC_B, key=imsi.encode('utf-8'), value=event_b_str)
        if idx % 500 == 0 or idx == TOTAL_EVENTS:
            print(f"    -> [Topic B] Produced {idx}/{TOTAL_EVENTS} records")
        time.sleep(0.001)
    producer.flush()
    print(f"[OK] Thread B completed in {time.time() - start_time:.2f}s")

# 2. Launch consumer on merged_events sink
consumer = KafkaConsumer(
    TOPIC_SINK,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id=f'telecom-verifier-{int(time.time())}',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    consumer_timeout_ms=15000  # 15s idle timeout
)

print(f"[*] Starting simultaneous parallel Telecom producers...")
start_join_time = time.time()

with ThreadPoolExecutor(max_workers=2) as executor:
    fut_a = executor.submit(produce_topic_a)
    fut_b = executor.submit(produce_topic_b)
    fut_a.result()
    fut_b.result()

print(f"[*] All {TOTAL_EVENTS * 2} events published. Consuming and validating joined events from '{TOPIC_SINK}'...")

received_joined = {}
sample_printed = 0
for msg in consumer:
    val = msg.value
    cid = val.get("common_id")
    if cid and cid.startswith("404450"):
        # Validate merged payload integrity across RAN (Topic A) and MME (Topic B)
        data = val.get("data", {})
        has_ran_fields = "Cell" in data or "RSRP" in data or "UE" in data
        has_mme_fields = "Procedure" in data or "MME_ID" in data or "TAI" in data
        
        if has_ran_fields and has_mme_fields:
            received_joined[cid] = val
            if sample_printed < 3:
                sample_printed += 1
                print(f"    [Sample Joined Telecom Event #{sample_printed}]: Key(IMSI)={cid}")
                print(f"        Payload: {json.dumps(data)}")

        if len(received_joined) >= TOTAL_EVENTS:
            print(f"    [OK] Reached 100% target ({TOTAL_EVENTS}/{TOTAL_EVENTS} joined events received)")
            break

total_elapsed = time.time() - start_join_time
consumer.close()
producer.close()

# 3. Print Final Invariant Verification Report
print("\n" + "=" * 70)
print("          TELECOM IMSI CORRELATION JOIN VERIFICATION REPORT          ")
print("=" * 70)
print(f"  Target Event Pairs               : {TOTAL_EVENTS}")
print(f"  Topic A RAN Events Produced      : {TOTAL_EVENTS}")
print(f"  Topic B MME Events Produced      : {TOTAL_EVENTS}")
print(f"  Joined & Merged Events Received  : {len(received_joined)}")
match_rate = (len(received_joined) / TOTAL_EVENTS) * 100.0 if TOTAL_EVENTS > 0 else 0
print(f"  Match & Consistency Rate         : {match_rate:.2f}%")
print(f"  Total Processing & Verify Time   : {total_elapsed:.2f} seconds")
throughput = (len(received_joined) / total_elapsed) if total_elapsed > 0 else 0
print(f"  Effective End-to-End Throughput   : {throughput:.2f} joins/second")
print("=" * 70)

if len(received_joined) == TOTAL_EVENTS:
    print("[SUCCESS] 100% of asynchronous Telecom RAN/MME events successfully")
    print("          correlated on IMSI, merged, and published with zero loss!")
else:
    missing_count = TOTAL_EVENTS - len(received_joined)
    print(f"[FAIL] Incomplete: Missing {missing_count} joined events.")
print("=" * 70)
