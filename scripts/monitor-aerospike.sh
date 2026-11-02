#!/usr/bin/env bash
# Real-time Aerospike Read & Write Live Telemetry Monitor

INTERVAL=${1:-1}

echo -e "\033[36m==================================================================\033[0m"
echo -e "\033[36m   AEROSPIKE REAL-TIME READ & WRITE LIVE MONITOR (1-sec refresh)  \033[0m"
echo -e "\033[36m==================================================================\033[0m"
echo -e "Press Ctrl+C to stop monitoring.\n"

get_stat() {
    docker exec aerospike asinfo -v "namespace/test" 2>/dev/null
}

parse_val() {
    echo "$1" | grep -o "$2=[0-9]*" | cut -d= -f2
}

RAW=$(get_stat)
PREV_W=$(parse_val "$RAW" "client_write_success")
PREV_R=$(parse_val "$RAW" "client_read_success")
PREV_O=$(parse_val "$RAW" "objects")
PREV_W=${PREV_W:-0}
PREV_R=${PREV_R:-0}
PREV_O=${PREV_O:-0}

while true; do
    sleep "$INTERVAL"
    NOW=$(date +"%H:%M:%S")
    RAW=$(get_stat)
    CURR_W=$(parse_val "$RAW" "client_write_success")
    CURR_R=$(parse_val "$RAW" "client_read_success")
    CURR_O=$(parse_val "$RAW" "objects")
    CURR_W=${CURR_W:-0}
    CURR_R=${CURR_R:-0}
    CURR_O=${CURR_O:-0}

    DIFF_W=$(( (CURR_W - PREV_W) / INTERVAL ))
    DIFF_R=$(( (CURR_R - PREV_R) / INTERVAL ))

    printf "\033[36m[%s]\033[0m Writes/sec: \033[32m%6d\033[0m ops/s | Reads/sec: \033[33m%6d\033[0m ops/s | Total Writes: %'d | Total Reads: %'d | Stored Objects: %'d\n" \
        "$NOW" "$DIFF_W" "$DIFF_R" "$CURR_W" "$CURR_R" "$CURR_O"

    PREV_W=$CURR_W
    PREV_R=$CURR_R
    PREV_O=$CURR_O
done
