package com.company.flink.metrics;

import java.io.Serializable;

/**
 * Metric names and constants utilized throughout the pipeline.
 */
public final class PipelineMetrics implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final String GROUP_CORRELATION = "correlation";
    public static final String GROUP_AEROSPIKE = "aerospike";
    public static final String GROUP_INGESTION = "ingestion";

    public static final String METRIC_MATCH_TOTAL = "matches_total";
    public static final String METRIC_TIMEOUT_TOTAL = "timeouts_total";
    public static final String METRIC_DUPLICATES_TOTAL = "duplicates_total";
    public static final String METRIC_PENDING_EVENTS = "pending_events";

    public static final String METRIC_AEROSPIKE_WRITE_SUCCESS = "write_success_total";
    public static final String METRIC_AEROSPIKE_WRITE_FAILURE = "write_failure_total";
    public static final String METRIC_AEROSPIKE_FETCH_SUCCESS = "fetch_success_total";
    public static final String METRIC_AEROSPIKE_FETCH_FAILURE = "fetch_failure_total";
    public static final String METRIC_AEROSPIKE_MISSING_PAYLOAD = "missing_payload_total";

    private PipelineMetrics() {}
}
