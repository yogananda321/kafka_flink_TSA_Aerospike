package com.company.flink.config;

import java.io.Serializable;

/**
 * Flink execution runtime parameters, checkpointing policies, RocksDB options, and timers.
 */
public final class FlinkConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private final int defaultParallelism;
    private final long checkpointIntervalMs;
    private final long checkpointTimeoutMs;
    private final long checkpointMinPauseMs;
    private final int maxConcurrentCheckpoints;
    private final int tolerableFailedCheckpoints;
    private final String checkpointStorageDirectory;
    private final boolean rocksDbIncremental;
    private final long correlationTimeoutSeconds;
    private final String timerType; // PROCESSING_TIME or EVENT_TIME
    private final long maxOutOfOrdernessSeconds;

    public FlinkConfig(
            int defaultParallelism,
            long checkpointIntervalMs,
            long checkpointTimeoutMs,
            long checkpointMinPauseMs,
            int maxConcurrentCheckpoints,
            int tolerableFailedCheckpoints,
            String checkpointStorageDirectory,
            boolean rocksDbIncremental,
            long correlationTimeoutSeconds,
            String timerType,
            long maxOutOfOrdernessSeconds) {
        this.defaultParallelism = defaultParallelism;
        this.checkpointIntervalMs = checkpointIntervalMs;
        this.checkpointTimeoutMs = checkpointTimeoutMs;
        this.checkpointMinPauseMs = checkpointMinPauseMs;
        this.maxConcurrentCheckpoints = maxConcurrentCheckpoints;
        this.tolerableFailedCheckpoints = tolerableFailedCheckpoints;
        this.checkpointStorageDirectory = checkpointStorageDirectory;
        this.rocksDbIncremental = rocksDbIncremental;
        this.correlationTimeoutSeconds = correlationTimeoutSeconds;
        this.timerType = timerType;
        this.maxOutOfOrdernessSeconds = maxOutOfOrdernessSeconds;
    }

    public int getDefaultParallelism() { return defaultParallelism; }
    public long getCheckpointIntervalMs() { return checkpointIntervalMs; }
    public long getCheckpointTimeoutMs() { return checkpointTimeoutMs; }
    public long getCheckpointMinPauseMs() { return checkpointMinPauseMs; }
    public int getMaxConcurrentCheckpoints() { return maxConcurrentCheckpoints; }
    public int getTolerableFailedCheckpoints() { return tolerableFailedCheckpoints; }
    public String getCheckpointStorageDirectory() { return checkpointStorageDirectory; }
    public boolean isRocksDbIncremental() { return rocksDbIncremental; }
    public long getCorrelationTimeoutSeconds() { return correlationTimeoutSeconds; }
    public String getTimerType() { return timerType; }
    public long getMaxOutOfOrdernessSeconds() { return maxOutOfOrdernessSeconds; }
}
