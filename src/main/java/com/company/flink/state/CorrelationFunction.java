package com.company.flink.state;

import com.company.flink.config.FlinkConfig;
import com.company.flink.model.DLQEvent;
import com.company.flink.model.JoinedShadowEvent;
import com.company.flink.model.ShadowEvent;
import com.company.flink.util.JsonUtil;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * High-performance stateful correlation operator partitioned by keyBy(common_id).
 * Maintains separate RocksDB ValueState for Source A and Source B.
 * Registers timeout timers and emits JoinedShadowEvent upon match or DLQEvent upon timeout.
 */
public class CorrelationFunction extends KeyedProcessFunction<String, ShadowEvent, JoinedShadowEvent> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(CorrelationFunction.class);

    private final FlinkConfig flinkConfig;
    private final OutputTag<DLQEvent> dlqOutputTag;

    private transient ValueState<ShadowEvent> stateA;
    private transient ValueState<ShadowEvent> stateB;

    private transient Counter matchCounter;
    private transient Counter timeoutCounter;
    private transient Counter duplicateCounter;
    private transient Counter pendingEventsCounter;

    public CorrelationFunction(FlinkConfig flinkConfig, OutputTag<DLQEvent> dlqOutputTag) {
        this.flinkConfig = flinkConfig;
        this.dlqOutputTag = dlqOutputTag;
    }

    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<ShadowEvent> descriptorA = new ValueStateDescriptor<>("stateA", Types.POJO(ShadowEvent.class));
        ValueStateDescriptor<ShadowEvent> descriptorB = new ValueStateDescriptor<>("stateB", Types.POJO(ShadowEvent.class));

        this.stateA = getRuntimeContext().getState(descriptorA);
        this.stateB = getRuntimeContext().getState(descriptorB);

        this.matchCounter = getRuntimeContext().getMetricGroup().counter("correlation_matches_total");
        this.timeoutCounter = getRuntimeContext().getMetricGroup().counter("correlation_timeouts_total");
        this.duplicateCounter = getRuntimeContext().getMetricGroup().counter("correlation_duplicates_total");
        this.pendingEventsCounter = getRuntimeContext().getMetricGroup().counter("correlation_pending_events");
    }

    @Override
    public void processElement(ShadowEvent incoming, Context ctx, Collector<JoinedShadowEvent> out) throws Exception {
        String commonId = ctx.getCurrentKey();
        long currentProcessingTime = ctx.timerService().currentProcessingTime();
        String sourceType = incoming.getSourceType();

        if ("A".equals(sourceType)) {
            ShadowEvent currentB = stateB.value();
            if (currentB != null) {
                // MATCH FOUND: A joins with previously stored B
                emitMatch(commonId, incoming.getPayloadId(), currentB.getPayloadId(), currentB.getIngestionTimestamp(), incoming.getIngestionTimestamp(), ctx, out);
            } else {
                ShadowEvent existingA = stateA.value();
                if (existingA != null && existingA.getPayloadId().equals(incoming.getPayloadId())) {
                    duplicateCounter.inc();
                    LOG.debug("Duplicate A shadow event received for commonId: {}", commonId);
                    return;
                }
                // Store A in state and register timeout timer
                stateA.update(incoming);
                pendingEventsCounter.inc();
                registerTimeoutTimer(ctx, currentProcessingTime);
            }
        } else if ("B".equals(sourceType)) {
            ShadowEvent currentA = stateA.value();
            if (currentA != null) {
                // MATCH FOUND: B joins with previously stored A
                emitMatch(commonId, currentA.getPayloadId(), incoming.getPayloadId(), currentA.getIngestionTimestamp(), incoming.getIngestionTimestamp(), ctx, out);
            } else {
                ShadowEvent existingB = stateB.value();
                if (existingB != null && existingB.getPayloadId().equals(incoming.getPayloadId())) {
                    duplicateCounter.inc();
                    LOG.debug("Duplicate B shadow event received for commonId: {}", commonId);
                    return;
                }
                // Store B in state and register timeout timer
                stateB.update(incoming);
                pendingEventsCounter.inc();
                registerTimeoutTimer(ctx, currentProcessingTime);
            }
        } else {
            LOG.error("Unknown source type '{}' for commonId: {}", sourceType, commonId);
        }
    }

    private void emitMatch(String commonId, String payloadIdA, String payloadIdB, long firstIngest, long secondIngest, Context ctx, Collector<JoinedShadowEvent> out) throws Exception {
        long now = System.currentTimeMillis();
        long latency = Math.abs(secondIngest - firstIngest);

        JoinedShadowEvent joined = new JoinedShadowEvent(commonId, payloadIdA, payloadIdB, now, latency);
        matchCounter.inc();
        pendingEventsCounter.dec();

        // Clean up RocksDB state on successful join
        stateA.clear();
        stateB.clear();

        out.collect(joined);
    }

    private void registerTimeoutTimer(Context ctx, long currentProcessingTime) {
        long timerTimestamp = currentProcessingTime + (flinkConfig.getCorrelationTimeoutSeconds() * 1000L);
        ctx.timerService().registerProcessingTimeTimer(timerTimestamp);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<JoinedShadowEvent> out) throws Exception {
        String commonId = ctx.getCurrentKey();
        ShadowEvent a = stateA.value();
        ShadowEvent b = stateB.value();

        if (a != null || b != null) {
            timeoutCounter.inc();
            pendingEventsCounter.dec();

            ShadowEvent orphan = (a != null) ? a : b;
            String originalJson = JsonUtil.toJson(orphan);

            DLQEvent dlqEvent = DLQEvent.builder()
                    .commonId(commonId)
                    .payloadId(orphan.getPayloadId())
                    .sourceType(orphan.getSourceType())
                    .originalEvent(originalJson)
                    .errorMessage("Correlation timed out after " + flinkConfig.getCorrelationTimeoutSeconds() + " seconds without matching event")
                    .errorType("CORRELATION_TIMEOUT")
                    .processingStage("STATEFUL_JOIN")
                    .exceptionClass("CorrelationTimeoutException")
                    .firstFailureTimestamp(orphan.getIngestionTimestamp())
                    .lastFailureTimestamp(timestamp)
                    .build();

            ctx.output(dlqOutputTag, dlqEvent);

            // Clean up state on expiration
            stateA.clear();
            stateB.clear();
        }
    }
}
