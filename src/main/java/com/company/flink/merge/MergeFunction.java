package com.company.flink.merge;

import com.company.flink.model.DLQEvent;
import com.company.flink.model.MergedEvent;
import com.company.flink.model.PayloadEvent;
import com.company.flink.util.JsonUtil;
import java.util.Map;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Merges PayloadEvent A and PayloadEvent B into the canonical MergedEvent.
 */
public class MergeFunction extends ProcessFunction<Tuple2<PayloadEvent, PayloadEvent>, MergedEvent> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(MergeFunction.class);

    private final ConflictResolutionStrategy strategy;
    private final OutputTag<DLQEvent> dlqOutputTag;

    public MergeFunction(ConflictResolutionStrategy strategy, OutputTag<DLQEvent> dlqOutputTag) {
        this.strategy = strategy;
        this.dlqOutputTag = dlqOutputTag;
    }

    public MergeFunction(OutputTag<DLQEvent> dlqOutputTag) {
        this(ConflictResolutionStrategy.create(ConflictResolutionStrategy.StrategyType.DEEP_MERGE), dlqOutputTag);
    }

    @Override
    public void processElement(Tuple2<PayloadEvent, PayloadEvent> pair, Context ctx, Collector<MergedEvent> out) {
        long startMs = System.currentTimeMillis();
        PayloadEvent a = pair.f0;
        PayloadEvent b = pair.f1;

        if (a == null || b == null) {
            String commonId = (a != null) ? a.getCommonId() : (b != null ? b.getCommonId() : "UNKNOWN");
            DLQEvent dlqEvent = DLQEvent.builder()
                    .commonId(commonId)
                    .errorMessage("One of the payload events is null in merge pair")
                    .errorType("NULL_MERGE_OPERAND")
                    .processingStage("MERGE")
                    .exceptionClass("NullPointerException")
                    .firstFailureTimestamp(startMs)
                    .lastFailureTimestamp(System.currentTimeMillis())
                    .build();
            ctx.output(dlqOutputTag, dlqEvent);
            return;
        }

        try {
            String padA = null;
            if (a.getData() != null) {
                Object p = a.getData().get("padding");
                if (p == null) p = a.getData().get("Padding");
                if (p != null) padA = String.valueOf(p);
            }

            String padB = null;
            if (b.getData() != null) {
                Object p = b.getData().get("padding");
                if (p == null) p = b.getData().get("Padding");
                if (p != null) padB = String.valueOf(p);
            }

            Map<String, Object> dataA = a.getData();
            Map<String, Object> dataB = b.getData();

            String rawA = (dataA != null && dataA.get("raw_event") != null) ? String.valueOf(dataA.get("raw_event")) : null;
            String rawB = (dataB != null && dataB.get("raw_event") != null) ? String.valueOf(dataB.get("raw_event")) : null;

            int sizeA = rawA != null ? rawA.getBytes(java.nio.charset.StandardCharsets.UTF_8).length : (padA != null ? padA.length() + 200 : 1024);
            int sizeB = rawB != null ? rawB.getBytes(java.nio.charset.StandardCharsets.UTF_8).length : (padB != null ? padB.length() + 200 : 1024);

            Map<String, Object> mergedData = strategy.resolve(dataA, dataB, a.getPayloadId(), b.getPayloadId());
            if (padA != null) {
                mergedData.put("padding_a", padA);
            }
            if (padB != null) {
                mergedData.put("padding_b", padB);
            }
            if (rawA != null) {
                mergedData.put("raw_event_a", rawA);
            }
            if (rawB != null) {
                mergedData.put("raw_event_b", rawB);
            }
            mergedData.put("payload_a_size_bytes", sizeA);
            mergedData.put("payload_b_size_bytes", sizeB);

            MergedEvent merged = new MergedEvent(
                    a.getCommonId(),
                    a.getPayloadId(),
                    b.getPayloadId(),
                    System.currentTimeMillis(),
                    mergedData,
                    padA,
                    padB,
                    dataA,
                    dataB,
                    sizeA,
                    sizeB,
                    sizeA + sizeB
            );
            out.collect(merged);
        } catch (Exception ex) {
            LOG.error("Failed to merge payloads for commonId: {}", a.getCommonId(), ex);
            DLQEvent dlqEvent = DLQEvent.builder()
                    .commonId(a.getCommonId())
                    .payloadId(a.getPayloadId() + " & " + b.getPayloadId())
                    .originalEvent(JsonUtil.toJson(pair))
                    .errorMessage("Error during merge execution: " + ex.getMessage())
                    .errorType("MERGE_EXCEPTION")
                    .processingStage("MERGE")
                    .exceptionClass(ex.getClass().getName())
                    .firstFailureTimestamp(startMs)
                    .lastFailureTimestamp(System.currentTimeMillis())
                    .build();
            ctx.output(dlqOutputTag, dlqEvent);
        }
    }
}
