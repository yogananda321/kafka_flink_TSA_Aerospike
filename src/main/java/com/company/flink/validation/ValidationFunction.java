package com.company.flink.validation;

import com.company.flink.model.CommonEvent;
import com.company.flink.model.DLQEvent;
import com.company.flink.util.JsonUtil;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * Validates CommonEvent fields prior to payload storage and stateful correlation.
 */
public class ValidationFunction extends ProcessFunction<CommonEvent, CommonEvent> {

    private static final long serialVersionUID = 1L;

    private final OutputTag<DLQEvent> dlqOutputTag;

    public ValidationFunction(OutputTag<DLQEvent> dlqOutputTag) {
        this.dlqOutputTag = dlqOutputTag;
    }

    @Override
    public void processElement(CommonEvent event, Context ctx, Collector<CommonEvent> out) {
        long now = System.currentTimeMillis();

        if (event == null) {
            emitDlq(ctx, null, null, null, null, "CommonEvent is null", "NULL_EVENT", now);
            return;
        }

        if (event.getCommonId() == null || event.getCommonId().trim().isEmpty()) {
            emitDlq(ctx, event.getEventId(), null, event.getPayloadId(), event.getSourceType(), "Missing common_id", "INVALID_COMMON_ID", now);
            return;
        }

        if (event.getPayloadId() == null || event.getPayloadId().trim().isEmpty()) {
            emitDlq(ctx, event.getEventId(), event.getCommonId(), null, event.getSourceType(), "Missing payload_id", "INVALID_PAYLOAD_ID", now);
            return;
        }

        if (event.getSourceType() == null || (!event.getSourceType().equals("A") && !event.getSourceType().equals("B"))) {
            emitDlq(ctx, event.getEventId(), event.getCommonId(), event.getPayloadId(), event.getSourceType(), "Invalid sourceType: " + event.getSourceType(), "INVALID_SOURCE_TYPE", now);
            return;
        }

        out.collect(event);
    }

    private void emitDlq(Context ctx, String eventId, String commonId, String payloadId, String sourceType, String errorMsg, String errorType, long now) {
        DLQEvent dlqEvent = DLQEvent.builder()
                .eventId(eventId)
                .commonId(commonId)
                .payloadId(payloadId)
                .sourceType(sourceType)
                .errorMessage(errorMsg)
                .errorType(errorType)
                .processingStage("VALIDATION")
                .exceptionClass("ValidationError")
                .firstFailureTimestamp(now)
                .lastFailureTimestamp(now)
                .build();
        ctx.output(dlqOutputTag, dlqEvent);
    }
}
