package com.company.flink.aerospike;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.company.flink.config.AerospikeConfig;
import com.company.flink.model.CommonEvent;
import com.company.flink.model.DLQEvent;
import com.company.flink.model.PayloadEvent;
import com.company.flink.model.ShadowEvent;
import com.company.flink.util.JsonUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Idempotently persists the full PayloadEvent to Aerospike.
 * Upon successful Aerospike ACK, emits the lightweight ShadowEvent to downstream correlation.
 * If write fails after configured retries, routes failure to DLQ.
 */
public class PayloadWriter extends ProcessFunction<CommonEvent, ShadowEvent> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(PayloadWriter.class);

    private final AerospikeConfig config;
    private final OutputTag<DLQEvent> dlqOutputTag;

    private transient AerospikeClientProvider clientProvider;
    private transient WritePolicy writePolicy;
    private transient Counter writeSuccessCounter;
    private transient Counter writeFailureCounter;
    private transient Counter writeRetryCounter;
    private transient Histogram writeLatencyHistogram;

    public PayloadWriter(AerospikeConfig config, OutputTag<DLQEvent> dlqOutputTag) {
        this.config = config;
        this.dlqOutputTag = dlqOutputTag;
    }

    @Override
    public void open(Configuration parameters) {
        this.clientProvider = new AerospikeClientProvider(config);
        this.writePolicy = new WritePolicy();
        this.writePolicy.recordExistsAction = RecordExistsAction.UPDATE;
        this.writePolicy.totalTimeout = config.getTotalTimeoutMs();
        this.writePolicy.socketTimeout = config.getSocketTimeoutMs();
        this.writePolicy.expiration = config.getPayloadTtlSeconds();
        this.writePolicy.maxRetries = config.getMaxRetries();
        this.writePolicy.sleepBetweenRetries = Math.min(20, config.getSleepBetweenRetriesMs());

        this.writeSuccessCounter = getRuntimeContext().getMetricGroup().counter("aerospike_write_success_total");
        this.writeFailureCounter = getRuntimeContext().getMetricGroup().counter("aerospike_write_failure_total");
        this.writeRetryCounter = getRuntimeContext().getMetricGroup().counter("aerospike_write_retry_total");
        this.writeLatencyHistogram = getRuntimeContext().getMetricGroup().histogram("aerospike_write_latency_ms", new DescriptiveStatisticsHistogram(1000));
    }

    @Override
    public void processElement(CommonEvent commonEvent, Context ctx, Collector<ShadowEvent> out) {
        long startMs = System.currentTimeMillis();
        String payloadId = commonEvent.getPayloadId();
        String padding = null;
        if (commonEvent.getPayload() != null) {
            Object padObj = commonEvent.getPayload().get("Padding");
            if (padObj == null) {
                padObj = commonEvent.getPayload().get("padding");
            }
            if (padObj != null) {
                padding = String.valueOf(padObj);
            }
        }

        PayloadEvent payloadEvent = PayloadEvent.fromCommonEvent(commonEvent);
        if (padding != null && !padding.isEmpty()) {
            if (payloadEvent.getData() == null) {
                payloadEvent.setData(new java.util.HashMap<>());
            }
            payloadEvent.getData().put("padding", padding);
            payloadEvent.getData().put("Padding", padding);
        }
        String payloadJson = JsonUtil.toJson(payloadEvent);

        Key recordKey = new Key(config.getNamespace(), config.getSetName(), payloadId);
        Bin payloadBin = new Bin("payload", payloadJson);
        Bin timestampBin = new Bin("updated_at", startMs);
        Bin commonIdBin = new Bin("common_id", commonEvent.getCommonId());
        Bin sourceBin = new Bin("source_type", commonEvent.getSourceType());

        String rawEvent = (commonEvent.getPayload() != null && commonEvent.getPayload().get("raw_event") != null)
                ? String.valueOf(commonEvent.getPayload().get("raw_event"))
                : null;

        java.util.List<Bin> binsList = new java.util.ArrayList<>(6);
        binsList.add(payloadBin);
        binsList.add(timestampBin);
        binsList.add(commonIdBin);
        binsList.add(sourceBin);
        if (padding != null && !padding.isEmpty()) {
            binsList.add(new Bin("padding", padding));
        }
        if (rawEvent != null && !rawEvent.isEmpty()) {
            binsList.add(new Bin("raw_event", rawEvent));
        }
        Bin[] bins = binsList.toArray(new Bin[0]);

        int attempts = 0;
        int maxRetries = config.getMaxRetries();
        boolean writeSucceeded = false;
        Exception lastException = null;

        while (attempts <= maxRetries && !writeSucceeded) {
            attempts++;
            try {
                AerospikeClient client = clientProvider.getClient();
                // Persist payload to Aerospike with dedicated padding bin
                client.put(writePolicy, recordKey, bins);
                writeSucceeded = true;
                writeSuccessCounter.inc();
                long elapsed = System.currentTimeMillis() - startMs;
                writeLatencyHistogram.update(elapsed);

                // Consistency boundary: Emit shadow event only AFTER confirmed Aerospike write
                ShadowEvent shadowEvent = ShadowEvent.fromCommonEvent(commonEvent);
                out.collect(shadowEvent);

            } catch (Exception ex) {
                lastException = ex;
                writeRetryCounter.inc();
                LOG.warn("Aerospike write failed for payloadId: {} on attempt {}/{}", payloadId, attempts, maxRetries + 1, ex);
                if (attempts <= maxRetries) {
                    try {
                        Thread.sleep((long) Math.min(15, config.getSleepBetweenRetriesMs()) * attempts);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        }

        if (!writeSucceeded) {
            writeFailureCounter.inc();
            LOG.error("Aerospike write exhausted retries for payloadId: {}", payloadId, lastException);
            DLQEvent dlqEvent = DLQEvent.builder()
                    .eventId(commonEvent.getEventId())
                    .commonId(commonEvent.getCommonId())
                    .payloadId(payloadId)
                    .sourceType(commonEvent.getSourceType())
                    .originalEvent(payloadJson)
                    .errorMessage(lastException != null ? lastException.getMessage() : "Aerospike write failed")
                    .errorType("AEROSPIKE_WRITE_ERROR")
                    .processingStage("AEROSPIKE_WRITE")
                    .exceptionClass(lastException != null ? lastException.getClass().getName() : "AerospikeException")
                    .retryCount(attempts - 1)
                    .firstFailureTimestamp(startMs)
                    .lastFailureTimestamp(System.currentTimeMillis())
                    .build();
            ctx.output(dlqOutputTag, dlqEvent);
        }
    }

    @Override
    public void close() {
        AerospikeClientProvider.close();
    }
}
