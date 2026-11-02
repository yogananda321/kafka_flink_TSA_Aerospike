package com.company.flink.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;
import java.util.Objects;

/**
 * Universal Dead Letter Queue (DLQ) event model capturing failure diagnostics,
 * stage metadata, stack traces, and original payload.
 */
public class DLQEvent implements Serializable {

    private static final long serialVersionUID = 1L;

    @JsonProperty("event_id")
    private String eventId;

    @JsonProperty("common_id")
    private String commonId;

    @JsonProperty("payload_id")
    private String payloadId;

    @JsonProperty("source_type")
    private String sourceType;

    @JsonProperty("original_event")
    private String originalEvent;

    @JsonProperty("error_message")
    private String errorMessage;

    @JsonProperty("error_type")
    private String errorType; // VALIDATION_ERROR, AEROSPIKE_WRITE_ERROR, CORRELATION_TIMEOUT, PAYLOAD_FETCH_ERROR, PROCESSING_ERROR

    @JsonProperty("processing_stage")
    private String processingStage; // NORMALIZATION, VALIDATION, AEROSPIKE_WRITE, STATEFUL_JOIN, PAYLOAD_FETCH, MERGE

    @JsonProperty("exception_class")
    private String exceptionClass;

    @JsonProperty("retry_count")
    private int retryCount;

    @JsonProperty("first_failure_timestamp")
    private long firstFailureTimestamp;

    @JsonProperty("last_failure_timestamp")
    private long lastFailureTimestamp;

    public DLQEvent() {}

    public DLQEvent(
            String eventId,
            String commonId,
            String payloadId,
            String sourceType,
            String originalEvent,
            String errorMessage,
            String errorType,
            String processingStage,
            String exceptionClass,
            int retryCount,
            long firstFailureTimestamp,
            long lastFailureTimestamp) {
        this.eventId = eventId;
        this.commonId = commonId;
        this.payloadId = payloadId;
        this.sourceType = sourceType;
        this.originalEvent = originalEvent;
        this.errorMessage = errorMessage;
        this.errorType = errorType;
        this.processingStage = processingStage;
        this.exceptionClass = exceptionClass;
        this.retryCount = retryCount;
        this.firstFailureTimestamp = firstFailureTimestamp;
        this.lastFailureTimestamp = lastFailureTimestamp;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String eventId;
        private String commonId;
        private String payloadId;
        private String sourceType;
        private String originalEvent;
        private String errorMessage;
        private String errorType;
        private String processingStage;
        private String exceptionClass;
        private int retryCount = 0;
        private long firstFailureTimestamp = System.currentTimeMillis();
        private long lastFailureTimestamp = System.currentTimeMillis();

        public Builder eventId(String eventId) { this.eventId = eventId; return this; }
        public Builder commonId(String commonId) { this.commonId = commonId; return this; }
        public Builder payloadId(String payloadId) { this.payloadId = payloadId; return this; }
        public Builder sourceType(String sourceType) { this.sourceType = sourceType; return this; }
        public Builder originalEvent(String originalEvent) { this.originalEvent = originalEvent; return this; }
        public Builder errorMessage(String errorMessage) { this.errorMessage = errorMessage; return this; }
        public Builder errorType(String errorType) { this.errorType = errorType; return this; }
        public Builder processingStage(String processingStage) { this.processingStage = processingStage; return this; }
        public Builder exceptionClass(String exceptionClass) { this.exceptionClass = exceptionClass; return this; }
        public Builder retryCount(int retryCount) { this.retryCount = retryCount; return this; }
        public Builder firstFailureTimestamp(long firstFailureTimestamp) { this.firstFailureTimestamp = firstFailureTimestamp; return this; }
        public Builder lastFailureTimestamp(long lastFailureTimestamp) { this.lastFailureTimestamp = lastFailureTimestamp; return this; }

        public DLQEvent build() {
            return new DLQEvent(
                    eventId, commonId, payloadId, sourceType, originalEvent,
                    errorMessage, errorType, processingStage, exceptionClass,
                    retryCount, firstFailureTimestamp, lastFailureTimestamp
            );
        }
    }

    // Getters and Setters
    public String getEventId() { return eventId; }
    public void setEventId(String eventId) { this.eventId = eventId; }

    public String getCommonId() { return commonId; }
    public void setCommonId(String commonId) { this.commonId = commonId; }

    public String getPayloadId() { return payloadId; }
    public void setPayloadId(String payloadId) { this.payloadId = payloadId; }

    public String getSourceType() { return sourceType; }
    public void setSourceType(String sourceType) { this.sourceType = sourceType; }

    public String getOriginalEvent() { return originalEvent; }
    public void setOriginalEvent(String originalEvent) { this.originalEvent = originalEvent; }

    public String getErrorMessage() { return errorMessage; }
    public void setErrorMessage(String errorMessage) { this.errorMessage = errorMessage; }

    public String getErrorType() { return errorType; }
    public void setErrorType(String errorType) { this.errorType = errorType; }

    public String getProcessingStage() { return processingStage; }
    public void setProcessingStage(String processingStage) { this.processingStage = processingStage; }

    public String getExceptionClass() { return exceptionClass; }
    public void setExceptionClass(String exceptionClass) { this.exceptionClass = exceptionClass; }

    public int getRetryCount() { return retryCount; }
    public void setRetryCount(int retryCount) { this.retryCount = retryCount; }

    public long getFirstFailureTimestamp() { return firstFailureTimestamp; }
    public void setFirstFailureTimestamp(long firstFailureTimestamp) { this.firstFailureTimestamp = firstFailureTimestamp; }

    public long getLastFailureTimestamp() { return lastFailureTimestamp; }
    public void setLastFailureTimestamp(long lastFailureTimestamp) { this.lastFailureTimestamp = lastFailureTimestamp; }

    @Override
    public String toString() {
        return "DLQEvent{" +
                "commonId='" + commonId + '\'' +
                ", payloadId='" + payloadId + '\'' +
                ", errorType='" + errorType + '\'' +
                ", processingStage='" + processingStage + '\'' +
                ", errorMessage='" + errorMessage + '\'' +
                '}';
    }
}
