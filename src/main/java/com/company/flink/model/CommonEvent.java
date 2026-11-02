package com.company.flink.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;
import java.util.Map;
import java.util.Objects;

/**
 * Normalized internal event model unifying events from Topic A and Topic B.
 */
public class CommonEvent implements Serializable {

    private static final long serialVersionUID = 1L;

    @JsonProperty("event_id")
    private String eventId;

    @JsonProperty("source_type")
    private String sourceType; // "A" or "B"

    @JsonProperty("common_id")
    private String commonId;

    @JsonProperty("payload_id")
    private String payloadId;

    @JsonProperty("event_timestamp")
    private String eventTimestamp;

    @JsonProperty("ingestion_timestamp")
    private long ingestionTimestamp;

    @JsonProperty("schema_version")
    private String schemaVersion;

    @JsonProperty("payload")
    private Map<String, Object> payload;

    public CommonEvent() {}

    public CommonEvent(
            String eventId,
            String sourceType,
            String commonId,
            String payloadId,
            String eventTimestamp,
            long ingestionTimestamp,
            String schemaVersion,
            Map<String, Object> payload) {
        this.eventId = eventId;
        this.sourceType = sourceType;
        this.commonId = commonId;
        this.payloadId = payloadId;
        this.eventTimestamp = eventTimestamp;
        this.ingestionTimestamp = ingestionTimestamp;
        this.schemaVersion = schemaVersion;
        this.payload = payload;
    }

    public String getEventId() { return eventId; }
    public void setEventId(String eventId) { this.eventId = eventId; }

    public String getSourceType() { return sourceType; }
    public void setSourceType(String sourceType) { this.sourceType = sourceType; }

    public String getCommonId() { return commonId; }
    public void setCommonId(String commonId) { this.commonId = commonId; }

    public String getPayloadId() { return payloadId; }
    public void setPayloadId(String payloadId) { this.payloadId = payloadId; }

    public String getEventTimestamp() { return eventTimestamp; }
    public void setEventTimestamp(String eventTimestamp) { this.eventTimestamp = eventTimestamp; }

    public long getIngestionTimestamp() { return ingestionTimestamp; }
    public void setIngestionTimestamp(long ingestionTimestamp) { this.ingestionTimestamp = ingestionTimestamp; }

    public String getSchemaVersion() { return schemaVersion; }
    public void setSchemaVersion(String schemaVersion) { this.schemaVersion = schemaVersion; }

    public Map<String, Object> getPayload() { return payload; }
    public void setPayload(Map<String, Object> payload) { this.payload = payload; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CommonEvent that = (CommonEvent) o;
        return Objects.equals(payloadId, that.payloadId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(payloadId);
    }

    @Override
    public String toString() {
        return "CommonEvent{" +
                "eventId='" + eventId + '\'' +
                ", sourceType='" + sourceType + '\'' +
                ", commonId='" + commonId + '\'' +
                ", payloadId='" + payloadId + '\'' +
                ", eventTimestamp='" + eventTimestamp + '\'' +
                ", ingestionTimestamp=" + ingestionTimestamp +
                ", schemaVersion='" + schemaVersion + '\'' +
                '}';
    }
}
