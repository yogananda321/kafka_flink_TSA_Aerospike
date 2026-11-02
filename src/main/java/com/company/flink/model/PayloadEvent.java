package com.company.flink.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;
import java.util.Map;
import java.util.Objects;

/**
 * Complete event payload offloaded to Aerospike.
 */
public class PayloadEvent implements Serializable {

    private static final long serialVersionUID = 1L;

    @JsonProperty("payload_id")
    private String payloadId;

    @JsonProperty("event_id")
    private String eventId;

    @JsonProperty("source_type")
    private String sourceType;

    @JsonProperty("common_id")
    private String commonId;

    @JsonProperty("event_timestamp")
    private String eventTimestamp;

    @JsonProperty("ingestion_timestamp")
    private long ingestionTimestamp;

    @JsonProperty("schema_version")
    private String schemaVersion;

    @JsonProperty("data")
    private Map<String, Object> data;

    public PayloadEvent() {}

    public PayloadEvent(
            String payloadId,
            String eventId,
            String sourceType,
            String commonId,
            String eventTimestamp,
            long ingestionTimestamp,
            String schemaVersion,
            Map<String, Object> data) {
        this.payloadId = payloadId;
        this.eventId = eventId;
        this.sourceType = sourceType;
        this.commonId = commonId;
        this.eventTimestamp = eventTimestamp;
        this.ingestionTimestamp = ingestionTimestamp;
        this.schemaVersion = schemaVersion;
        this.data = data;
    }

    public static PayloadEvent fromCommonEvent(CommonEvent commonEvent) {
        return new PayloadEvent(
                commonEvent.getPayloadId(),
                commonEvent.getEventId(),
                commonEvent.getSourceType(),
                commonEvent.getCommonId(),
                commonEvent.getEventTimestamp(),
                commonEvent.getIngestionTimestamp(),
                commonEvent.getSchemaVersion(),
                commonEvent.getPayload()
        );
    }

    public String getPayloadId() { return payloadId; }
    public void setPayloadId(String payloadId) { this.payloadId = payloadId; }

    public String getEventId() { return eventId; }
    public void setEventId(String eventId) { this.eventId = eventId; }

    public String getSourceType() { return sourceType; }
    public void setSourceType(String sourceType) { this.sourceType = sourceType; }

    public String getCommonId() { return commonId; }
    public void setCommonId(String commonId) { this.commonId = commonId; }

    public String getEventTimestamp() { return eventTimestamp; }
    public void setEventTimestamp(String eventTimestamp) { this.eventTimestamp = eventTimestamp; }

    public long getIngestionTimestamp() { return ingestionTimestamp; }
    public void setIngestionTimestamp(long ingestionTimestamp) { this.ingestionTimestamp = ingestionTimestamp; }

    public String getSchemaVersion() { return schemaVersion; }
    public void setSchemaVersion(String schemaVersion) { this.schemaVersion = schemaVersion; }

    public Map<String, Object> getData() { return data; }
    public void setData(Map<String, Object> data) { this.data = data; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PayloadEvent that = (PayloadEvent) o;
        return Objects.equals(payloadId, that.payloadId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(payloadId);
    }

    @Override
    public String toString() {
        return "PayloadEvent{" +
                "payloadId='" + payloadId + '\'' +
                ", eventId='" + eventId + '\'' +
                ", sourceType='" + sourceType + '\'' +
                ", commonId='" + commonId + '\'' +
                '}';
    }
}
