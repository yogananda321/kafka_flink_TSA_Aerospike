package com.company.flink.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;
import java.util.Objects;

/**
 * Lightweight event metadata maintained inside Flink's RocksDB state for correlation.
 */
public class ShadowEvent implements Serializable {

    private static final long serialVersionUID = 1L;

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

    public ShadowEvent() {}

    public ShadowEvent(
            String sourceType,
            String commonId,
            String payloadId,
            String eventTimestamp,
            long ingestionTimestamp) {
        this.sourceType = sourceType;
        this.commonId = commonId;
        this.payloadId = payloadId;
        this.eventTimestamp = eventTimestamp;
        this.ingestionTimestamp = ingestionTimestamp;
    }

    public static ShadowEvent fromCommonEvent(CommonEvent commonEvent) {
        return new ShadowEvent(
                commonEvent.getSourceType(),
                commonEvent.getCommonId(),
                commonEvent.getPayloadId(),
                commonEvent.getEventTimestamp(),
                commonEvent.getIngestionTimestamp()
        );
    }

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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ShadowEvent that = (ShadowEvent) o;
        return Objects.equals(sourceType, that.sourceType) &&
               Objects.equals(commonId, that.commonId) &&
               Objects.equals(payloadId, that.payloadId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sourceType, commonId, payloadId);
    }

    @Override
    public String toString() {
        return "ShadowEvent{" +
                "sourceType='" + sourceType + '\'' +
                ", commonId='" + commonId + '\'' +
                ", payloadId='" + payloadId + '\'' +
                ", eventTimestamp='" + eventTimestamp + '\'' +
                '}';
    }
}
