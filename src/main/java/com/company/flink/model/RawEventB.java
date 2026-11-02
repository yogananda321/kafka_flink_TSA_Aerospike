package com.company.flink.model;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Inbound event model for Topic B (e.g., MME / EPC Core Network events).
 * Supports standard JSON, telecom schema fields, and arbitrary dynamic properties.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class RawEventB implements Serializable {

    private static final long serialVersionUID = 1L;

    @JsonProperty("event_id")
    private String eventId;

    @JsonProperty("common_id")
    private String commonId;

    @JsonProperty("Timestamp")
    private String timestamp;

    @JsonProperty("created_at")
    private String createdAt;

    @JsonProperty("IMSI")
    private String imsi;

    @JsonProperty("Procedure")
    private String procedure;

    @JsonProperty("MME_ID")
    private String mmeId;

    @JsonProperty("TAI")
    private String tai;

    @JsonProperty("Result")
    private String result;

    @JsonProperty("Padding")
    @JsonAlias({"padding", "PADDING"})
    private String padding;

    @JsonProperty("details")
    private Map<String, Object> details = new HashMap<>();

    private Map<String, Object> dynamicFields = new HashMap<>();

    public RawEventB() {}

    @JsonAnySetter
    public void setDynamicField(String key, Object value) {
        dynamicFields.put(key, value);
    }

    @JsonAnyGetter
    public Map<String, Object> getDynamicFields() {
        return dynamicFields;
    }

    public String getResolvedImsi() {
        if (imsi != null && !imsi.trim().isEmpty()) {
            return imsi.trim();
        }
        if (commonId != null && !commonId.trim().isEmpty()) {
            return commonId.trim();
        }
        return null;
    }

    public Map<String, Object> toDataMap() {
        Map<String, Object> map = new HashMap<>();
        if (details != null) map.putAll(details);
        if (dynamicFields != null) map.putAll(dynamicFields);
        if (timestamp != null) map.put("Timestamp", timestamp);
        if (createdAt != null) map.put("created_at", createdAt);
        if (imsi != null) map.put("IMSI", imsi);
        if (procedure != null) map.put("Procedure", procedure);
        if (mmeId != null) map.put("MME_ID", mmeId);
        if (tai != null) map.put("TAI", tai);
        if (result != null) map.put("Result", result);
        if (padding != null && !padding.isEmpty()) map.put("Padding", padding);
        return map;
    }

    // Getters and Setters
    public String getEventId() { return eventId; }
    public void setEventId(String eventId) { this.eventId = eventId; }

    public String getCommonId() { return commonId; }
    public void setCommonId(String commonId) { this.commonId = commonId; }

    public String getTimestamp() { return timestamp; }
    public void setTimestamp(String timestamp) { this.timestamp = timestamp; }

    public String getCreatedAt() { return createdAt; }
    public void setCreatedAt(String createdAt) { this.createdAt = createdAt; }

    public String getImsi() { return imsi; }
    public void setImsi(String imsi) { this.imsi = imsi; }

    public String getProcedure() { return procedure; }
    public void setProcedure(String procedure) { this.procedure = procedure; }

    public String getMmeId() { return mmeId; }
    public void setMmeId(String mmeId) { this.mmeId = mmeId; }

    public String getTai() { return tai; }
    public void setTai(String tai) { this.tai = tai; }

    public String getResult() { return result; }
    public void setResult(String result) { this.result = result; }

    public Map<String, Object> getDetails() { return details; }
    public void setDetails(Map<String, Object> details) { this.details = details; }

    public String getPadding() { return padding; }
    public void setPadding(String padding) { this.padding = padding; }
}
