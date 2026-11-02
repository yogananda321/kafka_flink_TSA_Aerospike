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
 * Inbound event model for Topic A (e.g., RAN / LTE Measurement Report events).
 * Supports standard JSON, telecom schema fields, and arbitrary dynamic properties.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class RawEventA implements Serializable {

    private static final long serialVersionUID = 1L;

    @JsonProperty("event_id")
    private String eventId;

    @JsonProperty("common_id")
    private String commonId;

    @JsonProperty("Timestamp")
    private String timestamp;

    @JsonProperty("UE")
    private String ue;

    @JsonProperty("IMSI")
    private String imsi;

    @JsonProperty("Cell")
    private String cell;

    @JsonProperty("Event")
    private String event;

    @JsonProperty("RSRP")
    private String rsrp;

    @JsonProperty("RSRQ")
    private String rsrq;

    @JsonProperty("Neighbor Cell")
    private String neighborCell;

    @JsonProperty("Padding")
    @JsonAlias({"padding", "PADDING"})
    private String padding;

    @JsonProperty("data")
    private Map<String, Object> data = new HashMap<>();

    private Map<String, Object> dynamicFields = new HashMap<>();

    public RawEventA() {}

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
        if (ue != null && !ue.trim().isEmpty()) {
            String cleaned = ue.replaceAll("(?i)IMSI", "").trim();
            if (!cleaned.isEmpty()) {
                return cleaned;
            }
        }
        return null;
    }

    public Map<String, Object> toDataMap() {
        Map<String, Object> map = new HashMap<>();
        if (data != null) map.putAll(data);
        if (dynamicFields != null) map.putAll(dynamicFields);
        if (timestamp != null) map.put("Timestamp", timestamp);
        if (ue != null) map.put("UE", ue);
        if (imsi != null) map.put("IMSI", imsi);
        if (cell != null) map.put("Cell", cell);
        if (event != null) map.put("Event", event);
        if (rsrp != null) map.put("RSRP", rsrp);
        if (rsrq != null) map.put("RSRQ", rsrq);
        if (neighborCell != null) map.put("Neighbor_Cell", neighborCell);
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

    public String getUe() { return ue; }
    public void setUe(String ue) { this.ue = ue; }

    public String getImsi() { return imsi; }
    public void setImsi(String imsi) { this.imsi = imsi; }

    public String getCell() { return cell; }
    public void setCell(String cell) { this.cell = cell; }

    public String getEvent() { return event; }
    public void setEvent(String event) { this.event = event; }

    public String getRsrp() { return rsrp; }
    public void setRsrp(String rsrp) { this.rsrp = rsrp; }

    public String getRsrq() { return rsrq; }
    public void setRsrq(String rsrq) { this.rsrq = rsrq; }

    public String getNeighborCell() { return neighborCell; }
    public void setNeighborCell(String neighborCell) { this.neighborCell = neighborCell; }

    public Map<String, Object> getData() { return data; }
    public void setData(Map<String, Object> data) { this.data = data; }

    public String getPadding() { return padding; }
    public void setPadding(String padding) { this.padding = padding; }
}
