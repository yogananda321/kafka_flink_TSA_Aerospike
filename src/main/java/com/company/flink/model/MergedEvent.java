package com.company.flink.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;
import java.util.Map;
import java.util.Objects;

/**
 * Final merged event produced after fetching both payloads from Aerospike and merging them.
 */
public class MergedEvent implements Serializable {

    private static final long serialVersionUID = 1L;

    @JsonProperty("common_id")
    private String commonId;

    @JsonProperty("payload_id_a")
    private String payloadIdA;

    @JsonProperty("payload_id_b")
    private String payloadIdB;

    @JsonProperty("merged_timestamp")
    private long mergedTimestamp;

    @JsonProperty("padding_a")
    private String paddingA;

    @JsonProperty("padding_b")
    private String paddingB;

    @JsonProperty("payload_a")
    private Map<String, Object> payloadA;

    @JsonProperty("payload_b")
    private Map<String, Object> payloadB;

    @JsonProperty("payload_a_size_bytes")
    private int payloadASizeBytes;

    @JsonProperty("payload_b_size_bytes")
    private int payloadBSizeBytes;

    @JsonProperty("total_merged_size_bytes")
    private int totalMergedSizeBytes;

    @JsonProperty("data")
    private Map<String, Object> data;

    public MergedEvent() {}

    public MergedEvent(
            String commonId,
            String payloadIdA,
            String payloadIdB,
            long mergedTimestamp,
            Map<String, Object> data) {
        this(commonId, payloadIdA, payloadIdB, mergedTimestamp, data, null, null, null, null, 0, 0, 0);
    }

    public MergedEvent(
            String commonId,
            String payloadIdA,
            String payloadIdB,
            long mergedTimestamp,
            Map<String, Object> data,
            String paddingA,
            String paddingB) {
        this(commonId, payloadIdA, payloadIdB, mergedTimestamp, data, paddingA, paddingB, null, null, 0, 0, 0);
    }

    public MergedEvent(
            String commonId,
            String payloadIdA,
            String payloadIdB,
            long mergedTimestamp,
            Map<String, Object> data,
            String paddingA,
            String paddingB,
            Map<String, Object> payloadA,
            Map<String, Object> payloadB,
            int payloadASizeBytes,
            int payloadBSizeBytes,
            int totalMergedSizeBytes) {
        this.commonId = commonId;
        this.payloadIdA = payloadIdA;
        this.payloadIdB = payloadIdB;
        this.mergedTimestamp = mergedTimestamp;
        this.data = data;
        this.paddingA = paddingA;
        this.paddingB = paddingB;
        this.payloadA = payloadA;
        this.payloadB = payloadB;
        this.payloadASizeBytes = payloadASizeBytes;
        this.payloadBSizeBytes = payloadBSizeBytes;
        this.totalMergedSizeBytes = totalMergedSizeBytes;
    }

    public String getCommonId() { return commonId; }
    public void setCommonId(String commonId) { this.commonId = commonId; }

    public String getPayloadIdA() { return payloadIdA; }
    public void setPayloadIdA(String payloadIdA) { this.payloadIdA = payloadIdA; }

    public String getPayloadIdB() { return payloadIdB; }
    public void setPayloadIdB(String payloadIdB) { this.payloadIdB = payloadIdB; }

    public long getMergedTimestamp() { return mergedTimestamp; }
    public void setMergedTimestamp(long mergedTimestamp) { this.mergedTimestamp = mergedTimestamp; }

    public String getPaddingA() { return paddingA; }
    public void setPaddingA(String paddingA) { this.paddingA = paddingA; }

    public String getPaddingB() { return paddingB; }
    public void setPaddingB(String paddingB) { this.paddingB = paddingB; }

    public Map<String, Object> getPayloadA() { return payloadA; }
    public void setPayloadA(Map<String, Object> payloadA) { this.payloadA = payloadA; }

    public Map<String, Object> getPayloadB() { return payloadB; }
    public void setPayloadB(Map<String, Object> payloadB) { this.payloadB = payloadB; }

    public int getPayloadASizeBytes() { return payloadASizeBytes; }
    public void setPayloadASizeBytes(int payloadASizeBytes) { this.payloadASizeBytes = payloadASizeBytes; }

    public int getPayloadBSizeBytes() { return payloadBSizeBytes; }
    public void setPayloadBSizeBytes(int payloadBSizeBytes) { this.payloadBSizeBytes = payloadBSizeBytes; }

    public int getTotalMergedSizeBytes() { return totalMergedSizeBytes; }
    public void setTotalMergedSizeBytes(int totalMergedSizeBytes) { this.totalMergedSizeBytes = totalMergedSizeBytes; }

    public Map<String, Object> getData() { return data; }
    public void setData(Map<String, Object> data) { this.data = data; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MergedEvent that = (MergedEvent) o;
        return Objects.equals(commonId, that.commonId) &&
               Objects.equals(payloadIdA, that.payloadIdA) &&
               Objects.equals(payloadIdB, that.payloadIdB);
    }

    @Override
    public int hashCode() {
        return Objects.hash(commonId, payloadIdA, payloadIdB);
    }

    @Override
    public String toString() {
        return "MergedEvent{" +
                "commonId='" + commonId + '\'' +
                ", payloadIdA='" + payloadIdA + '\'' +
                ", payloadIdB='" + payloadIdB + '\'' +
                ", mergedTimestamp=" + mergedTimestamp +
                '}';
    }
}
