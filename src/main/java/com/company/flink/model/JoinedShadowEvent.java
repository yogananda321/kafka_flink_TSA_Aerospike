package com.company.flink.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;
import java.util.Objects;

/**
 * Output of stateful correlation join containing keys to retrieve both payloads from Aerospike.
 */
public class JoinedShadowEvent implements Serializable {

    private static final long serialVersionUID = 1L;

    @JsonProperty("common_id")
    private String commonId;

    @JsonProperty("payload_id_a")
    private String payloadIdA;

    @JsonProperty("payload_id_b")
    private String payloadIdB;

    @JsonProperty("join_timestamp")
    private long joinTimestamp;

    @JsonProperty("latency_ms")
    private long latencyMs;

    public JoinedShadowEvent() {}

    public JoinedShadowEvent(
            String commonId,
            String payloadIdA,
            String payloadIdB,
            long joinTimestamp,
            long latencyMs) {
        this.commonId = commonId;
        this.payloadIdA = payloadIdA;
        this.payloadIdB = payloadIdB;
        this.joinTimestamp = joinTimestamp;
        this.latencyMs = latencyMs;
    }

    public String getCommonId() { return commonId; }
    public void setCommonId(String commonId) { this.commonId = commonId; }

    public String getPayloadIdA() { return payloadIdA; }
    public void setPayloadIdA(String payloadIdA) { this.payloadIdA = payloadIdA; }

    public String getPayloadIdB() { return payloadIdB; }
    public void setPayloadIdB(String payloadIdB) { this.payloadIdB = payloadIdB; }

    public long getJoinTimestamp() { return joinTimestamp; }
    public void setJoinTimestamp(long joinTimestamp) { this.joinTimestamp = joinTimestamp; }

    public long getLatencyMs() { return latencyMs; }
    public void setLatencyMs(long latencyMs) { this.latencyMs = latencyMs; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        JoinedShadowEvent that = (JoinedShadowEvent) o;
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
        return "JoinedShadowEvent{" +
                "commonId='" + commonId + '\'' +
                ", payloadIdA='" + payloadIdA + '\'' +
                ", payloadIdB='" + payloadIdB + '\'' +
                ", joinTimestamp=" + joinTimestamp +
                ", latencyMs=" + latencyMs +
                '}';
    }
}
