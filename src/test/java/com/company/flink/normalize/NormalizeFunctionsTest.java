package com.company.flink.normalize;

import com.company.flink.model.CommonEvent;
import com.company.flink.model.DLQEvent;
import com.company.flink.model.PayloadEvent;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class NormalizeFunctionsTest {

    private OutputTag<DLQEvent> dlqTag;
    private NormalizeAFunction normalizeA;
    private NormalizeBFunction normalizeB;

    @BeforeEach
    public void setup() {
        dlqTag = new OutputTag<>("dlq-validation", Types.POJO(DLQEvent.class));
        normalizeA = new NormalizeAFunction(dlqTag);
        normalizeB = new NormalizeBFunction(dlqTag);
    }

    private static class TestCollector<T> implements Collector<T> {
        final List<T> list = new ArrayList<>();
        @Override
        public void collect(T record) { list.add(record); }
        @Override
        public void close() {}
    }

    @Test
    public void testNormalizeAWithLargePadding() {
        String padding5k = "A".repeat(5000);
        String rawInput = "Timestamp: 10:15:34.123 UE: IMSI 404450000000001 Cell: LTE_5123 Event: Measurement Report RSRP: -95 dBm RSRQ: -12 dB Neighbor Cell: LTE_5124 Padding: " + padding5k;

        TestCollector<CommonEvent> collector = new TestCollector<>();
        normalizeA.processElement(rawInput, null, collector);

        assertEquals(1, collector.list.size());
        CommonEvent event = collector.list.get(0);

        assertEquals("404450000000001", event.getCommonId());
        assertEquals("A", event.getSourceType());
        assertEquals("10:15:34.123", event.getPayload().get("Timestamp"));
        assertEquals("LTE_5123", event.getPayload().get("Cell"));
        assertEquals("Measurement Report", event.getPayload().get("Event"));
        assertEquals("-95 dBm", event.getPayload().get("RSRP"));
        assertEquals("-12 dB", event.getPayload().get("RSRQ"));
        assertEquals("LTE_5124", event.getPayload().get("Neighbor Cell"));
        assertEquals(padding5k, event.getPayload().get("Padding"));

        PayloadEvent payloadEvent = PayloadEvent.fromCommonEvent(event);
        assertNotNull(payloadEvent.getData());
        assertEquals(padding5k, payloadEvent.getData().get("Padding"));
    }

    @Test
    public void testNormalizeBWithLargePadding() {
        String padding5k = "B".repeat(5000);
        String rawInput = "Timestamp: 10:15:33.900 IMSI: 404450000000001 Procedure: Attach Request MME_ID: MME_01 TAI: Chennai_125 Result: Success Padding: " + padding5k;

        TestCollector<CommonEvent> collector = new TestCollector<>();
        normalizeB.processElement(rawInput, null, collector);

        assertEquals(1, collector.list.size());
        CommonEvent event = collector.list.get(0);

        assertEquals("404450000000001", event.getCommonId());
        assertEquals("B", event.getSourceType());
        assertEquals("10:15:33.900", event.getPayload().get("Timestamp"));
        assertEquals("Attach Request", event.getPayload().get("Procedure"));
        assertEquals("MME_01", event.getPayload().get("MME_ID"));
        assertEquals("Chennai_125", event.getPayload().get("TAI"));
        assertEquals("Success", event.getPayload().get("Result"));
        assertEquals(padding5k, event.getPayload().get("Padding"));

        PayloadEvent payloadEvent = PayloadEvent.fromCommonEvent(event);
        assertNotNull(payloadEvent.getData());
        assertEquals(padding5k, payloadEvent.getData().get("Padding"));
    }

    @Test
    public void testNormalizeAWithoutPadding() {
        String rawInput = "Timestamp: 10:15:34.123 UE: IMSI 404450000000001 Cell: LTE_5123 Event: Measurement Report RSRP: -95 dBm RSRQ: -12 dB Neighbor Cell: LTE_5124";

        TestCollector<CommonEvent> collector = new TestCollector<>();
        normalizeA.processElement(rawInput, null, collector);

        assertEquals(1, collector.list.size());
        CommonEvent event = collector.list.get(0);

        assertEquals("404450000000001", event.getCommonId());
        assertEquals("LTE_5124", event.getPayload().get("Neighbor Cell"));
        assertNull(event.getPayload().get("Padding"));
    }

    @Test
    public void testNormalizeBWithoutPadding() {
        String rawInput = "Timestamp: 10:15:33.900 IMSI: 404450000000001 Procedure: Attach Request MME_ID: MME_01 TAI: Chennai_125 Result: Success";

        TestCollector<CommonEvent> collector = new TestCollector<>();
        normalizeB.processElement(rawInput, null, collector);

        assertEquals(1, collector.list.size());
        CommonEvent event = collector.list.get(0);

        assertEquals("404450000000001", event.getCommonId());
        assertEquals("Success", event.getPayload().get("Result"));
        assertNull(event.getPayload().get("Padding"));
    }

    @Test
    public void testNormalizeAJsonWithPadding() {
        String json = "{\"event_id\":\"A-101\",\"common_id\":\"404450000000001\",\"Timestamp\":\"10:15:34.000\",\"Cell\":\"LTE_5123\",\"Padding\":\"AAAAA\"}";

        TestCollector<CommonEvent> collector = new TestCollector<>();
        normalizeA.processElement(json, null, collector);

        assertEquals(1, collector.list.size());
        CommonEvent event = collector.list.get(0);
        assertEquals("404450000000001", event.getCommonId());
        assertEquals("AAAAA", event.getPayload().get("Padding"));
    }

    @Test
    public void testNormalizeBJsonWithPadding() {
        String json = "{\"event_id\":\"B-101\",\"common_id\":\"404450000000001\",\"Timestamp\":\"10:15:33.000\",\"Procedure\":\"Attach\",\"Padding\":\"BBBBB\"}";

        TestCollector<CommonEvent> collector = new TestCollector<>();
        normalizeB.processElement(json, null, collector);

        assertEquals(1, collector.list.size());
        CommonEvent event = collector.list.get(0);
        assertEquals("404450000000001", event.getCommonId());
        assertEquals("BBBBB", event.getPayload().get("Padding"));
    }
}
