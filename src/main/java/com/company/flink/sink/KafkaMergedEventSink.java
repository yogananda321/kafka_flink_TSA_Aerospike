package com.company.flink.sink;

import com.company.flink.config.KafkaConfig;
import com.company.flink.model.MergedEvent;
import com.company.flink.util.JsonUtil;
import java.nio.charset.StandardCharsets;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * High-reliability Kafka Sink for publishing final joined and merged events.
 * Uses common_id as record key for deterministic partition routing and downstream idempotency.
 */
public final class KafkaMergedEventSink {

    private KafkaMergedEventSink() {}

    public static KafkaSink<MergedEvent> create(KafkaConfig config) {
        return KafkaSink.<MergedEvent>builder()
                .setBootstrapServers(config.getBootstrapServers())
                .setKafkaProducerConfig(config.toProducerProperties())
                .setRecordSerializer(new KafkaRecordSerializationSchema<MergedEvent>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(
                            MergedEvent element,
                            KafkaSinkContext context,
                            Long timestamp) {
                        byte[] key = element.getCommonId() != null
                                ? element.getCommonId().getBytes(StandardCharsets.UTF_8)
                                : null;
                        byte[] value = JsonUtil.toJson(element).getBytes(StandardCharsets.UTF_8);
                        return new ProducerRecord<>(config.getTopicSinkMerged(), key, value);
                    }
                })
                .build();
    }
}
