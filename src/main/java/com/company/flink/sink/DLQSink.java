package com.company.flink.sink;

import com.company.flink.config.KafkaConfig;
import com.company.flink.model.DLQEvent;
import com.company.flink.util.JsonUtil;
import java.nio.charset.StandardCharsets;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Universal Dead Letter Queue (DLQ) Kafka Sink creator.
 */
public final class DLQSink {

    private DLQSink() {}

    public static KafkaSink<DLQEvent> create(KafkaConfig config, String targetTopic) {
        return KafkaSink.<DLQEvent>builder()
                .setBootstrapServers(config.getBootstrapServers())
                .setKafkaProducerConfig(config.toProducerProperties())
                .setRecordSerializer(new KafkaRecordSerializationSchema<DLQEvent>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(
                            DLQEvent element,
                            KafkaSinkContext context,
                            Long timestamp) {
                        byte[] key = element.getCommonId() != null
                                ? element.getCommonId().getBytes(StandardCharsets.UTF_8)
                                : null;
                        byte[] value = JsonUtil.toJson(element).getBytes(StandardCharsets.UTF_8);
                        return new ProducerRecord<>(targetTopic, key, value);
                    }
                })
                .build();
    }
}
