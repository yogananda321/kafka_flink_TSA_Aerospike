package com.company.flink.sink;

import com.company.flink.config.KafkaConfig;
import com.company.flink.model.ShadowEvent;
import com.company.flink.util.JsonUtil;
import java.nio.charset.StandardCharsets;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Kafka Sink for publishing lightweight Shadow Events to topic 'shadow_events'.
 * Keyed by common_id (IMSI) for partition alignment.
 */
public final class KafkaShadowEventSink {

    private KafkaShadowEventSink() {}

    public static KafkaSink<ShadowEvent> create(KafkaConfig config) {
        return KafkaSink.<ShadowEvent>builder()
                .setBootstrapServers(config.getBootstrapServers())
                .setKafkaProducerConfig(config.toProducerProperties())
                .setRecordSerializer(new KafkaRecordSerializationSchema<ShadowEvent>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(
                            ShadowEvent element,
                            KafkaSinkContext context,
                            Long timestamp) {
                        byte[] key = element.getCommonId() != null
                                ? element.getCommonId().getBytes(StandardCharsets.UTF_8)
                                : null;
                        byte[] value = JsonUtil.toJson(element).getBytes(StandardCharsets.UTF_8);
                        return new ProducerRecord<>(config.getTopicSinkShadow(), key, value);
                    }
                })
                .build();
    }
}
