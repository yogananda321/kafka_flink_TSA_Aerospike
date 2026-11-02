package com.company.flink.source;

import com.company.flink.config.KafkaConfig;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

/**
 * Factory for creating the KafkaSource for Topic A.
 */
public final class TopicASource {

    private TopicASource() {}

    public static KafkaSource<String> create(KafkaConfig kafkaConfig) {
        OffsetsInitializer startingOffsets = OffsetsInitializer.earliest();
        if ("latest".equalsIgnoreCase(kafkaConfig.getAutoOffsetReset())) {
            startingOffsets = OffsetsInitializer.latest();
        } else if ("committed".equalsIgnoreCase(kafkaConfig.getAutoOffsetReset())) {
            startingOffsets = OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST);
        }

        return KafkaSource.<String>builder()
                .setBootstrapServers(kafkaConfig.getBootstrapServers())
                .setTopics(kafkaConfig.getTopicSourceA())
                .setGroupId(kafkaConfig.getGroupId() + "-a")
                .setStartingOffsets(startingOffsets)
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperties(kafkaConfig.toConsumerProperties())
                .build();
    }
}
