package com.company.flink.config;

import java.io.Serializable;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

/**
 * Immutable Kafka configuration model supporting source, sink, and DLQ parameters.
 */
public final class KafkaConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String bootstrapServers;
    private final String groupId;
    private final String autoOffsetReset;
    private final String isolationLevel;

    private final String topicSourceA;
    private final String topicSourceB;
    private final String topicSinkMerged;
    private final String topicSinkShadow;
    private final String topicSinkPayload;

    private final String topicDlqValidation;
    private final String topicDlqAerospike;
    private final String topicDlqCorrelation;
    private final String topicDlqPayloadFetch;
    private final String topicDlqProcessing;

    private final String producerAcks;
    private final int producerRetries;
    private final int maxInFlightRequestsPerConnection;
    private final String compressionType;
    private final int batchSize;
    private final int lingerMs;

    public KafkaConfig(
            String bootstrapServers,
            String groupId,
            String autoOffsetReset,
            String isolationLevel,
            String topicSourceA,
            String topicSourceB,
            String topicSinkMerged,
            String topicSinkShadow,
            String topicSinkPayload,
            String topicDlqValidation,
            String topicDlqAerospike,
            String topicDlqCorrelation,
            String topicDlqPayloadFetch,
            String topicDlqProcessing,
            String producerAcks,
            int producerRetries,
            int maxInFlightRequestsPerConnection,
            String compressionType,
            int batchSize,
            int lingerMs) {
        this.bootstrapServers = bootstrapServers;
        this.groupId = groupId;
        this.autoOffsetReset = autoOffsetReset;
        this.isolationLevel = isolationLevel;
        this.topicSourceA = topicSourceA;
        this.topicSourceB = topicSourceB;
        this.topicSinkMerged = topicSinkMerged;
        this.topicSinkShadow = topicSinkShadow;
        this.topicSinkPayload = topicSinkPayload;
        this.topicDlqValidation = topicDlqValidation;
        this.topicDlqAerospike = topicDlqAerospike;
        this.topicDlqCorrelation = topicDlqCorrelation;
        this.topicDlqPayloadFetch = topicDlqPayloadFetch;
        this.topicDlqProcessing = topicDlqProcessing;
        this.producerAcks = producerAcks;
        this.producerRetries = producerRetries;
        this.maxInFlightRequestsPerConnection = maxInFlightRequestsPerConnection;
        this.compressionType = compressionType;
        this.batchSize = batchSize;
        this.lingerMs = lingerMs;
    }

    public Properties toConsumerProperties() {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
        props.setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, isolationLevel);
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        return props;
    }

    public Properties toProducerProperties() {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.setProperty(ProducerConfig.ACKS_CONFIG, producerAcks);
        props.setProperty(ProducerConfig.RETRIES_CONFIG, String.valueOf(producerRetries));
        props.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, String.valueOf(maxInFlightRequestsPerConnection));
        props.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, compressionType);
        props.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, String.valueOf(batchSize));
        props.setProperty(ProducerConfig.LINGER_MS_CONFIG, String.valueOf(lingerMs));
        return props;
    }

    public String getBootstrapServers() { return bootstrapServers; }
    public String getGroupId() { return groupId; }
    public String getAutoOffsetReset() { return autoOffsetReset; }
    public String getIsolationLevel() { return isolationLevel; }
    public String getTopicSourceA() { return topicSourceA; }
    public String getTopicSourceB() { return topicSourceB; }
    public String getTopicSinkMerged() { return topicSinkMerged; }
    public String getTopicSinkShadow() { return topicSinkShadow; }
    public String getTopicSinkPayload() { return topicSinkPayload; }
    public String getTopicDlqValidation() { return topicDlqValidation; }
    public String getTopicDlqAerospike() { return topicDlqAerospike; }
    public String getTopicDlqCorrelation() { return topicDlqCorrelation; }
    public String getTopicDlqPayloadFetch() { return topicDlqPayloadFetch; }
    public String getTopicDlqProcessing() { return topicDlqProcessing; }
    public String getProducerAcks() { return producerAcks; }
    public int getProducerRetries() { return producerRetries; }
    public int getMaxInFlightRequestsPerConnection() { return maxInFlightRequestsPerConnection; }
    public String getCompressionType() { return compressionType; }
    public int getBatchSize() { return batchSize; }
    public int getLingerMs() { return lingerMs; }
}
