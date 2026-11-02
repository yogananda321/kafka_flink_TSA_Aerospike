package com.company.flink.config;

import java.io.Serializable;

/**
 * Production configuration for Aerospike client connections, retry policies, and TTL.
 */
public final class AerospikeConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String hosts;
    private final String namespace;
    private final String setName;
    private final String user;
    private final String password;
    private final int maxConnectionsPerNode;
    private final int connectionTimeoutMs;
    private final int socketTimeoutMs;
    private final int totalTimeoutMs;
    private final int maxRetries;
    private final int sleepBetweenRetriesMs;
    private final int payloadTtlSeconds;

    public AerospikeConfig(
            String hosts,
            String namespace,
            String setName,
            String user,
            String password,
            int maxConnectionsPerNode,
            int connectionTimeoutMs,
            int socketTimeoutMs,
            int totalTimeoutMs,
            int maxRetries,
            int sleepBetweenRetriesMs,
            int payloadTtlSeconds) {
        this.hosts = hosts;
        this.namespace = namespace;
        this.setName = setName;
        this.user = user;
        this.password = password;
        this.maxConnectionsPerNode = maxConnectionsPerNode;
        this.connectionTimeoutMs = connectionTimeoutMs;
        this.socketTimeoutMs = socketTimeoutMs;
        this.totalTimeoutMs = totalTimeoutMs;
        this.maxRetries = maxRetries;
        this.sleepBetweenRetriesMs = sleepBetweenRetriesMs;
        this.payloadTtlSeconds = payloadTtlSeconds;
    }

    public String getHosts() { return hosts; }
    public String getNamespace() { return namespace; }
    public String getSetName() { return setName; }
    public String getUser() { return user; }
    public String getPassword() { return password; }
    public int getMaxConnectionsPerNode() { return maxConnectionsPerNode; }
    public int getConnectionTimeoutMs() { return connectionTimeoutMs; }
    public int getSocketTimeoutMs() { return socketTimeoutMs; }
    public int getTotalTimeoutMs() { return totalTimeoutMs; }
    public int getMaxRetries() { return maxRetries; }
    public int getSleepBetweenRetriesMs() { return sleepBetweenRetriesMs; }
    public int getPayloadTtlSeconds() { return payloadTtlSeconds; }
}
