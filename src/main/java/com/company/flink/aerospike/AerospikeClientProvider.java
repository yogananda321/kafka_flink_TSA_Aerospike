package com.company.flink.aerospike;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Host;
import com.aerospike.client.policy.ClientPolicy;
import com.company.flink.config.AerospikeConfig;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Thread-safe Aerospike Client Provider managing client instance lifecycle per JVM/TaskManager.
 */
public class AerospikeClientProvider implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(AerospikeClientProvider.class);

    private static volatile AerospikeClient instance;
    private final AerospikeConfig config;

    public AerospikeClientProvider(AerospikeConfig config) {
        this.config = config;
    }

    public AerospikeClient getClient() {
        if (instance == null || !instance.isConnected()) {
            synchronized (AerospikeClientProvider.class) {
                if (instance == null || !instance.isConnected()) {
                    instance = createClient(config);
                }
            }
        }
        return instance;
    }

    private static AerospikeClient createClient(AerospikeConfig cfg) {
        LOG.info("Initializing Aerospike client for hosts: {}, namespace: {}", cfg.getHosts(), cfg.getNamespace());
        ClientPolicy clientPolicy = new ClientPolicy();
        clientPolicy.maxConnsPerNode = cfg.getMaxConnectionsPerNode();
        clientPolicy.timeout = cfg.getConnectionTimeoutMs();
        
        if (cfg.getUser() != null && !cfg.getUser().trim().isEmpty()) {
            clientPolicy.user = cfg.getUser();
            clientPolicy.password = cfg.getPassword();
        }

        List<Host> hostList = new ArrayList<>();
        String[] hostParts = cfg.getHosts().split(",");
        for (String hp : hostParts) {
            String[] parts = hp.trim().split(":");
            String h = parts[0].trim();
            int p = (parts.length > 1) ? Integer.parseInt(parts[1].trim()) : 3000;
            hostList.add(new Host(h, p));
        }

        return new AerospikeClient(clientPolicy, hostList.toArray(new Host[0]));
    }

    public static void close() {
        synchronized (AerospikeClientProvider.class) {
            if (instance != null) {
                LOG.info("Closing Aerospike client");
                instance.close();
                instance = null;
            }
        }
    }
}
