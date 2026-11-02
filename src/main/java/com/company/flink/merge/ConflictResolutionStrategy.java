package com.company.flink.merge;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Strategy interface and default implementations for resolving field conflicts during payload merge.
 */
public interface ConflictResolutionStrategy extends Serializable {

    Map<String, Object> resolve(
            Map<String, Object> dataA,
            Map<String, Object> dataB,
            String payloadIdA,
            String payloadIdB
    );

    enum StrategyType {
        DEEP_MERGE,
        PREFER_A,
        PREFER_B,
        SEPARATE_NAMESPACES
    }

    static ConflictResolutionStrategy create(StrategyType type) {
        switch (type) {
            case PREFER_A:
                return (a, b, idA, idB) -> {
                    Map<String, Object> merged = new HashMap<>(b != null ? b : Map.of());
                    if (a != null) merged.putAll(a);
                    return merged;
                };
            case PREFER_B:
                return (a, b, idA, idB) -> {
                    Map<String, Object> merged = new HashMap<>(a != null ? a : Map.of());
                    if (b != null) merged.putAll(b);
                    return merged;
                };
            case SEPARATE_NAMESPACES:
                return (a, b, idA, idB) -> {
                    Map<String, Object> merged = new HashMap<>();
                    merged.put("source_a_payload", a);
                    merged.put("source_b_payload", b);
                    return merged;
                };
            case DEEP_MERGE:
            default:
                return (a, b, idA, idB) -> {
                    Map<String, Object> result = new HashMap<>();
                    if (a != null) result.putAll(a);
                    if (b != null) {
                        for (Map.Entry<String, Object> entry : b.entrySet()) {
                            if (result.containsKey(entry.getKey())) {
                                result.put("b_" + entry.getKey(), entry.getValue());
                            } else {
                                result.put(entry.getKey(), entry.getValue());
                            }
                        }
                    }
                    return result;
                };
        }
    }
}
