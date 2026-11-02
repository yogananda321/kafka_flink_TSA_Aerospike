package com.company.flink.util;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Utility for generating deterministic hashes and payload IDs for replay safety and idempotency.
 */
public final class HashUtil {

    private HashUtil() {}

    /**
     * Generates a deterministic payload ID based on source type and event ID.
     * The same input will always generate the exact same payload ID on recovery/replay.
     */
    public static String generateDeterministicPayloadId(String sourceType, String eventId) {
        if (sourceType == null || eventId == null) {
            throw new IllegalArgumentException("sourceType and eventId cannot be null for deterministic hash");
        }
        String input = sourceType.trim().toUpperCase() + "::" + eventId.trim();
        return sha256Hex(input);
    }

    /**
     * Computes the SHA-256 hex string of any input string.
     */
    public static String sha256Hex(String input) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hashBytes = digest.digest(input.getBytes(StandardCharsets.UTF_8));
            StringBuilder hexString = new StringBuilder(2 * hashBytes.length);
            for (byte b : hashBytes) {
                String hex = Integer.toHexString(0xff & b);
                if (hex.length() == 1) {
                    hexString.append('0');
                }
                hexString.append(hex);
            }
            return hexString.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("SHA-256 algorithm not available in JVM", e);
        }
    }
}
