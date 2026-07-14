package com.minimq.storage;

import java.util.HashMap;
import java.util.Map;

/**
 * Persisted recovery point for WAL replay.
 */
public class RecoveryCheckpoint {
    public long walPosition;
    public Map<String, Long> partitionHighWatermarks = new HashMap<>();
    public long createdAtEpochMs;

    public RecoveryCheckpoint() {
    }

    public RecoveryCheckpoint(long walPosition, Map<String, Long> partitionHighWatermarks, long createdAtEpochMs) {
        if (walPosition < 0) {
            throw new IllegalArgumentException("walPosition must be non-negative");
        }
        this.walPosition = walPosition;
        this.partitionHighWatermarks = new HashMap<>(partitionHighWatermarks);
        this.createdAtEpochMs = createdAtEpochMs;
    }

    public static String partitionKey(String topic, int partitionId) {
        return topic + "-" + partitionId;
    }
}
