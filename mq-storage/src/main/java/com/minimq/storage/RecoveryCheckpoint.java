package com.minimq.storage;

import java.util.HashMap;
import java.util.Map;

/**
 * Persisted recovery point for WAL replay.
 */
public class RecoveryCheckpoint {
    public long walPosition;
    public Map<String, Long> partitionHighWatermarks = new HashMap<>();
    public Map<String, Map<Long, Long>> scheduledDeliveries = new HashMap<>();
    public Map<String, Long> visibleDeliveryWatermarks = new HashMap<>();
    public long createdAtEpochMs;

    public RecoveryCheckpoint() {
    }

    public RecoveryCheckpoint(long walPosition, Map<String, Long> partitionHighWatermarks, long createdAtEpochMs) {
        this(walPosition, partitionHighWatermarks, Map.of(), Map.of(), createdAtEpochMs);
    }

    public RecoveryCheckpoint(
            long walPosition,
            Map<String, Long> partitionHighWatermarks,
            Map<String, Map<Long, Long>> scheduledDeliveries,
            long createdAtEpochMs) {
        this(walPosition, partitionHighWatermarks, scheduledDeliveries, Map.of(), createdAtEpochMs);
    }

    public RecoveryCheckpoint(
            long walPosition,
            Map<String, Long> partitionHighWatermarks,
            Map<String, Map<Long, Long>> scheduledDeliveries,
            Map<String, Long> visibleDeliveryWatermarks,
            long createdAtEpochMs) {
        if (walPosition < 0) {
            throw new IllegalArgumentException("walPosition must be non-negative");
        }
        this.walPosition = walPosition;
        this.partitionHighWatermarks = new HashMap<>(partitionHighWatermarks);
        scheduledDeliveries.forEach((partition, deliveries) ->
                this.scheduledDeliveries.put(partition, new HashMap<>(deliveries)));
        this.visibleDeliveryWatermarks = new HashMap<>(visibleDeliveryWatermarks);
        this.createdAtEpochMs = createdAtEpochMs;
    }

    public static String partitionKey(String topic, int partitionId) {
        return topic + "-" + partitionId;
    }
}
