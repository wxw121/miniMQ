package com.minimq.storage;

/**
 * One segment-level maintenance action and its affected offset range.
 */
public record MaintenanceSegmentReport(
        String partition,
        MaintenanceAction action,
        long baseOffset,
        long firstOffset,
        long lastOffset,
        long sizeBytes) {

    public MaintenanceSegmentReport {
        if (partition == null || partition.isBlank()) {
            throw new IllegalArgumentException("partition is required");
        }
        if (action == null) {
            throw new IllegalArgumentException("action is required");
        }
        if (baseOffset < 0) {
            throw new IllegalArgumentException("baseOffset must be non-negative");
        }
        if (sizeBytes < 0) {
            throw new IllegalArgumentException("sizeBytes must be non-negative");
        }
    }
}
