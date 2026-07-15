package com.minimq.storage;

/**
 * Sealed-segment compaction settings.
 */
public record CompactionPolicy(boolean enabled, int maxSegmentsPerRun) {

    public CompactionPolicy {
        if (enabled && maxSegmentsPerRun <= 0) {
            throw new IllegalArgumentException("maxSegmentsPerRun must be positive when compaction is enabled");
        }
        if (!enabled && maxSegmentsPerRun < 0) {
            throw new IllegalArgumentException("maxSegmentsPerRun must be non-negative");
        }
    }

    public static CompactionPolicy disabled() {
        return new CompactionPolicy(false, 0);
    }

    public static CompactionPolicy enabled(int maxSegmentsPerRun) {
        return new CompactionPolicy(true, maxSegmentsPerRun);
    }
}
