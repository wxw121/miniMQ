package com.minimq.storage;

/**
 * Immutable settings that control segment rolling and sparse index density.
 */
public record SegmentedLogConfig(long segmentMaxBytes, int sparseIndexIntervalRecords) {
    public static final long DEFAULT_SEGMENT_MAX_BYTES = 64L * 1024 * 1024;
    public static final int DEFAULT_SPARSE_INDEX_INTERVAL_RECORDS = 64;

    public SegmentedLogConfig {
        if (segmentMaxBytes <= 0) {
            throw new IllegalArgumentException("segmentMaxBytes must be positive");
        }
        if (sparseIndexIntervalRecords <= 0) {
            throw new IllegalArgumentException("sparseIndexIntervalRecords must be positive");
        }
    }

    public static SegmentedLogConfig defaults() {
        return new SegmentedLogConfig(DEFAULT_SEGMENT_MAX_BYTES, DEFAULT_SPARSE_INDEX_INTERVAL_RECORDS);
    }
}
