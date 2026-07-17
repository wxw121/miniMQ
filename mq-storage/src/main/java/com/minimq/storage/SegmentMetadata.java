package com.minimq.storage;

/**
 * Immutable facts used by maintenance selectors.
 */
record SegmentMetadata(long baseOffset, long createdAtEpochMs, long sizeBytes, boolean active) {

    SegmentMetadata {
        if (baseOffset < 0) {
            throw new IllegalArgumentException("segment metadata baseOffset must be non-negative");
        }
        if (createdAtEpochMs < 0) {
            throw new IllegalArgumentException("segment metadata createdAtEpochMs must be non-negative");
        }
        if (sizeBytes < 0) {
            throw new IllegalArgumentException("segment metadata sizeBytes must be non-negative");
        }
    }
}
