package com.minimq.storage;

/**
 * Immutable identity and creation time for one partition log segment.
 */
public record SegmentDescriptor(long baseOffset, long createdAtEpochMs) {

    public SegmentDescriptor {
        if (baseOffset < 0) {
            throw new IllegalArgumentException("segment baseOffset must be non-negative");
        }
        if (createdAtEpochMs < 0) {
            throw new IllegalArgumentException("segment createdAtEpochMs must be non-negative");
        }
    }
}
