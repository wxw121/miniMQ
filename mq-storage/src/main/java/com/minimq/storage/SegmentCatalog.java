package com.minimq.storage;

import java.util.List;

/**
 * Immutable ordered view of the segment files visible for one partition.
 */
public record SegmentCatalog(
        long generation,
        long activeBaseOffset,
        long logStartOffset,
        List<SegmentDescriptor> segments) {

    public SegmentCatalog {
        if (generation < 0) {
            throw new IllegalArgumentException("segment catalog generation must be non-negative");
        }
        if (activeBaseOffset < 0) {
            throw new IllegalArgumentException("segment catalog activeBaseOffset must be non-negative");
        }
        if (logStartOffset < 0) {
            throw new IllegalArgumentException("segment catalog logStartOffset must be non-negative");
        }
        if (segments == null || segments.isEmpty()) {
            throw new IllegalArgumentException("segment catalog segments are required");
        }
        segments = List.copyOf(segments);
        long previousBaseOffset = -1;
        for (SegmentDescriptor segment : segments) {
            if (segment == null) {
                throw new IllegalArgumentException("segment catalog segment is required");
            }
            if (segment.baseOffset() <= previousBaseOffset) {
                throw new IllegalArgumentException("segment catalog base offsets must increase");
            }
            previousBaseOffset = segment.baseOffset();
        }
        if (logStartOffset > segments.get(0).baseOffset()) {
            throw new IllegalArgumentException("segment catalog logStartOffset exceeds first segment base");
        }
        if (activeBaseOffset != segments.get(segments.size() - 1).baseOffset()) {
            throw new IllegalArgumentException("segment catalog activeBaseOffset must be the last segment base");
        }
    }
}
