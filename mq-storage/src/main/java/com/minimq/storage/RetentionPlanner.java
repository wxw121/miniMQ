package com.minimq.storage;

import java.util.ArrayList;
import java.util.List;

/**
 * Selects whole sealed segments that retention may remove.
 */
final class RetentionPlanner {

    private RetentionPlanner() {
    }

    static List<Long> segmentsToDelete(
            List<SegmentMetadata> segments,
            RetentionPolicy policy,
            long nowEpochMs) {
        if (segments == null || segments.isEmpty() || policy == null || !policy.enabled()) {
            return List.of();
        }
        List<Long> selected = new ArrayList<>();
        if (policy.retentionMs() > 0) {
            for (SegmentMetadata segment : segments) {
                if (!segment.active()
                        && segment.createdAtEpochMs() + policy.retentionMs() <= nowEpochMs) {
                    selected.add(segment.baseOffset());
                }
            }
        }
        if (policy.retentionBytes() > 0) {
            long totalBytes = segments.stream().mapToLong(SegmentMetadata::sizeBytes).sum();
            for (SegmentMetadata segment : segments) {
                if (totalBytes <= policy.retentionBytes()) {
                    break;
                }
                if (segment.active() || selected.contains(segment.baseOffset())) {
                    continue;
                }
                selected.add(segment.baseOffset());
                totalBytes -= segment.sizeBytes();
            }
        }
        return List.copyOf(selected);
    }
}
