package com.minimq.storage;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
        Set<Long> selectedBases = new HashSet<>();
        long selectedBytes = 0;
        if (policy.retentionMs() > 0) {
            for (SegmentMetadata segment : segments) {
                if (!segment.active()
                        && segment.createdAtEpochMs() + policy.retentionMs() <= nowEpochMs) {
                    selected.add(segment.baseOffset());
                    selectedBases.add(segment.baseOffset());
                    selectedBytes += segment.sizeBytes();
                }
            }
        }
        if (policy.retentionBytes() > 0) {
            long totalBytes = segments.stream().mapToLong(SegmentMetadata::sizeBytes).sum() - selectedBytes;
            for (SegmentMetadata segment : segments) {
                if (totalBytes <= policy.retentionBytes()) {
                    break;
                }
                if (segment.active() || selectedBases.contains(segment.baseOffset())) {
                    continue;
                }
                selected.add(segment.baseOffset());
                selectedBases.add(segment.baseOffset());
                totalBytes -= segment.sizeBytes();
            }
        }
        return List.copyOf(selected);
    }
}
