package com.minimq.storage;

import com.minimq.common.model.Record;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Plans key-based compaction for sealed segments.
 */
final class SegmentCompactor {

    private SegmentCompactor() {
    }

    static List<SegmentReplacement> plan(List<SegmentFile> orderedSegments, CompactionPolicy policy)
            throws IOException {
        return plan(orderedSegments, policy, Set.of());
    }

    static List<SegmentReplacement> plan(
            List<SegmentFile> orderedSegments,
            CompactionPolicy policy,
            Set<Long> protectedBases) throws IOException {
        if (policy == null || !policy.enabled() || orderedSegments.size() <= 1) {
            return List.of();
        }
        int sealedCount = orderedSegments.size() - 1;
        List<SegmentFile> selectedSegments = selectedSegments(
                orderedSegments, policy, protectedBases, sealedCount);
        if (selectedSegments.isEmpty()) {
            return List.of();
        }

        Set<SegmentFile> selectedSet = new HashSet<>(selectedSegments);
        Set<ByteArrayKey> newerKeys = new HashSet<>();
        List<SegmentReplacement> reversed = new ArrayList<>();
        for (int i = orderedSegments.size() - 1; i >= 0; i--) {
            SegmentFile segment = orderedSegments.get(i);
            List<Record> records = readAll(segment);
            if (!selectedSet.contains(segment)) {
                for (Record record : records) {
                    if (record.key() != null) {
                        newerKeys.add(new ByteArrayKey(record.key()));
                    }
                }
                continue;
            }

            List<Record> retained = new ArrayList<>();
            for (int recordIndex = records.size() - 1; recordIndex >= 0; recordIndex--) {
                Record record = records.get(recordIndex);
                if (record.key() == null) {
                    retained.add(0, record);
                    continue;
                }
                ByteArrayKey key = new ByteArrayKey(record.key());
                if (newerKeys.add(key)) {
                    retained.add(0, record);
                }
            }
            reversed.add(new SegmentReplacement(segment.baseOffset(), segment, retained));
        }

        List<SegmentReplacement> replacements = new ArrayList<>();
        for (int i = reversed.size() - 1; i >= 0; i--) {
            replacements.add(reversed.get(i));
        }
        return List.copyOf(replacements);
    }

    private static List<SegmentFile> selectedSegments(
            List<SegmentFile> orderedSegments,
            CompactionPolicy policy,
            Set<Long> protectedBases,
            int sealedCount) {
        int maxSegments = Math.min(policy.maxSegmentsPerRun(), sealedCount);
        List<SegmentFile> selected = new ArrayList<>();
        long selectedBytes = 0;
        for (int i = 0; i < sealedCount && selected.size() < maxSegments; i++) {
            SegmentFile segment = orderedSegments.get(i);
            if (protectedBases.contains(segment.baseOffset())) {
                continue;
            }
            long nextSize = segment.size();
            if (policy.maxBytesPerRun() > 0
                    && selectedBytes + nextSize > policy.maxBytesPerRun()) {
                break;
            }
            selected.add(segment);
            selectedBytes += nextSize;
        }
        return selected;
    }

    private static List<Record> readAll(SegmentFile segment) throws IOException {
        return segment.read(0, Integer.MAX_VALUE);
    }

    record SegmentReplacement(long baseOffset, SegmentFile original, List<Record> retainedRecords) {
        SegmentReplacement {
            retainedRecords = List.copyOf(retainedRecords);
        }
    }

    private static final class ByteArrayKey {
        private final byte[] bytes;

        private ByteArrayKey(byte[] bytes) {
            this.bytes = java.util.Arrays.copyOf(bytes, bytes.length);
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof ByteArrayKey key && java.util.Arrays.equals(bytes, key.bytes);
        }

        @Override
        public int hashCode() {
            return java.util.Arrays.hashCode(bytes);
        }
    }
}
