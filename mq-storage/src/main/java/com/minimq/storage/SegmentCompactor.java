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
        if (policy == null || !policy.enabled() || orderedSegments.size() <= 1) {
            return List.of();
        }
        int sealedCount = orderedSegments.size() - 1;
        int selectedCount = Math.min(policy.maxSegmentsPerRun(), sealedCount);
        if (selectedCount <= 0) {
            return List.of();
        }

        Set<ByteArrayKey> newerKeys = new HashSet<>();
        for (int i = selectedCount; i < orderedSegments.size(); i++) {
            for (Record record : readAll(orderedSegments.get(i))) {
                if (record.key() != null) {
                    newerKeys.add(new ByteArrayKey(record.key()));
                }
            }
        }

        List<SegmentReplacement> reversed = new ArrayList<>();
        for (int i = selectedCount - 1; i >= 0; i--) {
            SegmentFile segment = orderedSegments.get(i);
            List<Record> records = readAll(segment);
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
