package com.minimq.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SegmentedPartitionLogFetchPlanTest {
    @Test
    void plansIndexedSlicesAcrossSegmentsAndMatchesHeapRead(@TempDir Path dir) throws Exception {
        try (SegmentedPartitionLog log = new SegmentedPartitionLog(dir, new SegmentedLogConfig(1, 2))) {
            for (int i = 0; i < 5; i++) {
                log.append(("k" + i).getBytes(), ("value-" + i).getBytes());
            }
            List<com.minimq.common.model.Record> heap = log.read(2, 1_024);

            try (FetchPlan plan = log.planFetch(2, 1_024)) {
                assertEquals(log.highWatermark(), plan.highWatermark());
                assertEquals(heap.size(), plan.recordCount());
                assertEquals(heap.stream().mapToInt(record ->
                        PartitionLogRecordCodec.encodedSize(record.offset(), record.key(), record.value())).sum(),
                        plan.recordBytes());
                assertTrue(plan.slices().size() >= 2);
                assertEquals(heap.stream().map(com.minimq.common.model.Record::offset).toList(),
                        decodeOffsets(plan));
            }
        }
    }

    @Test
    void planUsesCurrentHighWatermarkAndIsIdempotentlyCloseable(@TempDir Path dir) throws Exception {
        try (SegmentedPartitionLog log = new SegmentedPartitionLog(dir, new SegmentedLogConfig(1_024, 1))) {
            log.append("k0".getBytes(), "v0".getBytes());
            FetchPlan plan = log.planFetch(0, 1_024);
            log.append("k1".getBytes(), "v1".getBytes());

            assertEquals(1, plan.highWatermark());
            assertEquals(List.of(0L), decodeOffsets(plan));
            plan.close();
            plan.close();
        }
    }

    @Test
    void openPlanDefersRetentionUntilClosed(@TempDir Path dir) throws Exception {
        try (SegmentedPartitionLog log = new SegmentedPartitionLog(dir, new SegmentedLogConfig(1, 1))) {
            for (int i = 0; i < 3; i++) {
                log.append(("k" + i).getBytes(), ("v" + i).getBytes());
            }
            FetchPlan plan = log.planFetch(0, 1_024);

            LogMaintenanceReport skipped = log.applyRetentionWithReport(
                    "orders-0", RetentionPolicy.of(1, 0), System.currentTimeMillis() + 10_000);
            assertEquals(MaintenanceStatus.SKIPPED_READERS, skipped.status());

            plan.close();
            LogMaintenanceReport applied = log.applyRetentionWithReport(
                    "orders-0", RetentionPolicy.of(1, 0), System.currentTimeMillis() + 20_000);
            assertEquals(MaintenanceStatus.SUCCESS, applied.status());
        }
    }

    @Test
    void plansEmptyAndFirstRecordOverBudget(@TempDir Path dir) throws Exception {
        try (SegmentedPartitionLog log = new SegmentedPartitionLog(dir, new SegmentedLogConfig(1_024, 1))) {
            log.append("key".getBytes(), "value-larger-than-budget".getBytes());
            try (FetchPlan empty = log.planFetch(0, 0);
                 FetchPlan first = log.planFetch(0, 1)) {
                assertEquals(0, empty.recordCount());
                assertTrue(empty.slices().isEmpty());
                assertEquals(1, first.recordCount());
                assertFalse(first.slices().isEmpty());
            }
        }
    }

    @Test
    void exactBudgetDoesNotAdmitTheFirstRecordFromTheNextSegment(@TempDir Path dir) throws Exception {
        byte[] key = "k".getBytes();
        byte[] value = "v".getBytes();
        int oneRecord = PartitionLogRecordCodec.encodedSize(0, key, value);
        try (SegmentedPartitionLog log = new SegmentedPartitionLog(dir, new SegmentedLogConfig(1, 1))) {
            log.append(key, value);
            log.append(key, value);
            try (FetchPlan plan = log.planFetch(0, oneRecord)) {
                assertEquals(List.of(0L), decodeOffsets(plan));
            }
        }
    }

    @Test
    void planMatchesHeapAcrossCompactionHolesAndRetainedStart(@TempDir Path dir) throws Exception {
        try (SegmentedPartitionLog log = new SegmentedPartitionLog(dir, new SegmentedLogConfig(1, 1))) {
            log.append("a".getBytes(), "v0".getBytes());
            log.append("b".getBytes(), "v1".getBytes());
            log.append("a".getBytes(), "v2".getBytes());
            log.append("c".getBytes(), "v3".getBytes());
            log.applyCompaction(CompactionPolicy.enabled(10, 0));

            List<Long> compactedHeap = log.read(0, 1_024).stream()
                    .map(com.minimq.common.model.Record::offset).toList();
            try (FetchPlan compactedPlan = log.planFetch(0, 1_024)) {
                assertEquals(compactedHeap, decodeOffsets(compactedPlan));
                assertFalse(compactedHeap.contains(0L));
            }

            log.applyRetention(RetentionPolicy.of(1, 0), System.currentTimeMillis() + 20_000);
            List<Long> retainedHeap = log.read(0, 1_024).stream()
                    .map(com.minimq.common.model.Record::offset).toList();
            try (FetchPlan retainedPlan = log.planFetch(0, 1_024)) {
                assertEquals(retainedHeap, decodeOffsets(retainedPlan));
            }
        }
    }

    private static List<Long> decodeOffsets(FetchPlan plan) throws Exception {
        List<Long> offsets = new ArrayList<>();
        for (FileRegionSlice slice : plan.slices()) {
            try (FileChannel channel = FileChannel.open(slice.path(), StandardOpenOption.READ)) {
                long position = slice.position();
                long end = position + slice.length();
                while (position < end) {
                    PartitionLogRecordCodec.ScannedRecord scanned =
                            PartitionLogRecordCodec.scan(channel, position, end);
                    offsets.add(scanned.offset());
                    position = scanned.nextPosition();
                }
            }
        }
        return offsets;
    }
}
