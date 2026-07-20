package com.minimq.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SegmentedPartitionLogTruncationTest {
    @TempDir Path tempDir;

    @Test
    void truncatesActiveSegmentAndAllowsReplacementTail() throws Exception {
        try (SegmentedPartitionLog log = new SegmentedPartitionLog(tempDir,
                new SegmentedLogConfig(1024, 1))) {
            append(log, 6);
            log.truncateTo(3);
            assertEquals(3, log.highWatermark());
            assertEquals(List.of(0L, 1L, 2L), offsets(log));
            log.appendAt(3, null, new byte[]{99});
            assertEquals(4, log.highWatermark());
        }
    }

    @Test
    void truncatesAtSegmentBoundaryAndDeletesSuccessorsAcrossRestart() throws Exception {
        SegmentedLogConfig config = new SegmentedLogConfig(70, 1);
        try (SegmentedPartitionLog log = new SegmentedPartitionLog(tempDir, config)) {
            append(log, 10);
            log.truncateTo(4);
            log.truncateTo(4);
            assertEquals(List.of(0L, 1L, 2L, 3L), offsets(log));
        }
        try (SegmentedPartitionLog reopened = new SegmentedPartitionLog(tempDir, config)) {
            assertEquals(4, reopened.highWatermark());
            assertEquals(List.of(0L, 1L, 2L, 3L), offsets(reopened));
        }
    }

    @Test
    void rejectsOffsetsOutsideRetainedLogRange() throws Exception {
        try (SegmentedPartitionLog log = new SegmentedPartitionLog(tempDir, SegmentedLogConfig.defaults())) {
            append(log, 2);
            assertThrows(IllegalArgumentException.class, () -> log.truncateTo(-1));
            assertThrows(IllegalArgumentException.class, () -> log.truncateTo(3));
        }
    }

    private static void append(PartitionLog log, int count) {
        for (int i = 0; i < count; i++) {
            log.append(null, new byte[24]);
        }
    }

    private static List<Long> offsets(PartitionLog log) {
        return log.read(0, Integer.MAX_VALUE).stream().map(record -> record.offset()).toList();
    }
}
