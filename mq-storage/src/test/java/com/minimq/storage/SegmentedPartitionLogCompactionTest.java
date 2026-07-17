package com.minimq.storage;

import com.minimq.common.model.Record;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SegmentedPartitionLogCompactionTest {

    @Test
    void compactionKeepsLatestNonNullKeyAndPreservesNullKeys(@TempDir Path dir) throws Exception {
        try (SegmentedPartitionLog log = new SegmentedPartitionLog(dir, new SegmentedLogConfig(1, 1))) {
            log.append("a".getBytes(), "old".getBytes());
            log.append(null, "event".getBytes());
            log.append("a".getBytes(), "new".getBytes());
            log.append("b".getBytes(), "only".getBytes());

            log.applyCompaction(CompactionPolicy.enabled(3));

            List<Record> records = log.read(0, 4_096);
            assertEquals(List.of(1L, 2L, 3L), records.stream().map(Record::offset).toList());
            assertNull(records.get(0).key());
            assertArrayEquals("new".getBytes(), records.get(1).value());
            assertFalse(Files.exists(dir.resolve("00000000000000000000.log")));

            SegmentCatalog catalog = new SegmentCatalogStore(dir).load().orElseThrow();
            assertEquals(List.of(1L, 2L, 3L), catalog.segments().stream().map(SegmentDescriptor::baseOffset).toList());
        }
    }

    @Test
    void compactionNeverRewritesActiveSegment(@TempDir Path dir) throws Exception {
        try (SegmentedPartitionLog log = new SegmentedPartitionLog(dir, new SegmentedLogConfig(1_024 * 1024, 1))) {
            log.append("a".getBytes(), "old".getBytes());
            log.append("a".getBytes(), "new".getBytes());

            log.applyCompaction(CompactionPolicy.enabled(10));

            assertEquals(List.of(0L, 1L), log.read(0, 4_096).stream().map(Record::offset).toList());
        }
    }

    @Test
    void compactionHonorsMaxBytesPerRun(@TempDir Path dir) throws Exception {
        try (SegmentedPartitionLog log = new SegmentedPartitionLog(dir, new SegmentedLogConfig(1, 1))) {
            log.append("a".getBytes(), "old".getBytes());
            log.append("a".getBytes(), "new".getBytes());
            log.append("b".getBytes(), "active".getBytes());

            long firstSegmentSize = Files.size(SegmentFile.logPath(dir, 0));
            log.applyCompaction(CompactionPolicy.enabled(10, firstSegmentSize - 1));

            assertEquals(List.of(0L, 1L, 2L), log.read(0, 4_096).stream().map(Record::offset).toList());

            log.applyCompaction(CompactionPolicy.enabled(10, firstSegmentSize));

            assertEquals(List.of(1L, 2L), log.read(0, 4_096).stream().map(Record::offset).toList());
        }
    }

    @Test
    void startupRecoversInterruptedCompactionReplacement(@TempDir Path dir) throws Exception {
        SegmentedLogConfig config = new SegmentedLogConfig(1, 1);
        try (SegmentedPartitionLog log = new SegmentedPartitionLog(dir, config)) {
            log.append("a".getBytes(), "old".getBytes());
            log.append("a".getBytes(), "new".getBytes());
            log.append("b".getBytes(), "active".getBytes());
        }

        SegmentCatalog catalog = new SegmentCatalogStore(dir).load().orElseThrow();
        SegmentCatalog compactedCatalog = new SegmentCatalog(
                catalog.generation() + 1,
                catalog.activeBaseOffset(),
                1,
                catalog.segments().stream()
                        .filter(descriptor -> descriptor.baseOffset() != 0)
                        .toList());
        new SegmentCatalogStore(dir).save(compactedCatalog);
        SegmentFile.writeCleaned(
                dir,
                1,
                List.of(new Record(1, "a".getBytes(), "new".getBytes())),
                config);
        Files.move(
                SegmentFile.logPath(dir, 1),
                SegmentFile.obsoleteLogPath(dir, 1),
                StandardCopyOption.REPLACE_EXISTING);
        Files.move(
                SegmentFile.indexPath(dir, 1),
                SegmentFile.obsoleteIndexPath(dir, 1),
                StandardCopyOption.REPLACE_EXISTING);

        try (SegmentedPartitionLog recovered = assertDoesNotThrow(
                () -> new SegmentedPartitionLog(dir, config))) {
            assertEquals(List.of(1L, 2L), recovered.read(0, 4_096).stream().map(Record::offset).toList());
        }
        assertTrue(Files.exists(SegmentFile.logPath(dir, 1)));
        assertFalse(Files.exists(SegmentFile.cleanedLogPath(dir, 1)));
        assertFalse(Files.exists(SegmentFile.obsoleteLogPath(dir, 1)));
    }
}
