package com.minimq.storage;

import com.minimq.common.model.Record;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SegmentedPartitionLogTest {

    @Test
    void rollsAndReadsAcrossSegments(@TempDir Path dir) throws Exception {
        try (SegmentedPartitionLog log = new SegmentedPartitionLog(dir, new SegmentedLogConfig(1, 2))) {
            assertEquals(0, log.append("k0".getBytes(), "v0".getBytes()));
            assertEquals(1, log.append("k1".getBytes(), "v1".getBytes()));
            assertEquals(2, log.append("k2".getBytes(), "v2".getBytes()));

            List<Record> records = log.read(1, 1_024);
            assertEquals(List.of(1L, 2L), records.stream().map(Record::offset).toList());
        }

        assertTrue(Files.exists(dir.resolve("00000000000000000000.log")));
        assertTrue(Files.exists(dir.resolve("00000000000000000001.log")));
        assertTrue(Files.exists(dir.resolve("catalog.json")));
    }

    @Test
    void rebuildsMissingIndexAndTruncatesOnlyActiveTail(@TempDir Path dir) throws Exception {
        Path activeLog = dir.resolve("00000000000000000001.log");
        Path activeIndex = dir.resolve("00000000000000000001.index");
        try (SegmentedPartitionLog log = new SegmentedPartitionLog(dir, new SegmentedLogConfig(1, 1))) {
            log.append("k0".getBytes(), "v0".getBytes());
            log.append("k1".getBytes(), "v1".getBytes());
        }
        long completeSize = Files.size(activeLog);
        Files.delete(activeIndex);
        Files.write(activeLog, new byte[] {1, 2, 3}, StandardOpenOption.APPEND);

        try (SegmentedPartitionLog recovered = new SegmentedPartitionLog(dir, new SegmentedLogConfig(1, 1))) {
            assertEquals(2, recovered.highWatermark());
            assertEquals(2, recovered.read(0, 1_024).size());
        }

        assertEquals(completeSize, Files.size(activeLog));
        assertTrue(Files.exists(activeIndex));
    }

    @Test
    void rejectsIncompleteTailInSealedSegment(@TempDir Path dir) throws Exception {
        Path sealedLog = dir.resolve("00000000000000000000.log");
        try (SegmentedPartitionLog log = new SegmentedPartitionLog(dir, new SegmentedLogConfig(1, 1))) {
            log.append("k0".getBytes(), "v0".getBytes());
            log.append("k1".getBytes(), "v1".getBytes());
        }
        Files.write(sealedLog, new byte[] {1, 2, 3}, StandardOpenOption.APPEND);

        IOException failure = assertThrows(IOException.class,
                () -> new SegmentedPartitionLog(dir, new SegmentedLogConfig(1, 1)));

        assertTrue(failure.getMessage().contains("sealed"));
    }

    @Test
    void rebuildsCatalogAndAppliesIdempotentReplayRules(@TempDir Path dir) throws Exception {
        try (SegmentedPartitionLog log = new SegmentedPartitionLog(dir, new SegmentedLogConfig(1, 1))) {
            log.appendAt(0, "k0".getBytes(), "v0".getBytes());
            log.appendAt(0, "k0".getBytes(), "v0".getBytes());

            assertEquals(1, log.highWatermark());
            assertEquals(List.of(), log.read(0, 0));
            assertThrows(IllegalStateException.class,
                    () -> log.appendAt(0, "k0".getBytes(), "different".getBytes()));
            assertThrows(IllegalStateException.class,
                    () -> log.appendAt(2, "k2".getBytes(), "v2".getBytes()));
        }

        Files.delete(dir.resolve("catalog.json"));

        try (SegmentedPartitionLog recovered = new SegmentedPartitionLog(dir, new SegmentedLogConfig(1, 1))) {
            assertEquals(List.of(0L), recovered.read(0, 1_024).stream().map(Record::offset).toList());
        }
    }

    @Test
    void rejectsOperationsAfterClose(@TempDir Path dir) throws Exception {
        SegmentedPartitionLog log = new SegmentedPartitionLog(dir, new SegmentedLogConfig(1, 1));

        log.close();
        log.close();

        assertThrows(IllegalStateException.class, () -> log.append("k".getBytes(), "v".getBytes()));
        assertThrows(IllegalStateException.class, () -> log.read(0, 1));
        assertThrows(IllegalStateException.class, log::flush);
    }

    @Test
    void stopsReadAtBudgetAndRejectsOverlappingRecoveredSegments(@TempDir Path dir) throws Exception {
        byte[] firstKey = "k0".getBytes();
        byte[] firstValue = "v0".getBytes();
        byte[] secondKey = "k1".getBytes();
        byte[] secondValue = "value-that-exceeds-first-budget".getBytes();

        try (SegmentedPartitionLog log = new SegmentedPartitionLog(dir.resolve("budget"), new SegmentedLogConfig(1_024, 1))) {
            log.append(firstKey, firstValue);
            log.append(secondKey, secondValue);

            int firstRecordBudget = PartitionLogRecordCodec.encodedSize(0, firstKey, firstValue);
            assertEquals(List.of(0L), log.read(0, firstRecordBudget).stream().map(Record::offset).toList());
        }

        Path overlapping = dir.resolve("overlap");
        Files.createDirectories(overlapping);
        writeSegmentLog(overlapping, 0, 0);
        writeSegmentLog(overlapping, 1, 0);
        new SegmentCatalogStore(overlapping).save(new SegmentCatalog(
                1,
                1,
                0,
                List.of(new SegmentDescriptor(0, 100), new SegmentDescriptor(1, 200))));

        IOException failure = assertThrows(IOException.class,
                () -> new SegmentedPartitionLog(overlapping, new SegmentedLogConfig(1_024, 1)));
        assertTrue(failure.getMessage().contains("overlapping offsets"));
    }

    private void writeSegmentLog(Path dir, long baseOffset, long recordOffset) throws IOException {
        try (FileChannel channel = FileChannel.open(
                SegmentFile.logPath(dir, baseOffset),
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE)) {
            PartitionLogRecordCodec.write(channel, recordOffset, "k".getBytes(), "v".getBytes());
        }
    }
}
