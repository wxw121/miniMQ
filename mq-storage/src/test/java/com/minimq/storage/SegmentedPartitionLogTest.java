package com.minimq.storage;

import com.minimq.common.model.Record;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
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
}
