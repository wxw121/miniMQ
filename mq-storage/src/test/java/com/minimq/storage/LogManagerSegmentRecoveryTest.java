package com.minimq.storage;

import com.minimq.common.model.Record;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LogManagerSegmentRecoveryTest {

    @Test
    void migratesLegacyLogAndDeletesItAfterCheckpoint(@TempDir Path dir) throws Exception {
        Path legacyFile = dir.resolve("orders-0.log");
        try (FilePartitionLog legacy = new FilePartitionLog(legacyFile)) {
            legacy.append("k0".getBytes(), "v0".getBytes());
            legacy.append("k1".getBytes(), "v1".getBytes());
        }

        LogManager manager = new LogManager(dir, segmentConfig());
        List<Record> records = manager.getOrCreate("orders", 0).read(0, 1_024);

        assertEquals(List.of(0L, 1L), records.stream().map(Record::offset).toList());
        assertTrue(Files.exists(dir.resolve("segments/orders-0/catalog.json")));
        assertTrue(Files.exists(legacyFile));

        manager.checkpoint();

        assertFalse(Files.exists(legacyFile));
    }

    @Test
    void discardsInterruptedMigrationStagingBeforeMigratingLegacyFile(@TempDir Path dir) throws Exception {
        Path legacyFile = dir.resolve("orders-0.log");
        try (FilePartitionLog legacy = new FilePartitionLog(legacyFile)) {
            legacy.append("k0".getBytes(), "v0".getBytes());
        }
        Path staging = dir.resolve("segments").resolve("orders-0.migrating");
        Files.createDirectories(staging);
        Files.writeString(staging.resolve("partial"), "incomplete");

        LogManager manager = new LogManager(dir, segmentConfig());
        List<Record> records = manager.getOrCreate("orders", 0).read(0, 1_024);

        assertEquals(List.of(0L), records.stream().map(Record::offset).toList());
        assertFalse(Files.exists(staging));
        assertTrue(Files.exists(dir.resolve("segments/orders-0/catalog.json")));
    }

    private LogManagerConfig segmentConfig() {
        return new LogManagerConfig(FlushPolicy.always(), 0, new SegmentedLogConfig(1, 1));
    }
}
