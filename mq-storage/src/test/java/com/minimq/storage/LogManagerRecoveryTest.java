package com.minimq.storage;

import com.minimq.common.model.Record;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LogManagerRecoveryTest {

    @Test
    void replaysWalWhenNoCheckpointExists(@TempDir Path dir) throws Exception {
        try (WriteAheadLog wal = new WriteAheadLog(dir.resolve("wal").resolve("minimq.wal"))) {
            wal.append(WalRecord.dataAppend("orders", 0, 0, "k0".getBytes(), "v0".getBytes()));
            wal.append(WalRecord.dataAppend("orders", 0, 1, "k1".getBytes(), "v1".getBytes()));
        }

        LogManager manager = new LogManager(dir);

        List<Record> records = manager.getOrCreate("orders", 0).read(0, 1024 * 1024);
        assertEquals(2, records.size());
        assertEquals("v0", new String(records.get(0).value()));
        assertEquals("v1", new String(records.get(1).value()));
    }

    @Test
    void portableLegacyTopicNameSurvivesAppendAndRestart(@TempDir Path dir) {
        try (LogManager manager = new LogManager(dir)) {
            manager.append("sales east", 0, "k".getBytes(), "legacy".getBytes());
        }

        try (LogManager recovered = new LogManager(dir)) {
            List<Record> records = recovered.getOrCreate("sales east", 0)
                    .read(0, 1_024);
            assertEquals(1, records.size());
            assertEquals("legacy", new String(records.get(0).value()));
        }
    }

    @Test
    void rejectsTopicNamesThatAliasOnCaseInsensitiveFileSystems(@TempDir Path dir) {
        try (LogManager manager = new LogManager(dir)) {
            manager.append("orders", 0, "k".getBytes(), "lower".getBytes());

            IllegalArgumentException failure = assertThrows(
                    IllegalArgumentException.class,
                    () -> manager.append("Orders", 0, "k".getBytes(), "upper".getBytes()));

            assertTrue(failure.getMessage().contains("storage identity"), failure.getMessage());
        }
    }

    @Test
    void replaysFromWalWhenPartitionLogFallsBehindCheckpoint(@TempDir Path dir) throws Exception {
        LogManagerConfig config = new LogManagerConfig(
                FlushPolicy.always(), 0, new SegmentedLogConfig(1, 1));
        LogManager manager = new LogManager(dir, config);
        manager.append("orders", 0, "k0".getBytes(), "v0".getBytes());
        manager.append("orders", 0, "k1".getBytes(), "v1".getBytes());
        manager.checkpoint();
        manager.close();

        Files.write(dir.resolve("segments").resolve("orders-0").resolve("00000000000000000001.log"),
                new byte[0],
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.WRITE);

        LogManager recovered = new LogManager(dir, config);

        List<Record> records = recovered.getOrCreate("orders", 0).read(0, 1024 * 1024);
        assertEquals(2, records.size());
        assertEquals("v0", new String(records.get(0).value()));
        assertEquals("v1", new String(records.get(1).value()));
    }

    @Test
    void replayIsIdempotentWhenPartitionLogAlreadyContainsRecords(@TempDir Path dir) {
        LogManager manager = new LogManager(dir);
        manager.append("orders", 0, "k0".getBytes(), "v0".getBytes());
        manager.append("orders", 0, "k1".getBytes(), "v1".getBytes());

        LogManager recovered = new LogManager(dir);

        assertEquals(2, recovered.getOrCreate("orders", 0).highWatermark());
    }

    @Test
    void startupFailsOnInvalidCheckpointPartitionKey(@TempDir Path dir) {
        CheckpointStore checkpoints = new CheckpointStore(dir);
        checkpoints.save(new RecoveryCheckpoint(0, Map.of("orders-x", 1L), 1));

        IllegalStateException failure = assertThrows(IllegalStateException.class, () -> new LogManager(dir));

        assertEquals("Invalid checkpoint partition key orders-x", failure.getMessage());
    }

    @Test
    void startupFailsOnCheckpointPartitionKeyWithoutSeparator(@TempDir Path dir) {
        CheckpointStore checkpoints = new CheckpointStore(dir);
        checkpoints.save(new RecoveryCheckpoint(0, Map.of("orders", 1L), 1));

        IllegalStateException failure = assertThrows(IllegalStateException.class, () -> new LogManager(dir));

        assertEquals("Invalid checkpoint partition key orders", failure.getMessage());
    }
}
