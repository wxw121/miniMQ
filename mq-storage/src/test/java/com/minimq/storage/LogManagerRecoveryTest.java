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
    void replaysFromWalWhenPartitionLogFallsBehindCheckpoint(@TempDir Path dir) throws Exception {
        LogManager manager = new LogManager(dir);
        manager.append("orders", 0, "k0".getBytes(), "v0".getBytes());
        manager.append("orders", 0, "k1".getBytes(), "v1".getBytes());
        manager.checkpoint();

        Files.write(dir.resolve("orders-0.log"), new byte[0],
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.WRITE);

        LogManager recovered = new LogManager(dir);

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
