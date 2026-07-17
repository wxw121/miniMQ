package com.minimq.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CheckpointStoreTest {

    @Test
    void saveAndLoadCheckpoint(@TempDir Path dir) {
        CheckpointStore store = new CheckpointStore(
                dir,
                Clock.fixed(Instant.ofEpochMilli(1234), ZoneOffset.UTC));

        store.save(store.checkpoint(42, Map.of("orders-0", 7L)));

        Optional<RecoveryCheckpoint> loaded = store.load();
        assertTrue(loaded.isPresent());
        assertEquals(42, loaded.get().walPosition);
        assertEquals(7, loaded.get().partitionHighWatermarks.get("orders-0"));
        assertEquals(1234, loaded.get().createdAtEpochMs);
    }

    @Test
    void corruptCheckpointFallsBackToEmpty(@TempDir Path dir) throws Exception {
        Path checkpoint = dir.resolve("checkpoint").resolve("recovery-checkpoint.json");
        Files.createDirectories(checkpoint.getParent());
        Files.writeString(checkpoint, "{not-json");

        CheckpointStore store = new CheckpointStore(dir);

        assertTrue(store.load().isEmpty());
    }

    @Test
    void saveDoesNotLeaveTemporaryCheckpointFile(@TempDir Path dir) {
        CheckpointStore store = new CheckpointStore(dir);

        store.save(store.checkpoint(1, Map.of("orders-0", 1L)));

        assertFalse(Files.exists(dir.resolve("checkpoint").resolve("recovery-checkpoint.json.tmp")));
    }

    @Test
    void missingOrEmptyCheckpointFallsBackToEmpty(@TempDir Path dir) throws Exception {
        CheckpointStore store = new CheckpointStore(dir);
        assertTrue(store.load().isEmpty());

        Path checkpoint = dir.resolve("checkpoint").resolve("recovery-checkpoint.json");
        Files.createDirectories(checkpoint.getParent());
        Files.write(checkpoint, new byte[0]);

        assertTrue(store.load().isEmpty());
    }

    @Test
    void rejectsInvalidCheckpointData(@TempDir Path dir) {
        CheckpointStore store = new CheckpointStore(dir);

        assertThrows(IllegalArgumentException.class,
                () -> new RecoveryCheckpoint(-1, Map.of("orders-0", 1L), 1));
        assertThrows(IllegalArgumentException.class,
                () -> store.save(new RecoveryCheckpoint(1, invalidHighWatermarkMap("", 1L), 1)));
        assertThrows(IllegalArgumentException.class,
                () -> store.save(new RecoveryCheckpoint(1, invalidHighWatermarkMap("orders-0", -1L), 1)));
    }

    @Test
    void rejectsInvalidCheckpointObjectsDuringSave(@TempDir Path dir) {
        CheckpointStore store = new CheckpointStore(dir);

        assertThrows(IllegalArgumentException.class, () -> store.save(null));

        RecoveryCheckpoint negativeWalPosition = new RecoveryCheckpoint();
        negativeWalPosition.walPosition = -1;
        negativeWalPosition.partitionHighWatermarks = Map.of();
        assertThrows(IllegalArgumentException.class, () -> store.save(negativeWalPosition));

        RecoveryCheckpoint missingHighWatermarks = new RecoveryCheckpoint();
        missingHighWatermarks.walPosition = 1;
        missingHighWatermarks.partitionHighWatermarks = null;
        assertThrows(IllegalArgumentException.class, () -> store.save(missingHighWatermarks));
    }

    @Test
    void buildsPartitionKey() {
        assertEquals("orders-3", RecoveryCheckpoint.partitionKey("orders", 3));
    }

    private Map<String, Long> invalidHighWatermarkMap(String key, Long value) {
        Map<String, Long> highWatermarks = new HashMap<>();
        highWatermarks.put(key, value);
        return highWatermarks;
    }
}
