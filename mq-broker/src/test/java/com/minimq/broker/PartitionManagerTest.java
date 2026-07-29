package com.minimq.broker;

import com.minimq.storage.LogManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;

class PartitionManagerTest {

    @TempDir
    Path tempDir;

    @Test
    void exposesOneStorageVisibilitySnapshot() {
        LogManager logManager = new LogManager(tempDir);
        PartitionManager partitions = new PartitionManager(logManager);
        try {
            long deliverAtEpochMs = System.currentTimeMillis() + 60_000L;
            partitions.append(
                    "scheduled",
                    0,
                    "key".getBytes(StandardCharsets.UTF_8),
                    "value".getBytes(StandardCharsets.UTF_8),
                    deliverAtEpochMs);

            LogManager.VisibilitySnapshot snapshot =
                    partitions.visibilitySnapshot("scheduled", 0);

            assertEquals(1L, snapshot.physicalHighWatermark());
            assertEquals(0L, snapshot.visibleHighWatermark());
            assertEquals(deliverAtEpochMs, snapshot.nextVisibleAtEpochMs());
        } finally {
            partitions.close();
        }
    }
}
