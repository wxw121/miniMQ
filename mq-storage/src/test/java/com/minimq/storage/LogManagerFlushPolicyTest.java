package com.minimq.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LogManagerFlushPolicyTest {

    @Test
    void checkpointsAfterConfiguredAppendCount(@TempDir Path dir) {
        LogManager manager = new LogManager(
                dir,
                new LogManagerConfig(FlushPolicy.manualOnClose(), 2));
        CheckpointStore checkpoints = new CheckpointStore(dir);

        manager.append("orders", 0, "k0".getBytes(), "v0".getBytes());
        assertTrue(checkpoints.load().isPresent());
        assertFalse(checkpoints.load().get().partitionHighWatermarks.containsKey("orders-0"));

        manager.append("orders", 0, "k1".getBytes(), "v1".getBytes());
        RecoveryCheckpoint afterTwoAppends = checkpoints.load().orElseThrow();
        assertEquals(2, afterTwoAppends.partitionHighWatermarks.get("orders-0"));

        manager.append("orders", 0, "k2".getBytes(), "v2".getBytes());
        RecoveryCheckpoint stillAfterTwoAppends = checkpoints.load().orElseThrow();
        assertEquals(2, stillAfterTwoAppends.partitionHighWatermarks.get("orders-0"));

        manager.checkpoint();
        RecoveryCheckpoint explicitCheckpoint = checkpoints.load().orElseThrow();
        assertEquals(3, explicitCheckpoint.partitionHighWatermarks.get("orders-0"));
    }

    @Test
    void concurrentAppendsAllocateUniqueSequentialOffsets(@TempDir Path dir) throws Exception {
        LogManager manager = new LogManager(dir, new LogManagerConfig(FlushPolicy.always(), 0));
        int appendCount = 64;
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(8);
        List<Future<Long>> futures = new ArrayList<>();

        try {
            for (int i = 0; i < appendCount; i++) {
                final int recordId = i;
                futures.add(executor.submit((Callable<Long>) () -> {
                    start.await();
                    return manager.append(
                            "orders",
                            0,
                            ("k" + recordId).getBytes(),
                            ("v" + recordId).getBytes());
                }));
            }

            start.countDown();
            for (Future<Long> future : futures) {
                future.get();
            }
        } finally {
            executor.shutdownNow();
        }

        assertEquals(appendCount, manager.getOrCreate("orders", 0).highWatermark());
    }

    @Test
    void closeFlushesCheckpointAndRejectsFurtherAppends(@TempDir Path dir) {
        LogManager manager = new LogManager(dir, new LogManagerConfig(FlushPolicy.manualOnClose(), 0));
        manager.append("orders", 0, "k0".getBytes(), "v0".getBytes());

        manager.close();
        manager.close();

        RecoveryCheckpoint checkpoint = new CheckpointStore(dir).load().orElseThrow();
        assertEquals(1, checkpoint.partitionHighWatermarks.get("orders-0"));
        assertThrows(IllegalStateException.class, () -> manager.getOrCreate("orders", 0));
        assertThrows(IllegalStateException.class, manager::checkpoint);
        assertThrows(IllegalStateException.class,
                () -> manager.append("orders", 0, "k1".getBytes(), "v1".getBytes()));
    }
}
