package com.minimq.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
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
    void batchFlushesWhenItCrossesTheConfiguredAppendCount(@TempDir Path dir) throws Exception {
        MutableClock clock = new MutableClock();
        try (LogManager manager = new LogManager(
                dir, new LogManagerConfig(FlushPolicy.everyNAppends(100), 0), clock)) {
            manager.append("orders", 0, null, new byte[]{1});
            clock.now = 2_000L;
            manager.appendBatch("orders", 0, batch(50));
            manager.appendBatch("orders", 0, batch(50));

            var lastFlush = LogManager.class.getDeclaredField("lastFlushEpochMs");
            lastFlush.setAccessible(true);
            assertEquals(2_000L, lastFlush.getLong(manager));
            assertEquals(101L, manager.getOrCreate("orders", 0).highWatermark());
        }
    }

    @Test
    void batchCrossingCadencesRunsCheckpointAndMaintenance(@TempDir Path dir) {
        LogMaintenanceConfig maintenance = LogMaintenanceConfig.of(
                RetentionPolicy.of(1_000, 10_000), CompactionPolicy.disabled(), 100);
        try (LogManager manager = new LogManager(dir, new LogManagerConfig(
                FlushPolicy.manualOnClose(), 100, SegmentedLogConfig.defaults(), maintenance))) {
            manager.append("orders", 0, null, new byte[]{1});
            manager.appendBatch("orders", 0, batch(50));
            assertFalse(new CheckpointStore(dir).load().orElseThrow()
                    .partitionHighWatermarks.containsKey("orders-0"));
            manager.appendBatch("orders", 0, batch(50));

            assertEquals(101L, new CheckpointStore(dir).load().orElseThrow()
                    .partitionHighWatermarks.get("orders-0"));
            assertEquals(1L, manager.maintenanceMetrics().runs());
        }
    }

    private static List<LogManager.AppendRecord> batch(int size) {
        return Collections.nCopies(size, new LogManager.AppendRecord(null, new byte[]{1}, 0L));
    }

    private static final class MutableClock extends Clock {
        private long now = 1_000L;

        @Override public ZoneId getZone() { return ZoneOffset.UTC; }
        @Override public Clock withZone(ZoneId zone) { return this; }
        @Override public Instant instant() { return Instant.ofEpochMilli(now); }
        @Override public long millis() { return now; }
    }

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
    void concurrentAppendsToDistinctPartitionsDoNotShareOffsets(@TempDir Path dir) throws Exception {
        LogManager manager = new LogManager(dir, new LogManagerConfig(FlushPolicy.always(), 0));
        int partitions = 4;
        int perPartition = 32;
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(partitions);
        List<Future<?>> futures = new ArrayList<>();

        try {
            for (int partitionId = 0; partitionId < partitions; partitionId++) {
                final int id = partitionId;
                futures.add(executor.submit(() -> {
                    start.await();
                    for (int i = 0; i < perPartition; i++) {
                        manager.append(
                                "orders",
                                id,
                                ("k" + id + "-" + i).getBytes(),
                                ("v" + id + "-" + i).getBytes());
                    }
                    return null;
                }));
            }

            start.countDown();
            for (Future<?> future : futures) {
                future.get();
            }
        } finally {
            executor.shutdownNow();
        }

        for (int partitionId = 0; partitionId < partitions; partitionId++) {
            assertEquals(perPartition, manager.getOrCreate("orders", partitionId).highWatermark());
        }
        manager.close();
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

    @Test
    void appendBatchWritesContiguousOffsetsAndRejectsEmptyOrInvalidInput(@TempDir Path dir) {
        LogManager manager = new LogManager(dir, new LogManagerConfig(FlushPolicy.always(), 0));
        try {
            assertEquals(-1L, manager.appendBatch("orders", 0, List.of()));
            assertEquals(-1L, manager.appendBatch("orders", 0, null));

            long base = manager.appendBatch("orders", 0, List.of(
                    new LogManager.AppendRecord("k0".getBytes(), "v0".getBytes(), 0L),
                    new LogManager.AppendRecord("k1".getBytes(), "v1".getBytes(), 0L),
                    new LogManager.AppendRecord(
                            "k2".getBytes(),
                            "v2".getBytes(),
                            System.currentTimeMillis() + 86_400_000L)));
            assertEquals(0L, base);
            assertEquals(3L, manager.getOrCreate("orders", 0).highWatermark());

            manager.replicateAt("orders", 0, 3L, "k3".getBytes(), "v3".getBytes());
            assertEquals(4L, manager.getOrCreate("orders", 0).highWatermark());
        } finally {
            manager.close();
        }
    }

    @Test
    void futureAppendReleasesReservationWhenTopicIsAlreadyDeleted(@TempDir Path dir) {
        LogManager manager = new LogManager(
                dir,
                new LogManagerConfig(
                        FlushPolicy.always(),
                        0,
                        SegmentedLogConfig.defaults(),
                        LogMaintenanceConfig.disabled(),
                        1));
        try {
            long deliverAt = System.currentTimeMillis() + 86_400_000L;
            manager.deleteTopic("gone", 1L);
            assertThrows(IllegalStateException.class,
                    () -> manager.append("gone", 0, "k".getBytes(), "v".getBytes(), deliverAt));
            long offset = manager.append("kept", 0, "k".getBytes(), "v".getBytes(), deliverAt);
            assertEquals(0L, offset);
            assertEquals(1, manager.scheduledRecordCount());
        } finally {
            manager.close();
        }
    }
}
