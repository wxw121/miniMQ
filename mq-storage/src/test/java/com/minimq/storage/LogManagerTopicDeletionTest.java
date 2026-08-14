package com.minimq.storage;

import com.minimq.common.model.Record;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LogManagerTopicDeletionTest {
    @TempDir
    Path tempDir;

    @Test
    void rejectsTopicNamesThatEscapeTheSegmentsDirectory() {
        Path dataDir = tempDir.resolve("nested").resolve("data");
        LogManager manager = new LogManager(dataDir);
        try {
            assertThrows(IllegalArgumentException.class,
                    () -> manager.append("../../../escaped", 0, bytes("a"), bytes("value")));
            assertTrue(Files.notExists(tempDir.resolve("escaped-0")));
        } finally {
            manager.close();
        }
    }

    @Test
    void deletesOnlyExactLogicalTopicAndPersistsBeginCommit() throws Exception {
        LogManager manager = new LogManager(tempDir);
        manager.append("orders", 0, bytes("a"), bytes("one"));
        manager.append("orders-v2", 0, bytes("b"), bytes("two"));
        manager.deleteTopic("orders", 17L);
        manager.close();

        assertThrows(IllegalStateException.class, () -> {
            LogManager reopened = new LogManager(tempDir);
            try {
                reopened.getOrCreate("orders", 0);
            } finally {
                reopened.close();
            }
        });
        try (WriteAheadLog wal = new WriteAheadLog(tempDir.resolve("wal").resolve("minimq.wal"))) {
            List<WalRecordType> types = wal.readAll().stream().map(WalRecord::type).toList();
            assertTrue(types.contains(WalRecordType.TOPIC_DELETE_BEGIN));
            assertTrue(types.contains(WalRecordType.TOPIC_DELETE_COMMIT));
        }

        LogManager unaffected = new LogManager(tempDir);
        try {
            List<Record> records = unaffected.getOrCreate("orders-v2", 0).read(0, 1024);
            assertEquals(1, records.size());
        } finally {
            unaffected.close();
        }
    }

    @Test
    void deletedTopicPortableIdentityRemainsReservedAfterRestart() {
        LogManager manager = new LogManager(tempDir);
        manager.append("Orders", 0, bytes("key"), bytes("value"));
        manager.deleteTopic("Orders", 21L);
        manager.close();

        LogManager recovered = new LogManager(tempDir);
        try {
            IllegalArgumentException failure = assertThrows(
                    IllegalArgumentException.class,
                    () -> recovered.getOrCreate("orders", 0));
            assertTrue(failure.getMessage().contains("storage identity"), failure::getMessage);
        } finally {
            recovered.close();
        }
    }

    @Test
    void deleteSerializesWithNewPartitionOpen() throws Exception {
        LogManager manager = new LogManager(tempDir);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        AtomicReference<Thread> openerThread = new AtomicReference<>();
        try {
            Future<PartitionLog> opening;
            synchronized (manager) {
                opening = executor.submit(() -> {
                    openerThread.set(Thread.currentThread());
                    return manager.getOrCreate("orders", 0);
                });
                long waitDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                while (!opening.isDone()
                        && (openerThread.get() == null
                        || openerThread.get().getState() != Thread.State.BLOCKED)
                        && System.nanoTime() < waitDeadline) {
                    Thread.onSpinWait();
                }

                assertFalse(opening.isDone(),
                        "getOrCreate opened a partition while topic deletion held the manager lock");
                assertEquals(Thread.State.BLOCKED, openerThread.get().getState());
                manager.deleteTopic("orders", 41L);
            }

            ExecutionException failure = assertThrows(
                    ExecutionException.class,
                    () -> opening.get(5, TimeUnit.SECONDS));
            assertTrue(failure.getCause() instanceof IllegalStateException);
            assertTrue(Files.notExists(tempDir.resolve("segments").resolve("orders-0")));
        } finally {
            executor.shutdownNow();
            manager.close();
        }
    }

    @Test
    void recoveryCompletesUnmatchedBeginBeforeOpeningOldLog() throws Exception {
        LogManager manager = new LogManager(tempDir);
        manager.append("orders", 0, bytes("a"), bytes("one"));
        manager.close();

        try (WriteAheadLog wal = new WriteAheadLog(tempDir.resolve("wal").resolve("minimq.wal"))) {
            wal.append(WalRecord.topicDeleteBegin("orders", 23L), true);
        }

        LogManager recovered = new LogManager(tempDir);
        recovered.close();
        try (WriteAheadLog wal = new WriteAheadLog(tempDir.resolve("wal").resolve("minimq.wal"))) {
            List<WalRecord> records = wal.readAll();
            assertTrue(records.stream().anyMatch(r -> r.type() == WalRecordType.TOPIC_DELETE_COMMIT
                    && r.topic().equals("orders") && r.offset() == 23L));
        }
        assertTrue(Files.notExists(tempDir.resolve("segments").resolve("orders-0")));
    }

    @Test
    void scheduledDeliveriesSurviveCheckpointRecovery() {
        MutableClock clock = new MutableClock(1_000L);
        LogManagerConfig config = new LogManagerConfig(
                FlushPolicy.always(),
                1,
                SegmentedLogConfig.defaults(),
                LogMaintenanceConfig.disabled(),
                100);
        long deliverAtEpochMs = 5_000L;

        LogManager manager = new LogManager(tempDir, config, clock);
        manager.append("scheduled", 0, bytes("a"), bytes("future"), deliverAtEpochMs);
        manager.checkpoint();
        manager.close();

        LogManager recovered = new LogManager(tempDir, config, clock);
        try {
            assertEquals(0L, recovered.visibleHighWatermark("scheduled", 0));
            assertEquals(deliverAtEpochMs, recovered.nextVisibleAtEpochMs("scheduled", 0));
            assertEquals(1, recovered.scheduledRecordCount());

            clock.setMillis(6_000L);
            assertEquals(1L, recovered.visibleHighWatermark("scheduled", 0));
            assertEquals(0, recovered.scheduledRecordCount());
        } finally {
            recovered.close();
        }

        RecoveryCheckpoint checkpoint = new CheckpointStore(tempDir).load().orElseThrow();
        assertTrue(checkpoint.scheduledDeliveries.isEmpty());
    }

    @Test
    void scheduledCountPersistsPrunedVisibilityBeforeReturning() {
        MutableClock clock = new MutableClock(1_000L);
        LogManagerConfig config = new LogManagerConfig(
                FlushPolicy.always(),
                1,
                SegmentedLogConfig.defaults(),
                LogMaintenanceConfig.disabled(),
                100);
        LogManager manager = new LogManager(tempDir, config, clock);
        manager.append("scheduled", 0, bytes("a"), bytes("future"), 5_000L);

        clock.setMillis(6_000L);
        assertEquals(0, manager.scheduledRecordCount());

        clock.setMillis(1_000L);
        LogManager recovered = new LogManager(tempDir, config, clock);
        try {
            assertEquals(1L, recovered.visibleHighWatermark("scheduled", 0));
            assertEquals(-1L, recovered.nextVisibleAtEpochMs("scheduled", 0));
        } finally {
            recovered.close();
            manager.close();
        }
    }

    @Test
    void returnsPhysicalVisibleAndNextVisibilityFromOneSnapshot() {
        MutableClock clock = new MutableClock(1_000L);
        LogManager manager = new LogManager(tempDir, LogManagerConfig.defaults(), clock);
        try {
            manager.append("scheduled", 0, bytes("a"), bytes("ready"));
            manager.append("scheduled", 0, bytes("b"), bytes("future"), 5_000L);

            LogManager.VisibilitySnapshot snapshot = manager.visibilitySnapshot("scheduled", 0);

            assertEquals(2L, snapshot.physicalHighWatermark());
            assertEquals(1L, snapshot.visibleHighWatermark());
            assertEquals(5_000L, snapshot.nextVisibleAtEpochMs());
        } finally {
            manager.close();
        }
    }

    @Test
    void visibilitySnapshotUsesOneDeliveryTimeObservation() {
        BoundaryClock clock = new BoundaryClock();
        LogManager manager = new LogManager(tempDir, LogManagerConfig.defaults(), clock);
        try {
            manager.append("scheduled", 0, bytes("a"), bytes("future"), 5_000L);
            clock.armBoundary();

            LogManager.VisibilitySnapshot snapshot = manager.visibilitySnapshot("scheduled", 0);

            assertEquals(1L, snapshot.physicalHighWatermark());
            assertEquals(0L, snapshot.visibleHighWatermark());
            assertEquals(5_000L, snapshot.nextVisibleAtEpochMs());
            assertEquals(1, clock.boundaryReadCount());
        } finally {
            manager.close();
        }
    }

    @Test
    void visibilitySnapshotAndCheckpointShareTheSameDeliveryTimeObservation() {
        BoundaryClock clock = new BoundaryClock();
        LogManager manager = new LogManager(tempDir, LogManagerConfig.defaults(), clock);
        try {
            manager.append("scheduled", 0, bytes("a"), bytes("first"), 4_000L);
            manager.append("scheduled", 0, bytes("b"), bytes("second"), 5_000L);
            clock.armBoundary();

            LogManager.VisibilitySnapshot snapshot = manager.visibilitySnapshot("scheduled", 0);
            RecoveryCheckpoint checkpoint = new CheckpointStore(tempDir).load().orElseThrow();

            assertEquals(2L, snapshot.physicalHighWatermark());
            assertEquals(1L, snapshot.visibleHighWatermark());
            assertEquals(5_000L, snapshot.nextVisibleAtEpochMs());
            assertEquals(
                    Map.of(1L, 5_000L),
                    checkpoint.scheduledDeliveries.get("scheduled-0"));
            assertEquals(1L, checkpoint.visibleDeliveryWatermarks.get("scheduled-0"));
        } finally {
            manager.close();
        }
    }

    @Test
    void replicatedFutureDeliveryPersistsScheduledWalAndSurvivesRestart() throws Exception {
        MutableClock clock = new MutableClock(1_000L);
        LogManagerConfig config = new LogManagerConfig(
                FlushPolicy.always(),
                0,
                SegmentedLogConfig.defaults(),
                LogMaintenanceConfig.disabled(),
                100);
        long deliverAtEpochMs = 5_000L;

        LogManager manager = new LogManager(tempDir, config, clock);
        manager.replicateAt("scheduled", 0, 0L, bytes("a"), bytes("future"), deliverAtEpochMs);
        assertEquals(0L, manager.visibleHighWatermark("scheduled", 0));
        manager.close();

        try (WriteAheadLog wal = new WriteAheadLog(tempDir.resolve("wal").resolve("minimq.wal"))) {
            assertTrue(wal.readAll().stream().anyMatch(record ->
                    record.type() == WalRecordType.SCHEDULED_DATA_APPEND
                            && record.topic().equals("scheduled")
                            && record.partitionId() == 0
                            && record.offset() == 0L
                            && record.deliverAtEpochMs() == deliverAtEpochMs));
        }

        LogManager recovered = new LogManager(tempDir, config, clock);
        try {
            assertEquals(0L, recovered.visibleHighWatermark("scheduled", 0));
            assertEquals(deliverAtEpochMs, recovered.nextVisibleAtEpochMs("scheduled", 0));

            clock.setMillis(6_000L);
            assertEquals(1L, recovered.visibleHighWatermark("scheduled", 0));
        } finally {
            recovered.close();
        }
    }

    @Test
    void maintenanceProtectsOnlySegmentsContainingFutureScheduledRecords() {
        MutableClock clock = new MutableClock(1_000L);
        LogMaintenanceConfig maintenance = LogMaintenanceConfig.of(
                RetentionPolicy.of(0, 1),
                CompactionPolicy.enabled(10),
                0);
        LogManagerConfig config = new LogManagerConfig(
                FlushPolicy.always(),
                0,
                new SegmentedLogConfig(1, 1),
                maintenance,
                100);
        LogManager manager = new LogManager(tempDir, config, clock);
        try {
            manager.append("scheduled", 0, bytes("ready-0"), bytes("ready-0"));
            manager.append("scheduled", 0, bytes("ready-1"), bytes("ready-1"));
            manager.append("scheduled", 0, bytes("future-2"), bytes("future-2"), 50_000L);
            manager.append("scheduled", 0, bytes("ready-3"), bytes("ready-3"));
            manager.append("scheduled", 0, bytes("future-4"), bytes("future-4"), 60_000L);

            Path partitionDir = tempDir.resolve("segments").resolve("scheduled-0");
            LogMaintenanceReport preview = manager.dryRunMaintenance();

            assertEquals(List.of(0L, 1L, 3L), preview.segmentReports().stream()
                    .map(MaintenanceSegmentReport::baseOffset)
                    .toList());
            assertTrue(List.of(0L, 1L, 2L, 3L, 4L).stream()
                    .allMatch(baseOffset -> Files.exists(SegmentFile.logPath(partitionDir, baseOffset))));

            LogMaintenanceReport report = manager.maintain();

            assertEquals(preview.segmentReports(), report.segmentReports());
            assertEquals(MaintenanceStatus.SUCCESS, report.status());
            assertEquals(List.of(2L, 4L), manager.getOrCreate("scheduled", 0).read(0, 4_096)
                    .stream().map(Record::offset).toList());
            assertTrue(List.of(0L, 1L, 3L).stream()
                    .noneMatch(baseOffset -> Files.exists(SegmentFile.logPath(partitionDir, baseOffset))));
            assertTrue(List.of(2L, 4L).stream()
                    .allMatch(baseOffset -> Files.exists(SegmentFile.logPath(partitionDir, baseOffset))));
            assertEquals(2L, manager.visibleHighWatermark("scheduled", 0));
            assertEquals(50_000L, manager.nextVisibleAtEpochMs("scheduled", 0));
        } finally {
            manager.close();
        }
    }

    @Test
    void maxScheduledRecordsRejectsOverflowBeforeWriting() {
        MutableClock clock = new MutableClock(1_000L);
        LogManagerConfig config = new LogManagerConfig(
                FlushPolicy.always(),
                0,
                SegmentedLogConfig.defaults(),
                LogMaintenanceConfig.disabled(),
                1);
        LogManager manager = new LogManager(tempDir, config, clock);
        try {
            manager.append("scheduled", 0, bytes("a"), bytes("future"), 50_000L);

            assertThrows(IllegalStateException.class,
                    () -> manager.append("scheduled", 0, bytes("b"), bytes("overflow"), 60_000L));

            assertEquals(1, manager.scheduledRecordCount());
            assertEquals(List.of(0L), manager.getOrCreate("scheduled", 0).read(0, 4_096)
                    .stream().map(Record::offset).toList());
        } finally {
            manager.close();
        }
    }

    @Test
    void zeroMaxScheduledRecordsDisablesNewFutureAdmission() {
        MutableClock clock = new MutableClock(1_000L);
        LogManagerConfig config = new LogManagerConfig(
                FlushPolicy.always(),
                0,
                SegmentedLogConfig.defaults(),
                LogMaintenanceConfig.disabled(),
                0);
        LogManager manager = new LogManager(tempDir, config, clock);
        try {
            assertThrows(LogManager.ScheduledCapacityExceededException.class,
                    () -> manager.append("scheduled", 0, bytes("a"), bytes("future"), 50_000L));

            long offset = manager.append("scheduled", 0, bytes("b"), bytes("ready"), 0L);

            assertEquals(0L, offset);
            assertEquals(0, manager.scheduledRecordCount());
            assertEquals(1L, manager.getOrCreate("scheduled", 0).highWatermark());
        } finally {
            manager.close();
        }
    }

    @Test
    void appendBatchRejectsScheduledCapacityOverflowBeforeAnyRecordIsWritten() {
        MutableClock clock = new MutableClock(1_000L);
        LogManagerConfig config = new LogManagerConfig(
                FlushPolicy.always(),
                0,
                SegmentedLogConfig.defaults(),
                LogMaintenanceConfig.disabled(),
                1);
        LogManager manager = new LogManager(tempDir, config, clock);
        try {
            assertThrows(LogManager.ScheduledCapacityExceededException.class,
                    () -> manager.appendBatch(
                            "scheduled",
                            0,
                            List.of(
                                    new LogManager.AppendRecord(bytes("a"), bytes("future-1"), 50_000L),
                                    new LogManager.AppendRecord(bytes("b"), bytes("future-2"), 60_000L))));

            assertEquals(0, manager.scheduledRecordCount());
            assertEquals(0L, manager.getOrCreate("scheduled", 0).highWatermark());
        } finally {
            manager.close();
        }
    }

    @Test
    void replicateAtRejectsScheduledCapacityOverflow() {
        MutableClock clock = new MutableClock(1_000L);
        LogManagerConfig config = new LogManagerConfig(
                FlushPolicy.always(),
                0,
                SegmentedLogConfig.defaults(),
                LogMaintenanceConfig.disabled(),
                1);
        LogManager manager = new LogManager(tempDir, config, clock);
        try {
            manager.append("scheduled", 0, bytes("a"), bytes("future-1"), 50_000L);
            assertThrows(LogManager.ScheduledCapacityExceededException.class,
                    () -> manager.replicateAt(
                            "scheduled", 0, 1L, bytes("b"), bytes("future-2"), 60_000L));

            assertEquals(1, manager.scheduledRecordCount());
            assertEquals(1L, manager.getOrCreate("scheduled", 0).highWatermark());

            manager.replicateAt("scheduled", 0, 1L, bytes("c"), bytes("ready"), 0L);
            assertEquals(1, manager.scheduledRecordCount());
            assertEquals(2L, manager.getOrCreate("scheduled", 0).highWatermark());
        } finally {
            manager.close();
        }
    }

    @Test
    void recoveryKeepsScheduledRecordsAboveCapAndRejectsNewFutureAppends() {
        MutableClock clock = new MutableClock(1_000L);
        LogManagerConfig writeConfig = new LogManagerConfig(
                FlushPolicy.always(),
                0,
                SegmentedLogConfig.defaults(),
                LogMaintenanceConfig.disabled(),
                2);
        try (LogManager manager = new LogManager(tempDir, writeConfig, clock)) {
            manager.append("scheduled", 0, bytes("a"), bytes("future-1"), 50_000L);
            manager.append("scheduled", 0, bytes("b"), bytes("future-2"), 60_000L);
        }

        LogManagerConfig recoveredConfig = new LogManagerConfig(
                FlushPolicy.always(),
                0,
                SegmentedLogConfig.defaults(),
                LogMaintenanceConfig.disabled(),
                1);
        LogManager recovered = new LogManager(tempDir, recoveredConfig, clock);
        try {
            assertEquals(2, recovered.scheduledRecordCount());
            assertThrows(LogManager.ScheduledCapacityExceededException.class,
                    () -> recovered.append("scheduled", 0, bytes("c"), bytes("future-3"), 70_000L));
            assertEquals(2L, recovered.append("scheduled", 0, bytes("d"), bytes("ready"), 0L));
            assertEquals(2, recovered.scheduledRecordCount());
            assertEquals(3L, recovered.getOrCreate("scheduled", 0).highWatermark());
        } finally {
            recovered.close();
        }
    }

    @Test
    void visibleDeliveryWatermarkSurvivesRestartAndClockRollback() {
        MutableClock clock = new MutableClock(1_000L);
        LogManagerConfig config = new LogManagerConfig(
                FlushPolicy.always(),
                1,
                SegmentedLogConfig.defaults(),
                LogMaintenanceConfig.disabled(),
                100);
        LogManager manager = new LogManager(tempDir, config, clock);
        manager.append("scheduled", 0, bytes("a"), bytes("future"), 5_000L);
        clock.setMillis(6_000L);
        assertEquals(1L, manager.visibleHighWatermark("scheduled", 0));
        manager.close();

        RecoveryCheckpoint checkpoint = new CheckpointStore(tempDir).load().orElseThrow();
        assertEquals(1L, checkpoint.visibleDeliveryWatermarks.get("scheduled-0"));

        clock.setMillis(1_000L);
        LogManager recovered = new LogManager(tempDir, config, clock);
        try {
            assertEquals(1L, recovered.visibleHighWatermark("scheduled", 0));
            assertEquals(-1L, recovered.nextVisibleAtEpochMs("scheduled", 0));
            assertEquals(0, recovered.scheduledRecordCount());
        } finally {
            recovered.close();
        }
    }

    @Test
    void readyAppendsAfterDelayedDeliveryKeepVisibleWatermarkContiguous() {
        MutableClock clock = new MutableClock(1_000L);
        LogManagerConfig config = new LogManagerConfig(
                FlushPolicy.always(),
                1,
                SegmentedLogConfig.defaults(),
                LogMaintenanceConfig.disabled(),
                100);
        LogManager manager = new LogManager(tempDir, config, clock);
        try {
            manager.append("scheduled", 0, bytes("a"), bytes("future"), 5_000L);
            clock.setMillis(6_000L);
            assertEquals(1L, manager.visibleHighWatermark("scheduled", 0));

            manager.append("scheduled", 0, bytes("b"), bytes("ready"));
            assertEquals(2L, manager.visibleHighWatermark("scheduled", 0));
            assertEquals(-1L, manager.nextVisibleAtEpochMs("scheduled", 0));
        } finally {
            manager.close();
        }
    }

    @Test
    void concurrentFutureAppendsDoNotExceedScheduledCapacity() throws Exception {
        MutableClock clock = new MutableClock(1_000L);
        LogManagerConfig config = new LogManagerConfig(
                FlushPolicy.always(),
                0,
                SegmentedLogConfig.defaults(),
                LogMaintenanceConfig.disabled(),
                8);
        LogManager manager = new LogManager(tempDir, config, clock);
        int workers = 16;
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(workers);
        AtomicInteger admitted = new AtomicInteger();
        AtomicInteger rejected = new AtomicInteger();
        try {
            List<Future<?>> futures = new java.util.ArrayList<>();
            for (int i = 0; i < workers; i++) {
                final int partitionId = i;
                futures.add(executor.submit(() -> {
                    start.await();
                    try {
                        manager.append(
                                "scheduled",
                                partitionId,
                                bytes("k" + partitionId),
                                bytes("future"),
                                50_000L);
                        admitted.incrementAndGet();
                    } catch (LogManager.ScheduledCapacityExceededException ignored) {
                        rejected.incrementAndGet();
                    }
                    return null;
                }));
            }
            start.countDown();
            for (Future<?> future : futures) {
                future.get();
            }
            assertEquals(8, admitted.get());
            assertEquals(8, rejected.get());
            assertEquals(8, manager.scheduledRecordCount());

            assertThrows(
                    LogManager.ScheduledCapacityExceededException.class,
                    () -> manager.append("scheduled", 0, bytes("overflow"), bytes("future"), 60_000L));
        } finally {
            executor.shutdownNow();
            manager.close();
        }
    }

    private static byte[] bytes(String value) {
        return value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    private static final class MutableClock extends Clock {
        private long epochMillis;
        private final ZoneId zone;

        private MutableClock(long epochMillis) {
            this(epochMillis, ZoneId.of("UTC"));
        }

        private MutableClock(long epochMillis, ZoneId zone) {
            this.epochMillis = epochMillis;
            this.zone = zone;
        }

        private void setMillis(long epochMillis) {
            this.epochMillis = epochMillis;
        }

        @Override
        public ZoneId getZone() {
            return zone;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return new MutableClock(epochMillis, zone);
        }

        @Override
        public Instant instant() {
            return Instant.ofEpochMilli(epochMillis);
        }
    }

    private static final class BoundaryClock extends Clock {
        private int boundaryReads = -1;

        private void armBoundary() {
            boundaryReads = 0;
        }

        private int boundaryReadCount() {
            return boundaryReads;
        }

        @Override
        public ZoneId getZone() {
            return ZoneId.of("UTC");
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            if (boundaryReads < 0) {
                return Instant.ofEpochMilli(1_000L);
            }
            return Instant.ofEpochMilli(boundaryReads++ < 2 ? 4_999L : 5_000L);
        }
    }
}
