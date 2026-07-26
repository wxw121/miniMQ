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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LogManagerTopicDeletionTest {
    @TempDir
    Path tempDir;

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
    void maintenanceSkipsPartitionWithFutureScheduledRecord() {
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
            manager.append("scheduled", 0, bytes("a"), bytes("future"), 50_000L);
            manager.append("scheduled", 0, bytes("b"), bytes("ready"));

            LogMaintenanceReport report = manager.maintain();

            assertEquals(MaintenanceStatus.NOOP, report.status());
            assertEquals(List.of(0L, 1L), manager.getOrCreate("scheduled", 0).read(0, 4_096)
                    .stream().map(Record::offset).toList());
            assertEquals(0L, manager.visibleHighWatermark("scheduled", 0));
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
}
