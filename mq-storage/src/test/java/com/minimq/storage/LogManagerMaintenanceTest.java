package com.minimq.storage;

import com.minimq.common.model.Record;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Method;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LogManagerMaintenanceTest {

    @Test
    void appendCadenceRunsRetentionAndWritesCheckpoint(@TempDir Path dir) {
        LogMaintenanceConfig maintenance = LogMaintenanceConfig.of(
                RetentionPolicy.of(0, 1),
                CompactionPolicy.disabled(),
                1);
        LogManagerConfig config = new LogManagerConfig(
                FlushPolicy.always(),
                0,
                new SegmentedLogConfig(1, 1),
                maintenance);
        LogManager manager = new LogManager(dir, config);

        manager.append("orders", 0, "k0".getBytes(), "v0".getBytes());
        manager.append("orders", 0, "k1".getBytes(), "v1".getBytes());
        assertTrue(manager.maintenanceMetrics().deletedBytes() > 0);
        manager.close();

        LogManager recovered = new LogManager(dir, LogManagerConfig.defaults());
        try {
            assertEquals(List.of(1L), recovered.getOrCreate("orders", 0).read(0, 4_096)
                    .stream().map(Record::offset).toList());
        } finally {
            recovered.close();
        }
    }

    @Test
    void manualMaintenanceRunsCompaction(@TempDir Path dir) {
        LogMaintenanceConfig maintenance = LogMaintenanceConfig.of(
                RetentionPolicy.of(0, 1),
                CompactionPolicy.enabled(10),
                0);
        LogManager manager = new LogManager(dir, new LogManagerConfig(
                FlushPolicy.always(),
                0,
                new SegmentedLogConfig(1, 1),
                maintenance));

        manager.append("profiles", 0, "a".getBytes(), "old".getBytes());
        manager.append("profiles", 0, "a".getBytes(), "new".getBytes());
        manager.maintain();

        assertEquals(List.of(1L), manager.getOrCreate("profiles", 0).read(0, 4_096)
                .stream().map(Record::offset).toList());
    }

    @Test
    void dryRunReportsRetentionAndCompactionWithoutMutatingSegments(@TempDir Path dir) {
        LogMaintenanceConfig maintenance = LogMaintenanceConfig.of(
                RetentionPolicy.of(
                        0,
                        PartitionLogRecordCodec.encodedSize(1, "a".getBytes(), "new".getBytes())
                                + PartitionLogRecordCodec.encodedSize(2, "b".getBytes(), "active".getBytes())),
                CompactionPolicy.enabled(10),
                0);
        LogManager manager = new LogManager(dir, new LogManagerConfig(
                FlushPolicy.always(),
                0,
                new SegmentedLogConfig(1, 1),
                maintenance));

        manager.append("profiles", 0, "a".getBytes(), "old".getBytes());
        manager.append("profiles", 0, "a".getBytes(), "new".getBytes());
        manager.append("profiles", 0, "b".getBytes(), "active".getBytes());

        LogMaintenanceReport report = manager.dryRunMaintenance();

        assertTrue(report.dryRun());
        assertEquals(MaintenanceStatus.SUCCESS, report.status());
        assertTrue(report.deletedBytes() > 0);
        assertTrue(report.compactedSegments() > 0);
        assertTrue(report.segmentReports().stream().anyMatch(segment ->
                segment.partition().equals("profiles-0")
                        && segment.action() == MaintenanceAction.RETENTION_DELETE
                        && segment.baseOffset() == 0
                        && segment.firstOffset() == 0
                        && segment.lastOffset() == 0
                        && segment.sizeBytes() > 0));
        assertTrue(report.segmentReports().stream().anyMatch(segment ->
                segment.partition().equals("profiles-0")
                        && segment.action() == MaintenanceAction.COMPACTION_REWRITE
                        && segment.baseOffset() == 1
                        && segment.firstOffset() == 1
                        && segment.lastOffset() == 1
                        && segment.sizeBytes() > 0));
        assertEquals(List.of(0L, 1L, 2L), manager.getOrCreate("profiles", 0).read(0, 4_096)
                .stream().map(Record::offset).toList());
        assertEquals(1, manager.maintenanceMetrics().dryRuns());
        assertEquals(0, manager.maintenanceMetrics().runs());
    }

    @Test
    void dryRunPlansCompactionAfterSimulatedRetention(@TempDir Path dir) {
        LogMaintenanceConfig maintenance = LogMaintenanceConfig.of(
                RetentionPolicy.of(0, 1),
                CompactionPolicy.enabled(10),
                0);
        LogManager manager = new LogManager(dir, new LogManagerConfig(
                FlushPolicy.always(),
                0,
                new SegmentedLogConfig(1, 1),
                maintenance));

        manager.append("profiles", 0, "a".getBytes(), "old".getBytes());
        manager.append("profiles", 0, "a".getBytes(), "new".getBytes());
        manager.append("profiles", 0, "b".getBytes(), "active".getBytes());

        LogMaintenanceReport preview = manager.dryRunMaintenance();
        LogMaintenanceReport executed = manager.maintain();

        assertEquals(actionKeys(executed), actionKeys(preview));
        assertTrue(preview.segmentReports().stream().allMatch(
                report -> report.action() == MaintenanceAction.RETENTION_DELETE));
    }

    @Test
    void dryRunDoesNotReplaceLastRealMaintenanceStatus(@TempDir Path dir) {
        LogMaintenanceConfig maintenance = LogMaintenanceConfig.of(
                RetentionPolicy.disabled(),
                CompactionPolicy.enabled(10),
                0);
        LogManager manager = new LogManager(dir, new LogManagerConfig(
                FlushPolicy.always(),
                0,
                new SegmentedLogConfig(1, 1),
                maintenance));
        manager.append("profiles", 0, "a".getBytes(), "active".getBytes());

        manager.dryRunMaintenance();

        assertEquals(1, manager.maintenanceMetrics().dryRuns());
        assertEquals(MaintenanceStatus.NEVER_RUN, manager.maintenanceMetrics().lastRunStatus());
    }

    @Test
    void maintenanceSkipsWithoutBlockingWhenReaderSnapshotIsActive(@TempDir Path dir) throws Exception {
        LogMaintenanceConfig maintenance = LogMaintenanceConfig.of(
                RetentionPolicy.disabled(),
                CompactionPolicy.enabled(10),
                0);
        LogManager manager = new LogManager(dir, new LogManagerConfig(
                FlushPolicy.always(),
                0,
                new SegmentedLogConfig(1, 1),
                maintenance));
        manager.append("profiles", 0, "a".getBytes(), "old".getBytes());
        manager.append("profiles", 0, "a".getBytes(), "new".getBytes());
        manager.append("profiles", 0, "b".getBytes(), "active".getBytes());

        SegmentedPartitionLog log = (SegmentedPartitionLog) manager.getOrCreate("profiles", 0);
        Method acquireSnapshot = SegmentedPartitionLog.class.getDeclaredMethod("acquireReadSnapshot");
        acquireSnapshot.setAccessible(true);
        AutoCloseable snapshot = (AutoCloseable) acquireSnapshot.invoke(log);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<LogMaintenanceReport> future = executor.submit(manager::maintain);
            LogMaintenanceReport skipped = future.get(1, TimeUnit.SECONDS);

            assertEquals("SKIPPED_READERS", skipped.status().name());
            assertEquals(1, skipped.skippedDueToReaders());
            assertTrue(skipped.segmentReports().isEmpty());
            assertEquals(List.of(0L, 1L, 2L), manager.getOrCreate("profiles", 0).read(0, 4_096)
                    .stream().map(Record::offset).toList());
            assertEquals(1, manager.maintenanceMetrics().runs());
            assertEquals(1, manager.maintenanceMetrics().skippedDueToReaders());
            assertEquals("SKIPPED_READERS", manager.maintenanceMetrics().lastRunStatus().name());
        } finally {
            snapshot.close();
            executor.shutdownNow();
            executor.awaitTermination(5, TimeUnit.SECONDS);
        }

        LogMaintenanceReport executed = manager.maintain();
        assertEquals(MaintenanceStatus.SUCCESS, executed.status());
        assertEquals(List.of(1L, 2L), manager.getOrCreate("profiles", 0).read(0, 4_096)
                .stream().map(Record::offset).toList());
    }

    @Test
    void maintenanceReportsSuccessWhenAnotherPartitionSkipsActiveReaders(@TempDir Path dir) throws Exception {
        LogMaintenanceConfig maintenance = LogMaintenanceConfig.of(
                RetentionPolicy.disabled(),
                CompactionPolicy.enabled(10),
                0);
        LogManager manager = new LogManager(dir, new LogManagerConfig(
                FlushPolicy.always(),
                0,
                new SegmentedLogConfig(1, 1),
                maintenance));
        manager.append("blocked", 0, "a".getBytes(), "old".getBytes());
        manager.append("blocked", 0, "a".getBytes(), "new".getBytes());
        manager.append("blocked", 0, "b".getBytes(), "active".getBytes());
        manager.append("ready", 0, "a".getBytes(), "old".getBytes());
        manager.append("ready", 0, "a".getBytes(), "new".getBytes());
        manager.append("ready", 0, "b".getBytes(), "active".getBytes());

        SegmentedPartitionLog blocked = (SegmentedPartitionLog) manager.getOrCreate("blocked", 0);
        Method acquireSnapshot = SegmentedPartitionLog.class.getDeclaredMethod("acquireReadSnapshot");
        acquireSnapshot.setAccessible(true);
        AutoCloseable snapshot = (AutoCloseable) acquireSnapshot.invoke(blocked);
        try {
            LogMaintenanceReport report = manager.maintain();

            assertEquals(MaintenanceStatus.SUCCESS, report.status());
            assertEquals(1, report.skippedDueToReaders());
            assertTrue(report.segmentReports().stream()
                    .anyMatch(segment -> segment.partition().equals("ready-0")));
            assertEquals(MaintenanceStatus.SUCCESS, manager.maintenanceMetrics().lastRunStatus());
            assertEquals(1, manager.maintenanceMetrics().skippedDueToReaders());
        } finally {
            snapshot.close();
            manager.close();
        }
    }

    @Test
    void maintenanceMetricsTrackExecutedActionsAndLastStatus(@TempDir Path dir) {
        LogMaintenanceConfig maintenance = LogMaintenanceConfig.of(
                RetentionPolicy.disabled(),
                CompactionPolicy.enabled(10),
                0);
        LogManager manager = new LogManager(dir, new LogManagerConfig(
                FlushPolicy.always(),
                0,
                new SegmentedLogConfig(1, 1),
                maintenance));

        manager.append("profiles", 0, "a".getBytes(), "old".getBytes());
        manager.append("profiles", 0, "a".getBytes(), "new".getBytes());
        manager.append("profiles", 0, "b".getBytes(), "active".getBytes());

        LogMaintenanceReport report = manager.maintain();
        LogMaintenanceMetrics metrics = manager.maintenanceMetrics();

        assertFalse(report.dryRun());
        assertEquals(MaintenanceStatus.SUCCESS, report.status());
        assertEquals(1, metrics.runs());
        assertEquals(0, metrics.dryRuns());
        assertTrue(metrics.compactedSegments() > 0);
        assertTrue(metrics.compactedBytes() > 0);
        assertEquals(0, metrics.cleanupFailures());
        assertEquals(MaintenanceStatus.SUCCESS, metrics.lastRunStatus());
    }

    @Test
    void appendCadenceSkipsMaintenanceOutsideConfiguredWindowButManualRunStillExecutes(@TempDir Path dir) {
        Clock clock = Clock.fixed(Instant.parse("2026-07-16T03:00:00Z"), ZoneOffset.UTC);
        LogMaintenanceConfig maintenance = LogMaintenanceConfig.of(
                RetentionPolicy.of(0, 1),
                CompactionPolicy.disabled(),
                1,
                9,
                17);
        LogManager manager = new LogManager(dir, new LogManagerConfig(
                FlushPolicy.always(),
                0,
                new SegmentedLogConfig(1, 1),
                maintenance),
                clock);

        manager.append("orders", 0, "k0".getBytes(), "v0".getBytes());
        manager.append("orders", 0, "k1".getBytes(), "v1".getBytes());

        assertEquals(List.of(0L, 1L), manager.getOrCreate("orders", 0).read(0, 4_096)
                .stream().map(Record::offset).toList());
        assertEquals(MaintenanceStatus.SKIPPED_WINDOW, manager.maintenanceMetrics().lastRunStatus());

        manager.maintain();

        assertEquals(List.of(1L), manager.getOrCreate("orders", 0).read(0, 4_096)
                .stream().map(Record::offset).toList());
    }

    @Test
    void walReplaySkipsRetainedOffsetsBelowLogStartOffset(@TempDir Path dir) {
        LogMaintenanceConfig maintenance = LogMaintenanceConfig.of(
                RetentionPolicy.of(0, 1),
                CompactionPolicy.disabled(),
                1);
        LogManagerConfig config = new LogManagerConfig(
                FlushPolicy.always(),
                0,
                new SegmentedLogConfig(1, 1),
                maintenance);
        LogManager manager = new LogManager(dir, config);
        manager.append("orders", 0, "k0".getBytes(), "v0".getBytes());
        manager.append("orders", 0, "k1".getBytes(), "v1".getBytes());
        manager.close();

        new CheckpointStore(dir).save(new RecoveryCheckpoint(0, Map.of("orders-0", 999L), 1));

        LogManager recovered = new LogManager(dir, LogManagerConfig.defaults());
        try {
            assertEquals(List.of(1L), recovered.getOrCreate("orders", 0).read(0, 4_096)
                    .stream().map(Record::offset).toList());
        } finally {
            recovered.close();
        }
    }

    @Test
    void walReplaySkipsCompactedOffsetHolesAfterInvalidCheckpoint(@TempDir Path dir) {
        LogMaintenanceConfig maintenance = LogMaintenanceConfig.of(
                RetentionPolicy.disabled(),
                CompactionPolicy.enabled(10),
                0);
        LogManagerConfig config = new LogManagerConfig(
                FlushPolicy.always(),
                0,
                new SegmentedLogConfig(1, 1),
                maintenance);
        LogManager manager = new LogManager(dir, config);
        manager.append("profiles", 0, "a".getBytes(), "old".getBytes());
        manager.append("profiles", 0, "a".getBytes(), "new".getBytes());
        manager.append("profiles", 0, "b".getBytes(), "active".getBytes());
        manager.maintain();
        manager.close();

        new CheckpointStore(dir).save(new RecoveryCheckpoint(0, Map.of("profiles-0", 999L), 1));

        LogManager recovered = assertDoesNotThrow(() -> new LogManager(dir, LogManagerConfig.defaults()));
        try {
            assertEquals(List.of(1L, 2L), recovered.getOrCreate("profiles", 0).read(0, 4_096)
                    .stream().map(Record::offset).toList());
        } finally {
            recovered.close();
        }
    }

    @Test
    void walReplaySkipsCompactedMiddleOffsetHolesAfterInvalidCheckpoint(@TempDir Path dir) {
        LogMaintenanceConfig maintenance = LogMaintenanceConfig.of(
                RetentionPolicy.disabled(),
                CompactionPolicy.enabled(10),
                0);
        LogManagerConfig config = new LogManagerConfig(
                FlushPolicy.always(),
                0,
                new SegmentedLogConfig(1, 1),
                maintenance);
        LogManager manager = new LogManager(dir, config);
        manager.append("profiles", 0, "c".getBytes(), "keep".getBytes());
        manager.append("profiles", 0, "a".getBytes(), "old".getBytes());
        manager.append("profiles", 0, "a".getBytes(), "active".getBytes());
        manager.maintain();
        manager.close();

        new CheckpointStore(dir).save(new RecoveryCheckpoint(0, Map.of("profiles-0", 999L), 1));

        LogManager recovered = assertDoesNotThrow(() -> new LogManager(dir, LogManagerConfig.defaults()));
        try {
            assertEquals(List.of(0L, 2L), recovered.getOrCreate("profiles", 0).read(0, 4_096)
                    .stream().map(Record::offset).toList());
        } finally {
            recovered.close();
        }
    }

    private static List<String> actionKeys(LogMaintenanceReport report) {
        return report.segmentReports().stream()
                .map(segment -> segment.partition() + ":" + segment.baseOffset() + ":" + segment.action())
                .sorted()
                .toList();
    }
}
