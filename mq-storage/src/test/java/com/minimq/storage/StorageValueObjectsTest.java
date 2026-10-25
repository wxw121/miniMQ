package com.minimq.storage;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StorageValueObjectsTest {

    @Test
    void maintenanceReportsValidateAndAggregateActions() {
        List<MaintenanceSegmentReport> segments = new ArrayList<>(List.of(
                segment(MaintenanceAction.RETENTION_DELETE, 10L),
                segment(MaintenanceAction.COMPACTION_REWRITE, 20L),
                segment(MaintenanceAction.COMPACTION_DELETE, 30L)));

        LogMaintenanceReport report = new LogMaintenanceReport(
                false, MaintenanceStatus.SUCCESS, segments, 1L, 2L);
        segments.clear();

        assertEquals(10L, report.deletedBytes());
        assertEquals(1L, report.deletedSegments());
        assertEquals(50L, report.compactedBytes());
        assertEquals(2L, report.compactedSegments());
        assertEquals(3, report.segmentReports().size());
        assertTrue(new LogMaintenanceReport(
                true, MaintenanceStatus.NOOP, null, 0L, 0L).segmentReports().isEmpty());
        assertEquals(MaintenanceStatus.SKIPPED_DISABLED,
                LogMaintenanceReport.skipped(true, MaintenanceStatus.SKIPPED_DISABLED).status());

        assertThrows(IllegalArgumentException.class,
                () -> new LogMaintenanceReport(false, null, List.of(), 0L, 0L));
        assertThrows(IllegalArgumentException.class,
                () -> new LogMaintenanceReport(
                        false, MaintenanceStatus.SUCCESS, List.of(), -1L, 0L));
        assertThrows(IllegalArgumentException.class,
                () -> new LogMaintenanceReport(
                        false, MaintenanceStatus.SUCCESS, List.of(), 0L, -1L));
        assertThrows(IllegalArgumentException.class,
                () -> new MaintenanceSegmentReport(
                        " ", MaintenanceAction.RETENTION_DELETE, 0L, 0L, 0L, 0L));
        assertThrows(IllegalArgumentException.class,
                () -> new MaintenanceSegmentReport("orders-0", null, 0L, 0L, 0L, 0L));
        assertThrows(IllegalArgumentException.class,
                () -> new MaintenanceSegmentReport(
                        "orders-0", MaintenanceAction.RETENTION_DELETE, -1L, 0L, 0L, 0L));
        assertThrows(IllegalArgumentException.class,
                () -> new MaintenanceSegmentReport(
                        "orders-0", MaintenanceAction.RETENTION_DELETE, 0L, 0L, 0L, -1L));
        assertEquals(MaintenanceStatus.NEVER_RUN, LogMaintenanceMetrics.empty().lastRunStatus());
        assertThrows(IllegalArgumentException.class,
                () -> new LogMaintenanceMetrics(0, 0, 0, 0, 0, 0, 0, 0, null));
    }

    @Test
    void storageConfigurationRejectsMissingAndNegativeValues() {
        assertThrows(IllegalArgumentException.class,
                () -> new LogManagerConfig(
                        FlushPolicy.always(), 0, null, LogMaintenanceConfig.disabled()));
        assertThrows(IllegalArgumentException.class,
                () -> new LogManagerConfig(
                        FlushPolicy.always(), 0, SegmentedLogConfig.defaults(),
                        LogMaintenanceConfig.disabled(), -1));
        assertThrows(IllegalArgumentException.class,
                () -> new LogMaintenanceConfig(
                        null, CompactionPolicy.disabled(), 0, -1, -1));
        assertThrows(IllegalArgumentException.class,
                () -> new LogMaintenanceConfig(
                        RetentionPolicy.disabled(), null, 0, -1, -1));
        assertThrows(IllegalArgumentException.class, () -> RetentionPolicy.of(0L, -1L));
        assertThrows(IllegalArgumentException.class,
                () -> new CompactionPolicy(false, -1, 0L));
    }

    @Test
    void segmentCatalogRejectsEveryInvalidStructuralBoundary() {
        SegmentDescriptor first = new SegmentDescriptor(0L, 1L);

        assertThrows(IllegalArgumentException.class,
                () -> new SegmentCatalog(0L, -1L, 0L, List.of(first)));
        assertThrows(IllegalArgumentException.class,
                () -> new SegmentCatalog(0L, 0L, -1L, List.of(first)));
        assertThrows(IllegalArgumentException.class,
                () -> new SegmentCatalog(0L, 0L, 0L, null));
        assertThrows(IllegalArgumentException.class,
                () -> new SegmentCatalog(0L, 0L, 0L, List.of()));
        assertThrows(IllegalArgumentException.class,
                () -> new SegmentCatalog(0L, 0L, 0L, java.util.Arrays.asList((SegmentDescriptor) null)));
    }

    @Test
    void walFactoriesPreserveImmediateAndScheduledBoundaries() {
        WalRecord immediate = WalRecord.dataAppend("orders", 0, 1L, null, null, 0L);
        WalRecord scheduled = WalRecord.dataAppend("orders", 0, 2L, null, null, 3L);
        WalRecord legacyConstructor =
                new WalRecord(WalRecordType.DATA_APPEND, "orders", 0, 4L, null, null);

        assertEquals(WalRecordType.DATA_APPEND, immediate.type());
        assertEquals(WalRecordType.SCHEDULED_DATA_APPEND, scheduled.type());
        assertEquals(3L, scheduled.deliverAtEpochMs());
        assertEquals(0L, legacyConstructor.deliverAtEpochMs());
        assertThrows(IllegalArgumentException.class,
                () -> new WalRecord(
                        WalRecordType.DATA_APPEND, "orders", 0, 0L, null, null, -1L));
        assertThrows(IllegalArgumentException.class,
                () -> WalRecord.scheduledDataAppend("orders", 0, 0L, null, null, 0L));
    }

    private static MaintenanceSegmentReport segment(MaintenanceAction action, long sizeBytes) {
        return new MaintenanceSegmentReport("orders-0", action, 0L, 0L, 0L, sizeBytes);
    }
}
