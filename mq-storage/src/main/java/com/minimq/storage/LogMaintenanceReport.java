package com.minimq.storage;

import java.util.List;

/**
 * Dry-run or executed maintenance report aggregated across opened partitions.
 */
public record LogMaintenanceReport(
        boolean dryRun,
        MaintenanceStatus status,
        List<MaintenanceSegmentReport> segmentReports,
        long skippedDueToReaders,
        long cleanupFailures) {

    public LogMaintenanceReport {
        if (status == null) {
            throw new IllegalArgumentException("status is required");
        }
        segmentReports = segmentReports == null ? List.of() : List.copyOf(segmentReports);
        if (skippedDueToReaders < 0) {
            throw new IllegalArgumentException("skippedDueToReaders must be non-negative");
        }
        if (cleanupFailures < 0) {
            throw new IllegalArgumentException("cleanupFailures must be non-negative");
        }
    }

    public static LogMaintenanceReport skipped(boolean dryRun, MaintenanceStatus status) {
        return new LogMaintenanceReport(dryRun, status, List.of(), 0, 0);
    }

    public long deletedBytes() {
        return segmentReports.stream()
                .filter(report -> report.action() == MaintenanceAction.RETENTION_DELETE)
                .mapToLong(MaintenanceSegmentReport::sizeBytes)
                .sum();
    }

    public long deletedSegments() {
        return segmentReports.stream()
                .filter(report -> report.action() == MaintenanceAction.RETENTION_DELETE)
                .count();
    }

    public long compactedBytes() {
        return segmentReports.stream()
                .filter(LogMaintenanceReport::isCompactionAction)
                .mapToLong(MaintenanceSegmentReport::sizeBytes)
                .sum();
    }

    public long compactedSegments() {
        return segmentReports.stream()
                .filter(LogMaintenanceReport::isCompactionAction)
                .count();
    }

    private static boolean isCompactionAction(MaintenanceSegmentReport report) {
        return report.action() == MaintenanceAction.COMPACTION_REWRITE
                || report.action() == MaintenanceAction.COMPACTION_DELETE;
    }
}
