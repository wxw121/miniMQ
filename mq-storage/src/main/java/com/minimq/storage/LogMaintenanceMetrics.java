package com.minimq.storage;

/**
 * Cumulative maintenance counters exposed for operator observability.
 */
public record LogMaintenanceMetrics(
        long runs,
        long dryRuns,
        long deletedBytes,
        long deletedSegments,
        long compactedBytes,
        long compactedSegments,
        long skippedDueToReaders,
        long cleanupFailures,
        MaintenanceStatus lastRunStatus) {

    public LogMaintenanceMetrics {
        if (lastRunStatus == null) {
            throw new IllegalArgumentException("lastRunStatus is required");
        }
    }

    public static LogMaintenanceMetrics empty() {
        return new LogMaintenanceMetrics(0, 0, 0, 0, 0, 0, 0, 0, MaintenanceStatus.NEVER_RUN);
    }
}
