package com.minimq.storage;

/**
 * Maintenance action represented in dry-run and execution reports.
 */
public enum MaintenanceAction {
    RETENTION_DELETE,
    COMPACTION_REWRITE,
    COMPACTION_DELETE
}
