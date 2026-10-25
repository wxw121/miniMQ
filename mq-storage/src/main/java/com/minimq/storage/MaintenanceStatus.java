package com.minimq.storage;

/**
 * Last-known outcome for a maintenance attempt.
 */
public enum MaintenanceStatus {
    NEVER_RUN,
    SUCCESS,
    NOOP,
    SKIPPED_DISABLED,
    SKIPPED_WINDOW,
    SKIPPED_READERS,
    FAILED
}
