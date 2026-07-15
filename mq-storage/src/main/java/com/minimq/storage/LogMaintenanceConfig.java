package com.minimq.storage;

/**
 * Opt-in maintenance lifecycle for retention and compaction.
 */
public record LogMaintenanceConfig(
        RetentionPolicy retentionPolicy,
        CompactionPolicy compactionPolicy,
        int maintenanceEveryNAppends) {

    public LogMaintenanceConfig {
        if (retentionPolicy == null) {
            throw new IllegalArgumentException("retentionPolicy is required");
        }
        if (compactionPolicy == null) {
            throw new IllegalArgumentException("compactionPolicy is required");
        }
        if (maintenanceEveryNAppends < 0) {
            throw new IllegalArgumentException("maintenanceEveryNAppends must be non-negative");
        }
    }

    public static LogMaintenanceConfig disabled() {
        return new LogMaintenanceConfig(RetentionPolicy.disabled(), CompactionPolicy.disabled(), 0);
    }

    public static LogMaintenanceConfig of(
            RetentionPolicy retentionPolicy,
            CompactionPolicy compactionPolicy,
            int maintenanceEveryNAppends) {
        return new LogMaintenanceConfig(retentionPolicy, compactionPolicy, maintenanceEveryNAppends);
    }

    public boolean enabled() {
        return retentionPolicy.enabled() || compactionPolicy.enabled();
    }

    public boolean shouldRunAfterAppend(long successfulAppends) {
        return enabled() && maintenanceEveryNAppends > 0
                && successfulAppends % maintenanceEveryNAppends == 0;
    }
}
