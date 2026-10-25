package com.minimq.storage;

/**
 * Storage recovery and durability knobs for LogManager.
 */
public record LogManagerConfig(
        FlushPolicy flushPolicy,
        int checkpointEveryNAppends,
        SegmentedLogConfig segmentedLogConfig,
        LogMaintenanceConfig maintenanceConfig,
        int maxScheduledRecords) {
    public static final int DEFAULT_MAX_SCHEDULED_RECORDS = 100_000;
    public static final int DEFAULT_FLUSH_EVERY_N_APPENDS = 100;

    public LogManagerConfig {
        if (flushPolicy == null) {
            throw new IllegalArgumentException("flushPolicy is required");
        }
        if (checkpointEveryNAppends < 0) {
            throw new IllegalArgumentException("checkpointEveryNAppends must be non-negative");
        }
        if (segmentedLogConfig == null) {
            throw new IllegalArgumentException("segmentedLogConfig is required");
        }
        if (maintenanceConfig == null) {
            throw new IllegalArgumentException("maintenanceConfig is required");
        }
        if (maxScheduledRecords < 0) {
            throw new IllegalArgumentException("maxScheduledRecords must be non-negative");
        }
    }

    public LogManagerConfig(
            FlushPolicy flushPolicy,
            int checkpointEveryNAppends,
            SegmentedLogConfig segmentedLogConfig,
            LogMaintenanceConfig maintenanceConfig) {
        this(flushPolicy, checkpointEveryNAppends, segmentedLogConfig, maintenanceConfig,
                DEFAULT_MAX_SCHEDULED_RECORDS);
    }

    public LogManagerConfig(
            FlushPolicy flushPolicy,
            int checkpointEveryNAppends,
            SegmentedLogConfig segmentedLogConfig) {
        this(flushPolicy, checkpointEveryNAppends, segmentedLogConfig, LogMaintenanceConfig.disabled());
    }

    public LogManagerConfig(FlushPolicy flushPolicy, int checkpointEveryNAppends) {
        this(flushPolicy, checkpointEveryNAppends, SegmentedLogConfig.defaults(), LogMaintenanceConfig.disabled());
    }

    public static FlushPolicy defaultFlushPolicy() {
        return FlushPolicy.everyNAppends(DEFAULT_FLUSH_EVERY_N_APPENDS);
    }

    public static LogManagerConfig defaults() {
        return new LogManagerConfig(
                defaultFlushPolicy(),
                0,
                SegmentedLogConfig.defaults(),
                LogMaintenanceConfig.disabled(),
                DEFAULT_MAX_SCHEDULED_RECORDS);
    }

    boolean shouldCheckpointAfterAppend(long successfulAppends) {
        return checkpointEveryNAppends > 0 && successfulAppends % checkpointEveryNAppends == 0;
    }
}
