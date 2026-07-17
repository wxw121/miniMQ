package com.minimq.storage;

/**
 * Storage recovery and durability knobs for LogManager.
 */
public record LogManagerConfig(
        FlushPolicy flushPolicy,
        int checkpointEveryNAppends,
        SegmentedLogConfig segmentedLogConfig,
        LogMaintenanceConfig maintenanceConfig) {

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

    public static LogManagerConfig defaults() {
        return new LogManagerConfig(
                FlushPolicy.always(),
                0,
                SegmentedLogConfig.defaults(),
                LogMaintenanceConfig.disabled());
    }

    boolean shouldCheckpointAfterAppend(long successfulAppends) {
        return checkpointEveryNAppends > 0 && successfulAppends % checkpointEveryNAppends == 0;
    }
}
