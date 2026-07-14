package com.minimq.storage;

/**
 * Storage recovery and durability knobs for LogManager.
 */
public record LogManagerConfig(FlushPolicy flushPolicy, int checkpointEveryNAppends) {

    public LogManagerConfig {
        if (flushPolicy == null) {
            throw new IllegalArgumentException("flushPolicy is required");
        }
        if (checkpointEveryNAppends < 0) {
            throw new IllegalArgumentException("checkpointEveryNAppends must be non-negative");
        }
    }

    public static LogManagerConfig defaults() {
        return new LogManagerConfig(FlushPolicy.always(), 0);
    }

    boolean shouldCheckpointAfterAppend(long successfulAppends) {
        return checkpointEveryNAppends > 0 && successfulAppends % checkpointEveryNAppends == 0;
    }
}
