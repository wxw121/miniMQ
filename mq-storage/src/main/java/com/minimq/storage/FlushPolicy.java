package com.minimq.storage;

/**
 * Controls when WAL and partition-log data are forced to disk.
 */
public record FlushPolicy(Mode mode, int everyNAppends, long intervalMs) {

    public FlushPolicy {
        if (mode == null) {
            throw new IllegalArgumentException("flush policy mode is required");
        }
        if (mode == Mode.EVERY_N_APPENDS && everyNAppends <= 0) {
            throw new IllegalArgumentException("flush everyNAppends must be positive");
        }
        if (mode == Mode.INTERVAL_MS && intervalMs <= 0) {
            throw new IllegalArgumentException("flush intervalMs must be positive");
        }
    }

    public static FlushPolicy always() {
        return new FlushPolicy(Mode.ALWAYS, 1, 0);
    }

    public static FlushPolicy everyNAppends(int everyNAppends) {
        return new FlushPolicy(Mode.EVERY_N_APPENDS, everyNAppends, 0);
    }

    public static FlushPolicy intervalMs(long intervalMs) {
        return new FlushPolicy(Mode.INTERVAL_MS, 0, intervalMs);
    }

    public static FlushPolicy manualOnClose() {
        return new FlushPolicy(Mode.MANUAL_ON_CLOSE, 0, 0);
    }

    public boolean shouldFlush(long successfulAppends, long nowEpochMs, long lastFlushEpochMs) {
        if (successfulAppends <= 0) {
            return false;
        }
        return switch (mode) {
            case ALWAYS -> true;
            case EVERY_N_APPENDS -> successfulAppends % everyNAppends == 0;
            case INTERVAL_MS -> nowEpochMs - lastFlushEpochMs >= intervalMs;
            case MANUAL_ON_CLOSE -> false;
        };
    }

    public enum Mode {
        ALWAYS,
        EVERY_N_APPENDS,
        INTERVAL_MS,
        MANUAL_ON_CLOSE
    }
}
