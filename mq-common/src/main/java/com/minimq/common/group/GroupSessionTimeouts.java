package com.minimq.common.group;

/**
 * Per-member consumer-group session timeout bounds used by JOIN_GROUP and eviction.
 */
public final class GroupSessionTimeouts {
    public static final int DEFAULT_MS = 10_000;
    public static final int MIN_MS = 1_000;
    public static final int MAX_MS = 300_000;

    private GroupSessionTimeouts() {
    }

    public static int effective(int sessionTimeoutMs) {
        return sessionTimeoutMs <= 0 ? DEFAULT_MS : sessionTimeoutMs;
    }

    public static int heartbeatIntervalMs(int sessionTimeoutMs) {
        return Math.max(100, effective(sessionTimeoutMs) / 3);
    }

    public static void requireValid(int sessionTimeoutMs) {
        if (sessionTimeoutMs < MIN_MS || sessionTimeoutMs > MAX_MS) {
            throw new IllegalArgumentException(
                    "sessionTimeoutMs must be in " + MIN_MS + ".." + MAX_MS + ": " + sessionTimeoutMs);
        }
    }
}
