package com.minimq.common.acks;

/**
 * Topic-level minimum ISR size required before {@code acks=ALL} waits.
 */
public final class MinInSyncReplicas {
    public static final int DEFAULT = 1;
    public static final int MAX = 10_000;

    private MinInSyncReplicas() {
    }

    public static int effective(int minInSyncReplicas) {
        return minInSyncReplicas <= 0 ? DEFAULT : minInSyncReplicas;
    }

    public static boolean isValid(int minInSyncReplicas, int replicationFactor) {
        return minInSyncReplicas >= DEFAULT
                && minInSyncReplicas <= replicationFactor
                && minInSyncReplicas <= MAX;
    }

    public static void requireRange(int minInSyncReplicas) {
        if (minInSyncReplicas < DEFAULT || minInSyncReplicas > MAX) {
            throw new IllegalArgumentException(
                    "minInSyncReplicas must be in " + DEFAULT + ".." + MAX + ": " + minInSyncReplicas);
        }
    }

    public static void requireValid(int minInSyncReplicas, int replicationFactor) {
        requireRange(minInSyncReplicas);
        if (minInSyncReplicas > replicationFactor) {
            throw new IllegalArgumentException(
                    "minInSyncReplicas must be <= replicationFactor: "
                            + minInSyncReplicas + " > " + replicationFactor);
        }
    }
}
