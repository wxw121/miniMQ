package com.minimq.storage;

/**
 * Whole-segment retention limits. A zero limit means that dimension is disabled.
 */
public record RetentionPolicy(long retentionMs, long retentionBytes) {

    public RetentionPolicy {
        if (retentionMs < 0) {
            throw new IllegalArgumentException("retentionMs must be non-negative");
        }
        if (retentionBytes < 0) {
            throw new IllegalArgumentException("retentionBytes must be non-negative");
        }
    }

    public static RetentionPolicy disabled() {
        return new RetentionPolicy(0, 0);
    }

    public static RetentionPolicy of(long retentionMs, long retentionBytes) {
        return new RetentionPolicy(retentionMs, retentionBytes);
    }

    public boolean enabled() {
        return retentionMs > 0 || retentionBytes > 0;
    }
}
