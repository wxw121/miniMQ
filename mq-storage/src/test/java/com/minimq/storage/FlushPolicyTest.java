package com.minimq.storage;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FlushPolicyTest {

    @Test
    void alwaysFlushesAfterEveryAppend() {
        assertFalse(FlushPolicy.always().shouldFlush(0, 100, 100));
        assertTrue(FlushPolicy.always().shouldFlush(1, 100, 100));
    }

    @Test
    void everyNAppendsFlushesOnlyOnIntervalBoundary() {
        FlushPolicy policy = FlushPolicy.everyNAppends(3);

        assertFalse(policy.shouldFlush(1, 100, 0));
        assertFalse(policy.shouldFlush(2, 100, 0));
        assertTrue(policy.shouldFlush(3, 100, 0));
        assertFalse(policy.shouldFlush(4, 100, 0));
        assertTrue(policy.shouldFlush(6, 100, 0));
    }

    @Test
    void intervalFlushesOnlyAfterConfiguredElapsedTime() {
        FlushPolicy policy = FlushPolicy.intervalMs(100);

        assertFalse(policy.shouldFlush(1, 149, 50));
        assertTrue(policy.shouldFlush(1, 150, 50));
    }

    @Test
    void manualPolicyDoesNotFlushDuringAppend() {
        assertFalse(FlushPolicy.manualOnClose().shouldFlush(1, 1_000, 0));
    }

    @Test
    void rejectsInvalidPolicySettings() {
        assertThrows(IllegalArgumentException.class, () -> new FlushPolicy(null, 0, 0));
        assertThrows(IllegalArgumentException.class, () -> FlushPolicy.everyNAppends(0));
        assertThrows(IllegalArgumentException.class, () -> FlushPolicy.intervalMs(0));
        assertThrows(IllegalArgumentException.class,
                () -> new LogManagerConfig(FlushPolicy.always(), -1));
        assertThrows(IllegalArgumentException.class,
                () -> new LogManagerConfig(null, 0));
        assertEquals(FlushPolicy.everyNAppends(100), LogManagerConfig.defaults().flushPolicy());
        assertEquals(LogManagerConfig.defaultFlushPolicy(), LogManagerConfig.defaults().flushPolicy());
        assertEquals(0, LogManagerConfig.defaults().checkpointEveryNAppends());
    }
}
