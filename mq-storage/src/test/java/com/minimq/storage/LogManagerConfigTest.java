package com.minimq.storage;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LogManagerConfigTest {

    @Test
    void validatesMaintenanceConfiguration() {
        assertFalse(LogMaintenanceConfig.disabled().enabled());

        LogMaintenanceConfig config = LogMaintenanceConfig.of(
                RetentionPolicy.of(1_000, 10_000),
                CompactionPolicy.enabled(3),
                5);

        assertTrue(config.enabled());
        assertTrue(config.shouldRunAfterAppend(10));
        assertFalse(config.shouldRunAfterAppend(11));
        assertThrows(IllegalArgumentException.class, () -> RetentionPolicy.of(-1, 0));
        assertThrows(IllegalArgumentException.class, () -> CompactionPolicy.enabled(0));
        assertThrows(IllegalArgumentException.class,
                () -> LogMaintenanceConfig.of(RetentionPolicy.disabled(), CompactionPolicy.disabled(), -1));
    }

    @Test
    void rejectsMissingMaintenanceConfiguration() {
        assertThrows(IllegalArgumentException.class,
                () -> new LogManagerConfig(FlushPolicy.always(), 0, SegmentedLogConfig.defaults(), null));
    }
}
