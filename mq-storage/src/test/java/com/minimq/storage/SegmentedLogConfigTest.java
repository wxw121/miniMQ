package com.minimq.storage;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SegmentedLogConfigTest {

    @Test
    void twoArgumentLogManagerConfigUsesSegmentDefaults() {
        LogManagerConfig config = new LogManagerConfig(FlushPolicy.always(), 0);

        assertEquals(64L * 1024 * 1024, config.segmentedLogConfig().segmentMaxBytes());
        assertEquals(64, config.segmentedLogConfig().sparseIndexIntervalRecords());
    }

    @Test
    void rejectsInvalidSegmentSettings() {
        assertThrows(IllegalArgumentException.class, () -> new SegmentedLogConfig(0, 64));
        assertThrows(IllegalArgumentException.class, () -> new SegmentedLogConfig(1024, 0));
    }
}
