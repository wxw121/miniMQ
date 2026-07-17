package com.minimq.storage;

import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;

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

    @Test
    void evaluatesNormalOvernightAndAllDayUtcWindows() {
        LogMaintenanceConfig normal = window(9, 17);
        assertTrue(normal.shouldRunAfterAppend(1, utc("2026-07-17T09:00:00Z")));
        assertFalse(normal.shouldRunAfterAppend(1, utc("2026-07-17T17:00:00Z")));

        LogMaintenanceConfig overnight = window(22, 2);
        assertTrue(overnight.shouldRunAfterAppend(1, utc("2026-07-17T23:00:00Z")));
        assertTrue(overnight.shouldRunAfterAppend(1, utc("2026-07-18T01:00:00Z")));
        assertFalse(overnight.shouldRunAfterAppend(1, utc("2026-07-17T12:00:00Z")));

        LogMaintenanceConfig allDay = window(6, 6);
        assertTrue(allDay.shouldRunAfterAppend(1, utc("2026-07-17T00:00:00Z")));
        assertTrue(allDay.shouldRunAfterAppend(1, utc("2026-07-17T23:00:00Z")));
    }

    @Test
    void rejectsPartialWindowsAndNegativeCompactionByteBudget() {
        assertThrows(IllegalArgumentException.class, () -> LogMaintenanceConfig.of(
                RetentionPolicy.disabled(),
                CompactionPolicy.enabled(1),
                1,
                -1,
                17));
        assertThrows(IllegalArgumentException.class, () -> LogMaintenanceConfig.of(
                RetentionPolicy.disabled(),
                CompactionPolicy.enabled(1),
                1,
                9,
                -1));
        assertThrows(IllegalArgumentException.class, () -> CompactionPolicy.enabled(1, -1));
    }

    private static LogMaintenanceConfig window(int startHour, int endHour) {
        return LogMaintenanceConfig.of(
                RetentionPolicy.disabled(),
                CompactionPolicy.enabled(1),
                1,
                startHour,
                endHour);
    }

    private static Clock utc(String instant) {
        return Clock.fixed(Instant.parse(instant), ZoneOffset.UTC);
    }
}
