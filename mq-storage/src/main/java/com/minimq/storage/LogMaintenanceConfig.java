package com.minimq.storage;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;

/**
 * Opt-in maintenance lifecycle for retention and compaction.
 */
public record LogMaintenanceConfig(
        RetentionPolicy retentionPolicy,
        CompactionPolicy compactionPolicy,
        int maintenanceEveryNAppends,
        int maintenanceWindowStartHour,
        int maintenanceWindowEndHour) {

    private static final int WINDOW_DISABLED = -1;

    public LogMaintenanceConfig {
        if (retentionPolicy == null) {
            throw new IllegalArgumentException("retentionPolicy is required");
        }
        if (compactionPolicy == null) {
            throw new IllegalArgumentException("compactionPolicy is required");
        }
        if (maintenanceEveryNAppends < 0) {
            throw new IllegalArgumentException("maintenanceEveryNAppends must be non-negative");
        }
        if (!windowDisabled(maintenanceWindowStartHour, maintenanceWindowEndHour)) {
            validateHour(maintenanceWindowStartHour, "maintenanceWindowStartHour");
            validateHour(maintenanceWindowEndHour, "maintenanceWindowEndHour");
        }
    }

    public static LogMaintenanceConfig disabled() {
        return new LogMaintenanceConfig(
                RetentionPolicy.disabled(),
                CompactionPolicy.disabled(),
                0,
                WINDOW_DISABLED,
                WINDOW_DISABLED);
    }

    public static LogMaintenanceConfig of(
            RetentionPolicy retentionPolicy,
            CompactionPolicy compactionPolicy,
            int maintenanceEveryNAppends) {
        return new LogMaintenanceConfig(
                retentionPolicy,
                compactionPolicy,
                maintenanceEveryNAppends,
                WINDOW_DISABLED,
                WINDOW_DISABLED);
    }

    public static LogMaintenanceConfig of(
            RetentionPolicy retentionPolicy,
            CompactionPolicy compactionPolicy,
            int maintenanceEveryNAppends,
            int maintenanceWindowStartHour,
            int maintenanceWindowEndHour) {
        return new LogMaintenanceConfig(
                retentionPolicy,
                compactionPolicy,
                maintenanceEveryNAppends,
                maintenanceWindowStartHour,
                maintenanceWindowEndHour);
    }

    public boolean enabled() {
        return retentionPolicy.enabled() || compactionPolicy.enabled();
    }

    public boolean shouldRunAfterAppend(long successfulAppends) {
        return shouldRunAfterAppend(successfulAppends, Clock.systemUTC());
    }

    public boolean shouldRunAfterAppend(long successfulAppends, Clock clock) {
        return cadenceDue(successfulAppends) && withinWindow(clock);
    }

    public boolean shouldSkipAfterAppendForWindow(long successfulAppends, Clock clock) {
        return cadenceDue(successfulAppends) && !withinWindow(clock);
    }

    private boolean cadenceDue(long successfulAppends) {
        return enabled() && maintenanceEveryNAppends > 0
                && successfulAppends % maintenanceEveryNAppends == 0;
    }

    private boolean withinWindow(Clock clock) {
        if (windowDisabled(maintenanceWindowStartHour, maintenanceWindowEndHour)) {
            return true;
        }
        int hour = Instant.ofEpochMilli(clock.millis()).atZone(ZoneOffset.UTC).getHour();
        if (maintenanceWindowStartHour == maintenanceWindowEndHour) {
            return true;
        }
        if (maintenanceWindowStartHour < maintenanceWindowEndHour) {
            return hour >= maintenanceWindowStartHour && hour < maintenanceWindowEndHour;
        }
        return hour >= maintenanceWindowStartHour || hour < maintenanceWindowEndHour;
    }

    private static boolean windowDisabled(int startHour, int endHour) {
        return startHour == WINDOW_DISABLED && endHour == WINDOW_DISABLED;
    }

    private static void validateHour(int hour, String name) {
        if (hour < 0 || hour > 23) {
            throw new IllegalArgumentException(name + " must be between 0 and 23");
        }
    }
}
