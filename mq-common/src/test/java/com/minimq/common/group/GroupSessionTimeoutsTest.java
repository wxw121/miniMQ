package com.minimq.common.group;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class GroupSessionTimeoutsTest {

    @Test
    void defaultsAndHeartbeatInterval() {
        assertEquals(GroupSessionTimeouts.DEFAULT_MS, GroupSessionTimeouts.effective(0));
        assertEquals(30_000, GroupSessionTimeouts.effective(30_000));
        assertEquals(3_333, GroupSessionTimeouts.heartbeatIntervalMs(10_000));
        assertEquals(100, GroupSessionTimeouts.heartbeatIntervalMs(100));
        GroupSessionTimeouts.requireValid(GroupSessionTimeouts.MIN_MS);
        GroupSessionTimeouts.requireValid(GroupSessionTimeouts.MAX_MS);
        assertThrows(IllegalArgumentException.class, () -> GroupSessionTimeouts.requireValid(999));
        assertThrows(IllegalArgumentException.class, () -> GroupSessionTimeouts.requireValid(300_001));
    }
}
