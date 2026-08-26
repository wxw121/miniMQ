package com.minimq.common.acks;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ProduceAcksTest {

    @Test
    void parsesSupportedCodes() {
        assertEquals(ProduceAcks.ALL, ProduceAcks.fromCode((short) -1));
        assertEquals(ProduceAcks.LEADER, ProduceAcks.fromCode((short) 1));
        assertEquals((short) -1, ProduceAcks.ALL.code());
        assertEquals((short) 1, ProduceAcks.LEADER.code());
    }

    @Test
    void rejectsUnsupportedCodeAndTimeout() {
        assertThrows(IllegalArgumentException.class, () -> ProduceAcks.fromCode((short) 0));
        assertThrows(IllegalArgumentException.class, () -> ProduceAcks.requireValid((short) 2));
        assertThrows(IllegalArgumentException.class, () -> ProduceAcks.requireTimeoutMs(0));
        assertThrows(IllegalArgumentException.class, () -> ProduceAcks.requireTimeoutMs(60_001));
        ProduceAcks.requireTimeoutMs(1);
        ProduceAcks.requireTimeoutMs(ProduceAcks.MAX_TIMEOUT_MS);
    }
}
