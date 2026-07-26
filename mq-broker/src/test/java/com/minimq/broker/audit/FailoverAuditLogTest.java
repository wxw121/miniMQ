package com.minimq.broker.audit;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class FailoverAuditLogTest {
    @Test
    void evictsExactlyOldestAndReturnsNewestFirst() {
        AtomicLong now = new AtomicLong(100L);
        FailoverAuditLog log = new FailoverAuditLog(7, now::get, 2);

        log.append(FailoverAuditEventType.LEADER_CHANGED, "first");
        now.incrementAndGet();
        log.append(FailoverAuditEventType.ISR_SHRUNK, "second");
        now.incrementAndGet();
        log.append(FailoverAuditEventType.ISR_EXPANDED, "third");

        assertEquals(1L, log.overwrittenCount());
        assertEquals(2, log.size());
        assertEquals(List.of("third", "second"),
                log.newestFirst(2).stream().map(FailoverAuditLog.AuditEvent::detail).toList());
        assertEquals(List.of(3L, 2L),
                log.newestFirst(2).stream().map(FailoverAuditLog.AuditEvent::sequence).toList());
        assertEquals(7, log.brokerId());
        assertEquals(100L, log.startedAtEpochMs());
    }

    @Test
    void detailIsCappedWithoutSplittingUtf8CodePoints() {
        String value = "x".repeat(1_023) + "\u4E2D";
        FailoverAuditLog log = new FailoverAuditLog(1, () -> 1L);

        FailoverAuditLog.AuditEvent event = log.append(
                FailoverAuditEventType.LEADER_NOT_READY, value);

        assertEquals(1_023, event.detail().getBytes(StandardCharsets.UTF_8).length);
        assertEquals("x".repeat(1_023), event.detail());
    }

    @Test
    void rejectsInvalidCapacityAndLimits() {
        assertThrows(IllegalArgumentException.class,
                () -> new FailoverAuditLog(1, () -> 1L, 0));
        FailoverAuditLog log = new FailoverAuditLog(1, () -> 1L, 2);
        assertThrows(IllegalArgumentException.class, () -> log.newestFirst(0));
        assertThrows(IllegalArgumentException.class, () -> log.newestFirst(3));
    }
}
