package com.minimq.storage;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DeliveryScheduleTest {

    @Test
    void futureRecordBlocksTheEntireVisibleSuffixUntilItIsDue() {
        AtomicLong now = new AtomicLong(1_000L);
        DeliverySchedule schedule = new DeliverySchedule(now::get);

        schedule.put("orders", 0, 2L, 2_000L);
        schedule.put("orders", 0, 4L, 3_000L);

        assertEquals(2L, schedule.visibleHighWatermark("orders", 0, 5L));
        assertEquals(2_000L, schedule.nextVisibleAtEpochMs("orders", 0, 5L));

        now.set(2_000L);
        assertEquals(4L, schedule.visibleHighWatermark("orders", 0, 5L));
        assertEquals(3_000L, schedule.nextVisibleAtEpochMs("orders", 0, 5L));
        assertEquals(1, schedule.pruneDue("orders", 0, 5L));

        now.set(3_000L);
        assertEquals(5L, schedule.visibleHighWatermark("orders", 0, 5L));
        assertEquals(-1L, schedule.nextVisibleAtEpochMs("orders", 0, 5L));
        assertEquals(1, schedule.pruneDue("orders", 0, 5L));
        assertEquals(0, schedule.count());
    }

    @Test
    void pruningLaterDueRecordDoesNotExposeEarlierFutureBlocker() {
        AtomicLong now = new AtomicLong(1_000L);
        DeliverySchedule schedule = new DeliverySchedule(now::get);

        schedule.put("orders", 0, 2L, 3_000L);
        schedule.put("orders", 0, 4L, 2_000L);

        now.set(2_000L);
        assertEquals(2L, schedule.visibleHighWatermark("orders", 0, 5L));
        assertEquals(3_000L, schedule.nextVisibleAtEpochMs("orders", 0, 5L));
        assertEquals(1, schedule.pruneDue("orders", 0, 5L));
        assertEquals(2L, schedule.visibleHighWatermark("orders", 0, 5L));
        assertEquals(3_000L, schedule.nextVisibleAtEpochMs("orders", 0, 5L));
        assertTrue(schedule.deliveryAt("orders", 0, 2L).isPresent());

        now.set(3_000L);
        assertEquals(1, schedule.pruneDue("orders", 0, 5L));
        assertEquals(5L, schedule.visibleHighWatermark("orders", 0, 5L));
        assertEquals(0, schedule.count());
    }

    @Test
    void restoreKeepsOnlyStillFutureRecordsAndTopicRemovalIsExact() {
        AtomicLong now = new AtomicLong(100L);
        DeliverySchedule schedule = new DeliverySchedule(now::get);

        schedule.restore("a-b", 0, 1L, 101L);
        schedule.restore("a", 0, 1L, 101L);
        schedule.restore("a", 0, 2L, 100L);

        assertEquals(2, schedule.count());
        assertTrue(schedule.deliveryAt("a-b", 0, 1L).isPresent());
        assertTrue(schedule.deliveryAt("a", 0, 1L).isPresent());
        assertFalse(schedule.deliveryAt("a", 0, 2L).isPresent());
        schedule.removeTopic("a");
        assertEquals(1, schedule.count());
        assertTrue(schedule.deliveryAt("a-b", 0, 1L).isPresent());
        assertFalse(schedule.deliveryAt("a", 0, 1L).isPresent());
    }

    @Test
    void restoreSkipsRecordsBeforeVisibleWatermark() {
        AtomicLong now = new AtomicLong(100L);
        DeliverySchedule schedule = new DeliverySchedule(now::get);

        schedule.restore("orders", 0, 1L, 101L);
        schedule.restoreVisibleWatermark("orders", 0, 2L);
        schedule.restore("orders", 0, 1L, 101L);
        schedule.restore("orders", 0, 2L, 101L);
        schedule.restore("orders", 0, 3L, 100L);

        assertEquals(1, schedule.count());
        assertFalse(schedule.deliveryAt("orders", 0, 1L).isPresent());
        assertTrue(schedule.deliveryAt("orders", 0, 2L).isPresent());
        assertFalse(schedule.deliveryAt("orders", 0, 3L).isPresent());
    }
}
