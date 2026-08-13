package com.minimq.storage;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
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

    @Test
    void puttingAnAlreadyDueRecordRemovesItsScheduledEntry() {
        AtomicLong now = new AtomicLong(100L);
        DeliverySchedule schedule = new DeliverySchedule(now::get);

        schedule.put("orders", 0, 1L, 200L);
        schedule.put("orders", 0, 2L, 300L);
        schedule.put("orders", 0, 1L, 100L);

        assertEquals(1, schedule.count());
        assertFalse(schedule.deliveryAt("orders", 0, 1L).isPresent());
        assertTrue(schedule.deliveryAt("orders", 0, 2L).isPresent());
        schedule.remove("orders", 0, 99L);
        schedule.remove("orders", 0, 2L);
        schedule.remove("orders", 0, 2L);
        assertEquals(0, schedule.count());
        assertEquals(0, new DeliverySchedule().count());
    }

    @Test
    void restoredWatermarksAreMonotonicAndPruneOlderEntries() {
        AtomicLong now = new AtomicLong(100L);
        DeliverySchedule schedule = new DeliverySchedule(now::get);
        schedule.restore("orders", 0, 1L, 200L);
        schedule.restore("orders", 0, 2L, 200L);
        schedule.restore("orders", 0, 3L, 200L);

        schedule.restoreVisibleWatermark("orders", 0, 2L);
        schedule.restoreVisibleWatermark("orders", 0, 1L);
        schedule.restore("orders", 0, 1L, 200L);

        assertEquals(2, schedule.count());
        assertEquals(Map.of("orders-0", 2L),
                schedule.visibleWatermarkSnapshotByPartitionKey());

        schedule.restoreVisibleWatermark("orders", 0, 4L);
        assertEquals(0, schedule.count());
        assertEquals(Map.of("orders-0", 4L),
                schedule.visibleWatermarkSnapshotByPartitionKey());
        schedule.restoreVisibleWatermark("orders", 0, 0L);
        schedule.removePartition("orders", 0);
        assertTrue(schedule.visibleWatermarkSnapshotByPartitionKey().isEmpty());

        assertThrows(IllegalArgumentException.class,
                () -> schedule.restoreVisibleWatermark(null, 0, 0L));
        assertThrows(IllegalArgumentException.class,
                () -> schedule.restoreVisibleWatermark("orders", -1, 0L));
        assertThrows(IllegalArgumentException.class,
                () -> schedule.restoreVisibleWatermark("orders", 0, -1L));
    }

    @Test
    void truncateClampsVisibleStateAndFutureQueriesRemainBounded() {
        AtomicLong now = new AtomicLong(100L);
        DeliverySchedule schedule = new DeliverySchedule(now::get);
        schedule.restoreVisibleWatermark("orders", 0, 5L);

        schedule.truncatePartition("orders", 0, 3L);
        assertEquals(Map.of("orders-0", 3L),
                schedule.visibleWatermarkSnapshotByPartitionKey());

        schedule.put("orders", 0, 3L, 300L);
        schedule.put("orders", 0, 5L, 200L);
        assertTrue(schedule.hasFutureDelivery("orders", 0, 6L));
        assertEquals(List.of(3L, 5L), schedule.futureOffsets("orders", 0, 6L));

        schedule.truncatePartition("orders", 0, 5L);
        assertEquals(List.of(3L), schedule.futureOffsets("orders", 0, 6L));
        schedule.truncatePartition("orders", 0, 2L);
        assertEquals(0, schedule.count());
        assertFalse(schedule.hasFutureDelivery("orders", 0, 6L));
        assertTrue(schedule.futureOffsets("orders", 0, 6L).isEmpty());
        assertEquals(Map.of("orders-0", 2L),
                schedule.visibleWatermarkSnapshotByPartitionKey());

        assertEquals(2L, schedule.visibleHighWatermark("orders", 0, 6L));

        assertThrows(IllegalArgumentException.class,
                () -> schedule.visibleHighWatermark("orders", 0, -1L));
        assertThrows(IllegalArgumentException.class,
                () -> schedule.futureOffsets("orders", 0, -1L));
        assertThrows(IllegalArgumentException.class,
                () -> schedule.pruneDue("orders", 0, -1L));
        assertEquals(0, schedule.pruneDue("missing", 0, 1L));
    }

    @Test
    void snapshotsAreSortedAndDoNotExposeMutableScheduleState() {
        AtomicLong now = new AtomicLong(100L);
        DeliverySchedule schedule = new DeliverySchedule(now::get);
        schedule.put("b", 1, 5L, 500L);
        schedule.put("a", 2, 3L, 400L);
        schedule.put("a", 0, 7L, 600L);

        assertEquals(
                List.of(
                        new DeliverySchedule.ScheduledEntry("a", 0, 7L, 600L),
                        new DeliverySchedule.ScheduledEntry("a", 2, 3L, 400L),
                        new DeliverySchedule.ScheduledEntry("b", 1, 5L, 500L)),
                schedule.entries());
        Map<String, Map<Long, Long>> snapshot = schedule.snapshotByPartitionKey();
        snapshot.get("a-0").clear();
        assertTrue(schedule.deliveryAt("a", 0, 7L).isPresent());
    }

    @Test
    void rejectsInvalidScheduleCoordinates() {
        DeliverySchedule schedule = new DeliverySchedule(() -> 0L);

        assertThrows(NullPointerException.class, () -> new DeliverySchedule(null));
        assertThrows(IllegalArgumentException.class,
                () -> schedule.put(null, 0, 0L, 0L));
        assertThrows(IllegalArgumentException.class,
                () -> schedule.put(" ", 0, 0L, 0L));
        assertThrows(IllegalArgumentException.class,
                () -> schedule.put("orders", -1, 0L, 0L));
        assertThrows(IllegalArgumentException.class,
                () -> schedule.put("orders", 0, -1L, 0L));
        assertThrows(IllegalArgumentException.class,
                () -> schedule.put("orders", 0, 0L, -1L));
    }

    @Test
    void emptyScheduleKeepsStoredVisibleWatermarkBelowPhysicalHw() {
        AtomicLong now = new AtomicLong(100L);
        DeliverySchedule schedule = new DeliverySchedule(now::get);

        assertEquals(5L, schedule.visibleHighWatermark("orders", 0, 5L));

        schedule.restoreVisibleWatermark("orders", 0, 4L);
        assertEquals(4L, schedule.visibleHighWatermark("orders", 0, 10L));
        assertEquals(-1L, schedule.nextVisibleAtEpochMs("orders", 0, 10L));

        schedule.noteReadyOffset("orders", 0, 4L);
        schedule.noteReadyOffset("orders", 0, 5L);
        assertEquals(6L, schedule.visibleHighWatermark("orders", 0, 6L));
        assertEquals(Map.of("orders-0", 6L),
                schedule.visibleWatermarkSnapshotByPartitionKey());
    }
}
