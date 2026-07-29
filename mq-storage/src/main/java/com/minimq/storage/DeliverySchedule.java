package com.minimq.storage;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.function.LongSupplier;

/**
 * In-memory scheduling index for records whose delivery time is in the future.
 * The partition log remains the source of record bytes; this index only stores
 * delivery timestamps and is safe to reconstruct from WAL/checkpoint state.
 */
public final class DeliverySchedule {
    private final LongSupplier clockMillis;
    private final Map<PartitionKey, Map<Long, Long>> entries = new HashMap<>();
    private final Map<PartitionKey, Long> visibleWatermarks = new HashMap<>();

    public DeliverySchedule() {
        this(System::currentTimeMillis);
    }

    public DeliverySchedule(LongSupplier clockMillis) {
        this.clockMillis = Objects.requireNonNull(clockMillis, "clockMillis");
    }

    public synchronized void put(String topic, int partitionId, long offset, long deliverAtEpochMs) {
        validate(topic, partitionId, offset, deliverAtEpochMs);
        if (deliverAtEpochMs <= clockMillis.getAsLong()) {
            remove(topic, partitionId, offset);
            return;
        }
        entries.computeIfAbsent(new PartitionKey(topic, partitionId), ignored -> new HashMap<>())
                .put(offset, deliverAtEpochMs);
    }

    public synchronized void restore(String topic, int partitionId, long offset, long deliverAtEpochMs) {
        validate(topic, partitionId, offset, deliverAtEpochMs);
        if (deliverAtEpochMs > clockMillis.getAsLong()) {
            if (offset < visibleWatermark(topic, partitionId)) {
                return;
            }
            entries.computeIfAbsent(new PartitionKey(topic, partitionId), ignored -> new HashMap<>())
                    .put(offset, deliverAtEpochMs);
        }
    }

    public synchronized void restoreVisibleWatermark(String topic, int partitionId, long visibleWatermark) {
        if (topic == null || topic.isBlank() || partitionId < 0 || visibleWatermark < 0) {
            throw new IllegalArgumentException("invalid visible delivery watermark");
        }
        if (visibleWatermark == 0) {
            return;
        }
        PartitionKey key = new PartitionKey(topic, partitionId);
        visibleWatermarks.merge(key, visibleWatermark, Math::max);
        Map<Long, Long> partition = entries.get(key);
        if (partition != null) {
            partition.keySet().removeIf(offset -> offset < visibleWatermark);
            if (partition.isEmpty()) {
                entries.remove(key);
            }
        }
    }

    public synchronized void remove(String topic, int partitionId, long offset) {
        PartitionKey key = new PartitionKey(topic, partitionId);
        Map<Long, Long> partition = entries.get(key);
        if (partition == null) {
            return;
        }
        partition.remove(offset);
        if (partition.isEmpty()) {
            entries.remove(key);
        }
    }

    public synchronized void removePartition(String topic, int partitionId) {
        entries.remove(new PartitionKey(topic, partitionId));
        visibleWatermarks.remove(new PartitionKey(topic, partitionId));
    }

    public synchronized void truncatePartition(String topic, int partitionId, long offset) {
        PartitionKey key = new PartitionKey(topic, partitionId);
        Map<Long, Long> partition = entries.get(key);
        if (partition == null) {
            long visibleWatermark = visibleWatermarks.getOrDefault(key, 0L);
            if (visibleWatermark > offset) {
                visibleWatermarks.put(key, offset);
            }
            return;
        }
        partition.keySet().removeIf(deliveryOffset -> deliveryOffset >= offset);
        if (partition.isEmpty()) {
            entries.remove(key);
        }
        long visibleWatermark = visibleWatermarks.getOrDefault(key, 0L);
        if (visibleWatermark > offset) {
            visibleWatermarks.put(key, offset);
        }
    }

    public synchronized void removeTopic(String topic) {
        entries.keySet().removeIf(key -> key.topic.equals(topic));
        visibleWatermarks.keySet().removeIf(key -> key.topic.equals(topic));
    }

    public synchronized OptionalLong deliveryAt(
            String topic, int partitionId, long offset) {
        Map<Long, Long> partition = entries.get(new PartitionKey(topic, partitionId));
        if (partition == null || !partition.containsKey(offset)) {
            return OptionalLong.empty();
        }
        return OptionalLong.of(partition.get(offset));
    }

    /** Returns the first offset blocked by a future record, or physicalHighWatermark. */
    public synchronized long visibleHighWatermark(
            String topic, int partitionId, long physicalHighWatermark) {
        return visibilityState(topic, partitionId, physicalHighWatermark).visibleHighWatermark();
    }

    public synchronized long nextVisibleAtEpochMs(
            String topic, int partitionId, long physicalHighWatermark) {
        return visibilityState(topic, partitionId, physicalHighWatermark).nextVisibleAtEpochMs();
    }

    synchronized VisibilityState visibilityState(
            String topic, int partitionId, long physicalHighWatermark) {
        return visibilityStateAt(
                topic, partitionId, physicalHighWatermark, clockMillis.getAsLong());
    }

    synchronized VisibilityUpdate pruneAndGetVisibility(
            String topic, int partitionId, long physicalHighWatermark) {
        return pruneAndGetVisibilityAt(
                topic, partitionId, physicalHighWatermark, clockMillis.getAsLong());
    }

    synchronized VisibilityUpdate pruneAndGetVisibilityAt(
            String topic, int partitionId, long physicalHighWatermark, long now) {
        int prunedCount = pruneDueAt(topic, partitionId, physicalHighWatermark, now);
        VisibilityState state = visibilityStateAt(
                topic, partitionId, physicalHighWatermark, now);
        return new VisibilityUpdate(
                state.visibleHighWatermark(), state.nextVisibleAtEpochMs(), prunedCount);
    }

    private VisibilityState visibilityStateAt(
            String topic, int partitionId, long physicalHighWatermark, long now) {
        if (physicalHighWatermark < 0) {
            throw new IllegalArgumentException("physicalHighWatermark must be non-negative");
        }
        Map<Long, Long> partition = entries.get(new PartitionKey(topic, partitionId));
        if (partition == null || partition.isEmpty()) {
            return new VisibilityState(physicalHighWatermark, -1L);
        }
        long floor = visibleWatermark(topic, partitionId);
        long visible = partition.entrySet().stream()
                .filter(entry -> entry.getKey() < physicalHighWatermark
                        && entry.getKey() >= floor
                        && entry.getValue() > now)
                .mapToLong(Map.Entry::getKey)
                .min()
                .orElse(physicalHighWatermark);
        long nextVisibleAtEpochMs = visible < physicalHighWatermark
                ? partition.get(visible)
                : -1L;
        return new VisibilityState(visible, nextVisibleAtEpochMs);
    }

    public synchronized boolean hasFutureDelivery(
            String topic, int partitionId, long physicalHighWatermark) {
        Map<Long, Long> partition = entries.get(new PartitionKey(topic, partitionId));
        if (partition == null || partition.isEmpty()) {
            return false;
        }
        long now = clockMillis.getAsLong();
        long floor = visibleWatermark(topic, partitionId);
        return partition.entrySet().stream()
                .anyMatch(entry -> entry.getKey() < physicalHighWatermark
                        && entry.getKey() >= floor
                        && entry.getValue() > now);
    }

    /** Returns the scheduled offsets that are still blocked by future delivery. */
    public synchronized List<Long> futureOffsets(
            String topic, int partitionId, long physicalHighWatermark) {
        if (physicalHighWatermark < 0) {
            throw new IllegalArgumentException("physicalHighWatermark must be non-negative");
        }
        Map<Long, Long> partition = entries.get(new PartitionKey(topic, partitionId));
        if (partition == null || partition.isEmpty()) {
            return List.of();
        }
        long now = clockMillis.getAsLong();
        long floor = visibleWatermark(topic, partitionId);
        return partition.entrySet().stream()
                .filter(entry -> entry.getKey() < physicalHighWatermark
                        && entry.getKey() >= floor
                        && entry.getValue() > now)
                .map(Map.Entry::getKey)
                .sorted()
                .toList();
    }

    /** Removes due entries before the supplied physical high watermark. */
    public synchronized int pruneDue(String topic, int partitionId, long physicalHighWatermark) {
        return pruneDueAt(
                topic, partitionId, physicalHighWatermark, clockMillis.getAsLong());
    }

    synchronized int pruneDueAt(
            String topic, int partitionId, long physicalHighWatermark, long now) {
        if (physicalHighWatermark < 0) {
            throw new IllegalArgumentException("physicalHighWatermark must be non-negative");
        }
        PartitionKey key = new PartitionKey(topic, partitionId);
        Map<Long, Long> partition = entries.get(new PartitionKey(topic, partitionId));
        if (partition == null) {
            return 0;
        }
        int removed = 0;
        for (Long offset : new ArrayList<>(partition.keySet())) {
            Long dueAt = partition.get(offset);
            if (offset < physicalHighWatermark && dueAt <= now) {
                partition.remove(offset);
                removed++;
            }
        }
        if (removed > 0) {
            advanceVisiblePrefix(topic, partitionId, physicalHighWatermark, now);
        } else if (partition.isEmpty()) {
            entries.remove(key);
        }
        return removed;
    }

    public synchronized int count() {
        return entries.values().stream().mapToInt(Map::size).sum();
    }

    public synchronized Map<String, Map<Long, Long>> snapshotByPartitionKey() {
        Map<String, Map<Long, Long>> snapshot = new HashMap<>();
        entries.forEach((partition, offsets) -> snapshot.put(
                RecoveryCheckpoint.partitionKey(partition.topic, partition.partitionId),
                new HashMap<>(offsets)));
        return snapshot;
    }

    public synchronized Map<String, Long> visibleWatermarkSnapshotByPartitionKey() {
        Map<String, Long> snapshot = new HashMap<>();
        visibleWatermarks.forEach((partition, visibleWatermark) -> {
            if (visibleWatermark > 0) {
                snapshot.put(
                        RecoveryCheckpoint.partitionKey(partition.topic, partition.partitionId),
                        visibleWatermark);
            }
        });
        return snapshot;
    }

    public synchronized List<ScheduledEntry> entries() {
        List<ScheduledEntry> result = new ArrayList<>();
        entries.forEach((partition, offsets) -> offsets.forEach((offset, dueAt) ->
                result.add(new ScheduledEntry(partition.topic, partition.partitionId, offset, dueAt))));
        result.sort(Comparator.comparing(ScheduledEntry::topic)
                .thenComparingInt(ScheduledEntry::partitionId)
                .thenComparingLong(ScheduledEntry::offset));
        return List.copyOf(result);
    }

    private static void validate(String topic, int partitionId, long offset, long deliverAtEpochMs) {
        if (topic == null || topic.isBlank()) {
            throw new IllegalArgumentException("topic is required");
        }
        if (partitionId < 0 || offset < 0 || deliverAtEpochMs < 0) {
            throw new IllegalArgumentException("partition, offset and delivery time must be non-negative");
        }
    }

    private long visibleWatermark(String topic, int partitionId) {
        return visibleWatermarks.getOrDefault(new PartitionKey(topic, partitionId), 0L);
    }

    private void markVisible(String topic, int partitionId, long visibleWatermark) {
        restoreVisibleWatermark(topic, partitionId, visibleWatermark);
    }

    private void advanceVisiblePrefix(
            String topic, int partitionId, long physicalHighWatermark, long now) {
        PartitionKey key = new PartitionKey(topic, partitionId);
        Map<Long, Long> partition = entries.get(key);
        long floor = visibleWatermark(topic, partitionId);
        long nextFutureOffset = physicalHighWatermark;
        if (partition != null) {
            nextFutureOffset = partition.entrySet().stream()
                    .filter(entry -> entry.getKey() < physicalHighWatermark
                            && entry.getKey() >= floor
                            && entry.getValue() > now)
                    .mapToLong(Map.Entry::getKey)
                    .min()
                    .orElse(physicalHighWatermark);
        }
        markVisible(topic, partitionId, nextFutureOffset);
    }

    private record PartitionKey(String topic, int partitionId) {
        private PartitionKey {
            Objects.requireNonNull(topic, "topic");
        }
    }

    public record ScheduledEntry(String topic, int partitionId, long offset, long deliverAtEpochMs) {
    }

    record VisibilityState(long visibleHighWatermark, long nextVisibleAtEpochMs) {
    }

    record VisibilityUpdate(
            long visibleHighWatermark,
            long nextVisibleAtEpochMs,
            int prunedCount) {
    }
}
