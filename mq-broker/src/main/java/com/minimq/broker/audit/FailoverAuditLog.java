package com.minimq.broker.audit;

import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Objects;
import java.util.function.LongSupplier;

/** Best-effort, process-local bounded audit history. */
public final class FailoverAuditLog {
    public static final int DEFAULT_CAPACITY = 4096;
    public static final int MAX_DETAIL_BYTES = 1024;

    private final int capacity;
    private final LongSupplier clockMillis;
    private final int brokerId;
    private final long startedAtEpochMs;
    private final Deque<AuditEvent> events = new ArrayDeque<>();
    private long nextSequence = 1L;
    private long overwrittenCount;

    public FailoverAuditLog(int brokerId, LongSupplier clockMillis) {
        this(brokerId, clockMillis, DEFAULT_CAPACITY, clockMillis.getAsLong());
    }

    public FailoverAuditLog(int brokerId, LongSupplier clockMillis, int capacity) {
        this(brokerId, clockMillis, capacity, clockMillis.getAsLong());
    }

    /** Uses a caller-owned process start instant while retaining a live event clock. */
    public FailoverAuditLog(int brokerId, LongSupplier clockMillis, long startedAtEpochMs) {
        this(brokerId, clockMillis, DEFAULT_CAPACITY, startedAtEpochMs);
    }

    private FailoverAuditLog(
            int brokerId,
            LongSupplier clockMillis,
            int capacity,
            long startedAtEpochMs) {
        if (capacity < 1) {
            throw new IllegalArgumentException("capacity must be positive");
        }
        this.capacity = capacity;
        this.clockMillis = Objects.requireNonNull(clockMillis, "clockMillis");
        this.brokerId = brokerId;
        this.startedAtEpochMs = startedAtEpochMs;
    }

    public synchronized AuditEvent append(
            FailoverAuditEventType eventType,
            Integer eventBrokerId,
            String topicName,
            Integer partitionId,
            String oldValue,
            String newValue,
            String detail) {
        if (eventType == null) {
            throw new IllegalArgumentException("eventType is required");
        }
        AuditEvent event = new AuditEvent(
                nextSequence++,
                clockMillis.getAsLong(),
                eventType,
                eventBrokerId,
                topicName,
                partitionId,
                oldValue == null ? "" : oldValue,
                newValue == null ? "" : newValue,
                truncateUtf8(detail == null ? "" : detail, MAX_DETAIL_BYTES));
        if (events.size() == capacity) {
            events.removeFirst();
            overwrittenCount++;
        }
        events.addLast(event);
        return event;
    }

    public AuditEvent append(FailoverAuditEventType eventType, String detail) {
        return append(eventType, null, null, null, "", "", detail);
    }

    public synchronized List<AuditEvent> newestFirst(int limit) {
        if (limit < 1 || limit > capacity) {
            throw new IllegalArgumentException("limit must be in 1.." + capacity);
        }
        List<AuditEvent> result = new ArrayList<>(Math.min(limit, events.size()));
        var iterator = events.descendingIterator();
        while (iterator.hasNext() && result.size() < limit) {
            result.add(iterator.next());
        }
        return List.copyOf(result);
    }

    public synchronized List<AuditEvent> snapshot() {
        return List.copyOf(events);
    }

    /** Number of entries currently retained in the bounded ring. */
    public synchronized int size() {
        return events.size();
    }

    public int capacity() {
        return capacity;
    }

    public synchronized long overwrittenCount() {
        return overwrittenCount;
    }

    public int brokerId() {
        return brokerId;
    }

    public long startedAtEpochMs() {
        return startedAtEpochMs;
    }

    static String truncateUtf8(String value, int maxBytes) {
        byte[] encoded = value.getBytes(StandardCharsets.UTF_8);
        if (encoded.length <= maxBytes) {
            return value;
        }
        StringBuilder result = new StringBuilder();
        int bytes = 0;
        for (int index = 0; index < value.length();) {
            int codePoint = value.codePointAt(index);
            int codePointBytes = new String(Character.toChars(codePoint))
                    .getBytes(StandardCharsets.UTF_8).length;
            if (bytes + codePointBytes > maxBytes) {
                break;
            }
            result.appendCodePoint(codePoint);
            bytes += codePointBytes;
            index += Character.charCount(codePoint);
        }
        return result.toString();
    }

    public record AuditEvent(
            long sequence,
            long timestampEpochMs,
            FailoverAuditEventType eventType,
            Integer brokerId,
            String topicName,
            Integer partitionId,
            String oldValue,
            String newValue,
            String detail) {
    }
}
