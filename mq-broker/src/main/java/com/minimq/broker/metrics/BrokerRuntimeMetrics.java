package com.minimq.broker.metrics;

import com.minimq.common.error.ErrorCode;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

public final class BrokerRuntimeMetrics {
    private final ConcurrentHashMap<Short, LongAdder> requestsByApiKey = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<ErrorCode, LongAdder> responsesByErrorCode = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Short, LongAdder> latencyCountsByApiKey = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Short, LongAdder> totalLatencyMsByApiKey = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Short, LongAdder> totalLatencyNanosByApiKey = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Short, AtomicLong> maxLatencyNanosByApiKey = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Short, LongAdder> throttledByApiKey = new ConcurrentHashMap<>();
    private final LongAdder produceBytes = new LongAdder();
    private final LongAdder fetchBytes = new LongAdder();
    private final long startedAtEpochMs;

    public BrokerRuntimeMetrics() {
        this(System::currentTimeMillis);
    }

    public BrokerRuntimeMetrics(LongSupplier clockMillis) {
        this.startedAtEpochMs = clockMillis.getAsLong();
    }

    public void recordRequest(short apiKey) {
        increment(requestsByApiKey, apiKey);
    }

    public void recordResponse(ErrorCode errorCode) {
        increment(responsesByErrorCode, errorCode);
    }

    public void recordLatency(short apiKey, long latencyMs) {
        long nonNegativeMillis = Math.max(0L, latencyMs);
        long nanos = nonNegativeMillis > Long.MAX_VALUE / 1_000_000L
                ? Long.MAX_VALUE
                : nonNegativeMillis * 1_000_000L;
        recordLatencyNanos(apiKey, nanos);
    }

    public void recordLatencyNanos(short apiKey, long latencyNanos) {
        latencyNanos = Math.max(0L, latencyNanos);
        increment(latencyCountsByApiKey, apiKey);
        totalLatencyMsByApiKey.computeIfAbsent(apiKey, ignored -> new LongAdder())
                .add(TimeUnit.NANOSECONDS.toMillis(latencyNanos));
        totalLatencyNanosByApiKey.computeIfAbsent(apiKey, ignored -> new LongAdder()).add(latencyNanos);
        maxLatencyNanosByApiKey.computeIfAbsent(apiKey, ignored -> new AtomicLong())
                .accumulateAndGet(latencyNanos, Math::max);
    }

    public void recordProduceBytes(long bytes) {
        if (bytes < 0) {
            throw new IllegalArgumentException("bytes must be non-negative");
        }
        produceBytes.add(bytes);
    }

    public void recordFetchBytes(long bytes) {
        if (bytes < 0) {
            throw new IllegalArgumentException("bytes must be non-negative");
        }
        fetchBytes.add(bytes);
    }

    public void recordThrottled(short apiKey) {
        increment(throttledByApiKey, apiKey);
    }

    public BrokerRuntimeMetricsSnapshot snapshot() {
        return new BrokerRuntimeMetricsSnapshot(
                snapshot(requestsByApiKey),
                snapshot(responsesByErrorCode),
                snapshot(latencyCountsByApiKey),
                snapshot(totalLatencyMsByApiKey),
                snapshot(throttledByApiKey),
                snapshot(totalLatencyNanosByApiKey),
                maxLatencyNanosByApiKey.entrySet().stream()
                        .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, entry -> entry.getValue().get())),
                produceBytes.sum(),
                fetchBytes.sum(),
                startedAtEpochMs);
    }

    private static <K> void increment(ConcurrentHashMap<K, LongAdder> counters, K key) {
        counters.computeIfAbsent(key, ignored -> new LongAdder()).increment();
    }

    private static <K> Map<K, Long> snapshot(ConcurrentHashMap<K, LongAdder> counters) {
        return counters.entrySet().stream()
                .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, entry -> entry.getValue().sum()));
    }
}
