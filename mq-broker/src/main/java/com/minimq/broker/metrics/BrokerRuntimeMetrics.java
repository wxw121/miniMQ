package com.minimq.broker.metrics;

import com.minimq.common.error.ErrorCode;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;

public final class BrokerRuntimeMetrics {
    private final ConcurrentHashMap<Short, LongAdder> requestsByApiKey = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<ErrorCode, LongAdder> responsesByErrorCode = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Short, LongAdder> latencyCountsByApiKey = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Short, LongAdder> totalLatencyMsByApiKey = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Short, LongAdder> throttledByApiKey = new ConcurrentHashMap<>();

    public void recordRequest(short apiKey) {
        increment(requestsByApiKey, apiKey);
    }

    public void recordResponse(ErrorCode errorCode) {
        increment(responsesByErrorCode, errorCode);
    }

    public void recordLatency(short apiKey, long latencyMs) {
        increment(latencyCountsByApiKey, apiKey);
        totalLatencyMsByApiKey.computeIfAbsent(apiKey, ignored -> new LongAdder()).add(Math.max(0L, latencyMs));
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
                snapshot(throttledByApiKey));
    }

    private static <K> void increment(ConcurrentHashMap<K, LongAdder> counters, K key) {
        counters.computeIfAbsent(key, ignored -> new LongAdder()).increment();
    }

    private static <K> Map<K, Long> snapshot(ConcurrentHashMap<K, LongAdder> counters) {
        return counters.entrySet().stream()
                .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, entry -> entry.getValue().sum()));
    }
}
