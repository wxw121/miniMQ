package com.minimq.broker.metrics;

import com.minimq.common.error.ErrorCode;

import java.util.Map;

public record BrokerRuntimeMetricsSnapshot(
        Map<Short, Long> requestsByApiKey,
        Map<ErrorCode, Long> responsesByErrorCode,
        Map<Short, Long> latencyCountsByApiKey,
        Map<Short, Long> totalLatencyMsByApiKey,
        Map<Short, Long> throttledByApiKey,
        Map<Short, Long> totalLatencyNanosByApiKey,
        Map<Short, Long> maxLatencyNanosByApiKey,
        long produceBytes,
        long fetchBytes,
        long startedAtEpochMs) {

    public BrokerRuntimeMetricsSnapshot(
            Map<Short, Long> requestsByApiKey,
            Map<ErrorCode, Long> responsesByErrorCode,
            Map<Short, Long> latencyCountsByApiKey,
            Map<Short, Long> totalLatencyMsByApiKey,
            Map<Short, Long> throttledByApiKey) {
        this(
                requestsByApiKey,
                responsesByErrorCode,
                latencyCountsByApiKey,
                totalLatencyMsByApiKey,
                throttledByApiKey,
                Map.of(),
                Map.of(),
                0L,
                0L,
                0L);
    }

    public double averageLatencyMs(short apiKey) {
        long count = latencyCountsByApiKey.getOrDefault(apiKey, 0L);
        if (count == 0) {
            return 0.0;
        }
        return (double) totalLatencyMsByApiKey.getOrDefault(apiKey, 0L) / count;
    }

    public double maxLatencyMillis(short apiKey) {
        return maxLatencyNanosByApiKey.getOrDefault(apiKey, 0L) / 1_000_000.0;
    }
}
