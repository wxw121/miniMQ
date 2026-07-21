package com.minimq.broker.metrics;

import com.minimq.common.error.ErrorCode;

import java.util.Map;

public record BrokerRuntimeMetricsSnapshot(
        Map<Short, Long> requestsByApiKey,
        Map<ErrorCode, Long> responsesByErrorCode,
        Map<Short, Long> latencyCountsByApiKey,
        Map<Short, Long> totalLatencyMsByApiKey,
        Map<Short, Long> throttledByApiKey) {

    public double averageLatencyMs(short apiKey) {
        long count = latencyCountsByApiKey.getOrDefault(apiKey, 0L);
        if (count == 0) {
            return 0.0;
        }
        return (double) totalLatencyMsByApiKey.getOrDefault(apiKey, 0L) / count;
    }
}
