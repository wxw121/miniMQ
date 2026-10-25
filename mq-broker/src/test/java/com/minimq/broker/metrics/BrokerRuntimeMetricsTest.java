package com.minimq.broker.metrics;

import com.minimq.common.error.ErrorCode;
import com.minimq.protocol.ApiKeys;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class BrokerRuntimeMetricsTest {

    @Test
    void snapshotsRequestsResponsesLatencyAndThrottling() {
        BrokerRuntimeMetrics metrics = new BrokerRuntimeMetrics();

        metrics.recordRequest(ApiKeys.PRODUCE);
        metrics.recordRequest(ApiKeys.FETCH);
        metrics.recordResponse(ErrorCode.NONE);
        metrics.recordResponse(ErrorCode.THROTTLED);
        metrics.recordLatency(ApiKeys.PRODUCE, 7);
        metrics.recordLatency(ApiKeys.PRODUCE, 13);
        metrics.recordThrottled(ApiKeys.FETCH);

        BrokerRuntimeMetricsSnapshot snapshot = metrics.snapshot();

        assertEquals(1L, snapshot.requestsByApiKey().get(ApiKeys.PRODUCE));
        assertEquals(1L, snapshot.requestsByApiKey().get(ApiKeys.FETCH));
        assertEquals(1L, snapshot.responsesByErrorCode().get(ErrorCode.NONE));
        assertEquals(1L, snapshot.responsesByErrorCode().get(ErrorCode.THROTTLED));
        assertEquals(2L, snapshot.latencyCountsByApiKey().get(ApiKeys.PRODUCE));
        assertEquals(20L, snapshot.totalLatencyMsByApiKey().get(ApiKeys.PRODUCE));
        assertEquals(10.0, snapshot.averageLatencyMs(ApiKeys.PRODUCE));
        assertEquals(1L, snapshot.throttledByApiKey().get(ApiKeys.FETCH));
    }

    @Test
    void compatibilitySnapshotDefaultsExtendedMetricsAndHandlesMissingLatency() {
        BrokerRuntimeMetricsSnapshot snapshot = new BrokerRuntimeMetricsSnapshot(
                Map.of(),
                Map.of(),
                Map.of(ApiKeys.PRODUCE, 2L),
                Map.of(ApiKeys.PRODUCE, 7L),
                Map.of());

        assertEquals(3.5, snapshot.averageLatencyMs(ApiKeys.PRODUCE));
        assertEquals(0.0, snapshot.averageLatencyMs(ApiKeys.FETCH));
        assertEquals(0.0, snapshot.maxLatencyMillis(ApiKeys.PRODUCE));
        assertEquals(Map.of(), snapshot.totalLatencyNanosByApiKey());
        assertEquals(Map.of(), snapshot.maxLatencyNanosByApiKey());
        assertEquals(0L, snapshot.produceBytes());
        assertEquals(0L, snapshot.fetchBytes());
        assertEquals(0L, snapshot.startedAtEpochMs());
    }

    @Test
    void reportsMaximumLatencyInMilliseconds() {
        BrokerRuntimeMetricsSnapshot snapshot = new BrokerRuntimeMetricsSnapshot(
                Map.of(),
                Map.of(),
                Map.of(),
                Map.of(),
                Map.of(),
                Map.of(),
                Map.of(ApiKeys.FETCH, 2_500_000L),
                0L,
                0L,
                1L);

        assertEquals(2.5, snapshot.maxLatencyMillis(ApiKeys.FETCH));
    }
}
