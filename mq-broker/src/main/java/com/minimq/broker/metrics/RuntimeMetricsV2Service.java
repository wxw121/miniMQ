package com.minimq.broker.metrics;

import com.minimq.protocol.message.ProtocolMessages.PartitionLagInfo;
import com.minimq.protocol.message.ProtocolMessages.RuntimeErrorCount;
import com.minimq.protocol.message.ProtocolMessages.RuntimeLatencyV2Summary;
import com.minimq.protocol.message.ProtocolMessages.RuntimeMetricCount;
import com.minimq.protocol.message.ProtocolMessages.RuntimeMetricsV2Response;
import com.minimq.protocol.message.ProtocolMessages.V4BoundedList;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.LongSupplier;

/** Builds bounded, process-level metrics snapshots without exposing mutable counters. */
public final class RuntimeMetricsV2Service {
    private final BrokerRuntimeMetrics metrics;
    private final LongSupplier clockMillis;
    private final LongSupplier auditOverwrittenCount;

    public RuntimeMetricsV2Service(
            BrokerRuntimeMetrics metrics,
            LongSupplier clockMillis,
            LongSupplier auditOverwrittenCount) {
        this.metrics = metrics;
        this.clockMillis = clockMillis;
        this.auditOverwrittenCount = auditOverwrittenCount;
    }

    public RuntimeMetricsV2Response snapshot(
            int counterLimit,
            int latencyLimit,
            int lagLimit,
            List<PartitionLagInfo> lagMetrics) {
        validateLimit(counterLimit, "counterLimit");
        validateLimit(latencyLimit, "latencyLimit");
        validateLimit(lagLimit, "lagLimit");
        BrokerRuntimeMetricsSnapshot snapshot = metrics.snapshot();
        long now = clockMillis.getAsLong();
        long uptime = Math.max(0L, now - snapshot.startedAtEpochMs());

        List<RuntimeMetricCount> requests = snapshot.requestsByApiKey().entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .map(entry -> new RuntimeMetricCount(entry.getKey(), entry.getValue()))
                .toList();
        List<RuntimeErrorCount> responses = snapshot.responsesByErrorCode().entrySet().stream()
                .sorted(Comparator.comparingInt(entry -> entry.getKey().code))
                .map(entry -> new RuntimeErrorCount(entry.getKey().code, entry.getValue()))
                .toList();
        List<RuntimeLatencyV2Summary> latencies = snapshot.latencyCountsByApiKey().entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .map(entry -> new RuntimeLatencyV2Summary(
                        entry.getKey(),
                        entry.getValue(),
                        snapshot.totalLatencyNanosByApiKey().getOrDefault(entry.getKey(), 0L),
                        snapshot.maxLatencyMillis(entry.getKey())))
                .toList();
        List<RuntimeMetricCount> throttled = snapshot.throttledByApiKey().entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .map(entry -> new RuntimeMetricCount(entry.getKey(), entry.getValue()))
                .toList();
        List<PartitionLagInfo> sortedLag = lagMetrics == null
                ? List.of()
                : lagMetrics.stream()
                        .sorted(Comparator.comparing(PartitionLagInfo::topicName)
                                .thenComparingInt(PartitionLagInfo::partitionId))
                        .toList();

        return new RuntimeMetricsV2Response(
                snapshot.startedAtEpochMs(),
                now,
                uptime,
                bounded(requests, counterLimit),
                bounded(responses, counterLimit),
                bounded(latencies, latencyLimit),
                bounded(throttled, counterLimit),
                snapshot.produceBytes(),
                snapshot.fetchBytes(),
                auditOverwrittenCount.getAsLong(),
                bounded(sortedLag, lagLimit));
    }

    private static <T> V4BoundedList<T> bounded(List<T> values, int limit) {
        int total = values.size();
        List<T> items = values.subList(0, Math.min(total, limit));
        return new V4BoundedList<>(total, total > items.size(), items);
    }

    private static void validateLimit(int value, String name) {
        if (value < 1 || value > 1_000) {
            throw new IllegalArgumentException(name + " must be in 1..1000");
        }
    }

}
