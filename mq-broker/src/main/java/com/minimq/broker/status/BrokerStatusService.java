package com.minimq.broker.status;

import com.minimq.broker.BrokerAvailabilityGuard;
import com.minimq.broker.BrokerConfig;
import com.minimq.broker.ControllerRole;
import com.minimq.broker.metadata.ClusterMetadata;
import com.minimq.broker.metadata.ClusterMetadata.BrokerEntry;
import com.minimq.broker.metadata.ClusterMetadata.PartitionEntry;
import com.minimq.protocol.message.ProtocolMessages.OpsBrokerInfo;
import com.minimq.protocol.message.ProtocolMessages.OpsPartitionInfo;
import com.minimq.protocol.message.ProtocolMessages.OpsStatusResponse;
import com.minimq.protocol.message.ProtocolMessages.V4BoundedList;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.function.LongSupplier;

public final class BrokerStatusService {
    private final BrokerConfig config;
    private final com.minimq.broker.controller.ClusterController controller;
    private final BrokerAvailabilityGuard availabilityGuard;
    private final ControllerRole controllerRole;
    private final LongSupplier clockMillis;
    private final long startedAtEpochMs;

    public BrokerStatusService(
            BrokerConfig config,
            com.minimq.broker.controller.ClusterController controller,
            BrokerAvailabilityGuard availabilityGuard,
            LongSupplier clockMillis) {
        this(
                config,
                controller,
                availabilityGuard,
                ControllerRole.legacy(config),
                clockMillis,
                clockMillis.getAsLong());
    }

    public BrokerStatusService(
            BrokerConfig config,
            com.minimq.broker.controller.ClusterController controller,
            BrokerAvailabilityGuard availabilityGuard,
            ControllerRole controllerRole,
            LongSupplier clockMillis,
            long startedAtEpochMs) {
        this.config = config;
        this.controller = controller;
        this.availabilityGuard = availabilityGuard;
        this.controllerRole = controllerRole;
        this.clockMillis = clockMillis;
        this.startedAtEpochMs = startedAtEpochMs;
    }

    public OpsStatusResponse status(int brokerLimit, int partitionLimit) {
        int safeBrokerLimit = validateLimit(brokerLimit, "brokerLimit");
        int safePartitionLimit = validateLimit(partitionLimit, "partitionLimit");
        long snapshotAtEpochMs = clockMillis.getAsLong();
        ClusterMetadata snapshot = controller.snapshot();
        BrokerEntry localBroker = snapshot.brokers.get(config.brokerId());

        return new OpsStatusResponse(
                config.brokerId(),
                startedAtEpochMs,
                snapshotAtEpochMs,
                snapshot.metadataVersion,
                snapshot.metadataFormatVersion,
                controllerRole.isActive() ? "CONTROLLER" : "BROKER",
                localBroker == null ? -1L : localBroker.brokerEpoch,
                snapshot.controllerEpoch,
                config.networkTransportMode().name(),
                availabilityGuard.state().name(),
                bounded(brokers(snapshot), safeBrokerLimit),
                bounded(partitions(snapshot), safePartitionLimit));
    }

    private static List<OpsBrokerInfo> brokers(ClusterMetadata snapshot) {
        return snapshot.brokers.values().stream()
                .sorted(Comparator.comparingInt(broker -> broker.brokerId))
                .map(broker -> new OpsBrokerInfo(
                        broker.brokerId,
                        broker.host,
                        broker.port,
                        broker.online ? "ONLINE" : "OFFLINE",
                        broker.brokerEpoch,
                        -1L))
                .toList();
    }

    private static List<OpsPartitionInfo> partitions(ClusterMetadata snapshot) {
        List<OpsPartitionInfo> partitions = new ArrayList<>();
        snapshot.topics.values().stream()
                .sorted(Comparator.comparing(topic -> topic.topicName))
                .forEach(topic -> topic.partitions.stream()
                        .sorted(Comparator.comparingInt(partition -> partition.partitionId))
                        .map(partition -> partitionInfo(snapshot, topic.topicName, partition))
                        .forEach(partitions::add));
        return partitions;
    }

    static OpsPartitionInfo partitionInfo(
            ClusterMetadata snapshot, String topicName, PartitionEntry partition) {
        ClusterMetadata.WatermarkSnapshot watermark = currentWatermark(snapshot, partition);
        return new OpsPartitionInfo(
                topicName,
                partition.partitionId,
                partition.leaderBrokerId,
                partition.leaderEpoch,
                watermark.physicalHighWatermark,
                watermark.visibleHighWatermark,
                watermark.nextVisibleAtEpochMs,
                readiness(snapshot, partition),
                partition.frozen,
                fullIntList(partition.replicaBrokerIds),
                fullIntList(partition.isrBrokerIds));
    }

    static ClusterMetadata.WatermarkSnapshot currentWatermark(
            ClusterMetadata snapshot, PartitionEntry partition) {
        ClusterMetadata.WatermarkSnapshot watermark = partition.lastCompleteWatermark;
        if (watermark == null
                || watermark.physicalHighWatermark < 0
                || watermark.visibleHighWatermark < 0) {
            return new ClusterMetadata.WatermarkSnapshot();
        }
        if (!partition.frozen
                && (watermark.controllerEpoch != snapshot.controllerEpoch
                || watermark.leaderEpoch != partition.leaderEpoch)) {
            return new ClusterMetadata.WatermarkSnapshot();
        }
        return watermark;
    }

    static String readiness(ClusterMetadata snapshot, PartitionEntry partition) {
        if (partition.frozen) {
            return "FROZEN";
        }
        ClusterMetadata.WatermarkSnapshot watermark = currentWatermark(snapshot, partition);
        if (!partition.leaderReady || watermark.physicalHighWatermark < 0 || watermark.visibleHighWatermark < 0) {
            return "WATERMARK_UNKNOWN";
        }
        return "READY";
    }

    static V4BoundedList<Integer> fullIntList(List<Integer> values) {
        List<Integer> items = values == null
                ? List.of()
                : values.stream().sorted().toList();
        return new V4BoundedList<>(items.size(), false, items);
    }

    static <T> V4BoundedList<T> bounded(List<T> sortedItems, int limit) {
        int total = sortedItems.size();
        List<T> items = sortedItems.subList(0, Math.min(limit, total));
        return new V4BoundedList<>(total, total > items.size(), items);
    }

    private static int validateLimit(int limit, String name) {
        if (limit < 1 || limit > 1_000) {
            throw new IllegalArgumentException(name + " must be in 1..1000");
        }
        return limit;
    }
}
