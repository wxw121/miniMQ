package com.minimq.broker.status;

import com.minimq.broker.controller.ClusterController;
import com.minimq.broker.metadata.ClusterMetadata;
import com.minimq.broker.metadata.ClusterMetadata.ConsumerGroupEntry;
import com.minimq.broker.metadata.ClusterMetadata.MemberEntry;
import com.minimq.broker.metadata.ClusterMetadata.PartitionEntry;
import com.minimq.common.error.ErrorCode;
import com.minimq.protocol.message.ProtocolMessages.GroupAssignmentInfo;
import com.minimq.protocol.message.ProtocolMessages.GroupMemberInfo;
import com.minimq.protocol.message.ProtocolMessages.GroupStatusResponse;
import com.minimq.protocol.message.ProtocolMessages.PartitionLagInfo;
import com.minimq.protocol.message.ProtocolMessages.V4BoundedList;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.LongSupplier;

public final class GroupStatusService {
    private final ClusterController controller;
    private final LongSupplier clockMillis;

    public record Result(ErrorCode errorCode, GroupStatusResponse response) {
    }

    public GroupStatusService(ClusterController controller, LongSupplier clockMillis) {
        this.controller = controller;
        this.clockMillis = clockMillis;
    }

    public Result status(String groupId, String topicFilter, int memberLimit, int partitionLimit) {
        int safeMemberLimit = validateLimit(memberLimit, "memberLimit");
        int safePartitionLimit = validateLimit(partitionLimit, "partitionLimit");
        ClusterMetadata snapshot = controller.snapshot();
        ConsumerGroupEntry group = snapshot.consumerGroups.get(groupId);
        if (group == null) {
            return new Result(ErrorCode.GROUP_NOT_FOUND, null);
        }
        String topicName = group.topicName == null ? "" : group.topicName;
        if (topicFilter != null && !topicFilter.isBlank()) {
            if (!snapshot.topics.containsKey(topicFilter)) {
                return new Result(ErrorCode.UNKNOWN_TOPIC, null);
            }
            if (!topicFilter.equals(topicName)) {
                return new Result(ErrorCode.INVALID_REQUEST, null);
            }
        }
        ClusterMetadata.TopicEntry topic = snapshot.topics.get(topicName);
        if (topic == null) {
            return new Result(ErrorCode.UNKNOWN_TOPIC, null);
        }

        List<GroupMemberInfo> members = group.members.values().stream()
                .sorted(Comparator.comparing(member -> member.memberId))
                .map(member -> memberInfo(topicName, member, safePartitionLimit))
                .toList();
        List<PartitionLagInfo> partitionLags = topic.partitions.stream()
                .sorted(Comparator.comparingInt(partition -> partition.partitionId))
                .map(partition -> lagInfo(snapshot, group, topicName, partition))
                .toList();

        return new Result(
                ErrorCode.NONE,
                new GroupStatusResponse(
                        group.groupId,
                        topicName,
                        group.generationId,
                        BrokerStatusService.bounded(members, safeMemberLimit),
                        BrokerStatusService.bounded(partitionLags, safePartitionLimit)));
    }

    /**
     * Builds the runtime-metrics lag family from one controller snapshot. Since
     * the V4 wire item is partition-scoped and does not carry group id, multiple
     * groups on the same topic-partition are folded to the most severe row:
     * unknown/frozen states win first, then the largest known physical/visible
     * lag, with deterministic topic/partition ordering for the final response.
     */
    public List<PartitionLagInfo> lagMetricsSnapshot() {
        ClusterMetadata snapshot = controller.snapshot();
        Map<String, PartitionLagInfo> byPartition = new HashMap<>();
        snapshot.consumerGroups.values().stream()
                .sorted(Comparator.comparing(group -> group.groupId))
                .forEach(group -> {
                    String topicName = group.topicName == null ? "" : group.topicName;
                    ClusterMetadata.TopicEntry topic = snapshot.topics.get(topicName);
                    if (topic == null) {
                        return;
                    }
                    topic.partitions.stream()
                            .sorted(Comparator.comparingInt(partition -> partition.partitionId))
                            .map(partition -> lagInfo(snapshot, group, topicName, partition))
                            .forEach(lag -> byPartition.merge(
                                    lag.topicName() + "\u0000" + lag.partitionId(),
                                    lag,
                                    GroupStatusService::moreSevereLag));
                });
        return byPartition.values().stream()
                .sorted(Comparator.comparing(PartitionLagInfo::topicName)
                        .thenComparingInt(PartitionLagInfo::partitionId))
                .toList();
    }

    private GroupMemberInfo memberInfo(String topicName, MemberEntry member, int partitionLimit) {
        List<GroupAssignmentInfo> assignments = member.assignedPartitions.stream()
                .sorted()
                .map(partitionId -> new GroupAssignmentInfo(topicName, partitionId))
                .toList();
        long now = clockMillis.getAsLong();
        return new GroupMemberInfo(
                member.memberId,
                now - member.lastHeartbeatMs <= member.effectiveSessionTimeoutMs(),
                member.lastHeartbeatMs,
                BrokerStatusService.bounded(assignments, partitionLimit));
    }

    private static PartitionLagInfo lagInfo(
            ClusterMetadata snapshot,
            ConsumerGroupEntry group,
            String topicName,
            PartitionEntry partition) {
        ClusterMetadata.WatermarkSnapshot watermark =
                BrokerStatusService.currentWatermark(snapshot, partition);
        long committedOffset = group.committedOffsets.getOrDefault(
                offsetKey(topicName, partition.partitionId), -1L);
        String commitState = commitState(committedOffset, watermark.physicalHighWatermark);
        long physicalLag = lag(committedOffset, watermark.physicalHighWatermark);
        long visibleLag = lag(committedOffset, watermark.visibleHighWatermark);
        return new PartitionLagInfo(
                topicName,
                partition.partitionId,
                committedOffset,
                commitState,
                watermark.physicalHighWatermark,
                physicalLag,
                watermark.visibleHighWatermark,
                visibleLag,
                watermark.nextVisibleAtEpochMs,
                BrokerStatusService.readiness(snapshot, partition),
                partition.frozen);
    }

    private static String commitState(long committedOffset, long physicalHighWatermark) {
        if (committedOffset < 0) {
            return "UNCOMMITTED";
        }
        if (physicalHighWatermark >= 0 && committedOffset > physicalHighWatermark) {
            return "OFFSET_AHEAD";
        }
        return "NORMAL";
    }

    private static long lag(long committedOffset, long watermark) {
        if (watermark < 0) {
            return -1L;
        }
        if (committedOffset < 0) {
            return watermark;
        }
        return Math.max(0L, watermark - committedOffset);
    }

    private static PartitionLagInfo moreSevereLag(PartitionLagInfo left, PartitionLagInfo right) {
        Comparator<PartitionLagInfo> severity = Comparator
                .comparingInt(GroupStatusService::readinessSeverity)
                .thenComparingInt(GroupStatusService::commitSeverity)
                .thenComparingLong(info -> lagScore(info.physicalLag()))
                .thenComparingLong(info -> lagScore(info.visibleLag()))
                .thenComparingLong(info -> -info.committedOffset());
        return severity.compare(left, right) >= 0 ? left : right;
    }

    private static int readinessSeverity(PartitionLagInfo info) {
        return switch (info.readiness()) {
            case "WATERMARK_UNKNOWN" -> 3;
            case "FROZEN" -> 2;
            case "READY" -> 0;
            default -> 1;
        };
    }

    private static int commitSeverity(PartitionLagInfo info) {
        return switch (info.commitState()) {
            case "OFFSET_AHEAD" -> 2;
            case "UNCOMMITTED" -> 1;
            default -> 0;
        };
    }

    private static long lagScore(long lag) {
        return lag < 0 ? Long.MAX_VALUE : lag;
    }

    private static String offsetKey(String topic, int partitionId) {
        return topic + ":" + partitionId;
    }

    private static int validateLimit(int limit, String name) {
        if (limit < 1 || limit > 1_000) {
            throw new IllegalArgumentException(name + " must be in 1..1000");
        }
        return limit;
    }
}
