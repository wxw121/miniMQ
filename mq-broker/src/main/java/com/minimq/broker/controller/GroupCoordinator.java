package com.minimq.broker.controller;

import com.minimq.common.assignor.AssignmentSerde;
import com.minimq.common.assignor.RangeAssignor;
import com.minimq.broker.metadata.ClusterMetadata;
import com.minimq.broker.metadata.ClusterMetadata.ConsumerGroupEntry;
import com.minimq.broker.metadata.ClusterMetadata.MemberEntry;
import com.minimq.broker.metadata.ClusterMetadata.TopicEntry;
import com.minimq.common.error.ErrorCode;
import com.minimq.common.group.GroupSessionTimeouts;
import com.minimq.protocol.message.ProtocolMessages.HeartbeatRequest;
import com.minimq.protocol.message.ProtocolMessages.JoinGroupRequest;
import com.minimq.protocol.message.ProtocolMessages.JoinGroupResponse;
import com.minimq.protocol.message.ProtocolMessages.LeaveGroupRequest;
import com.minimq.protocol.message.ProtocolMessages.OffsetCommitRequest;
import com.minimq.protocol.message.ProtocolMessages.OffsetFetchRequest;
import com.minimq.protocol.message.ProtocolMessages.OffsetFetchResponse;
import com.minimq.protocol.message.ProtocolMessages.SyncGroupRequest;
import com.minimq.protocol.message.ProtocolMessages.SyncGroupResponse;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

/**
 * 消费组协调器，负责成员加入、分区分配同步、心跳和 offset 提交查询。
 */
public class GroupCoordinator {
    static final long HEARTBEAT_TIMEOUT_MS = GroupSessionTimeouts.DEFAULT_MS;

    private final ClusterController clusterController;
    private final LongSupplier clockMillis;

    public GroupCoordinator(ClusterController clusterController) {
        this(clusterController, System::currentTimeMillis);
    }

    GroupCoordinator(ClusterController clusterController, LongSupplier clockMillis) {
        this.clusterController = clusterController;
        this.clockMillis = clockMillis;
    }

    // 成员为空表示首次加入，协调器会生成稳定 memberId 并开启新一代分配。
    public synchronized JoinGroupResponse joinGroup(JoinGroupRequest request) {
        return clusterController.updateMetadata(metadata -> {
            ConsumerGroupEntry group = metadata.consumerGroups.computeIfAbsent(request.groupId(), id -> {
                ConsumerGroupEntry entry = new ConsumerGroupEntry();
                entry.groupId = request.groupId();
                entry.topicName = request.topicName();
                entry.generationId = 0;
                return entry;
            });
            evictStaleMembers(group, false);
            group.rebalancing = true;
            group.generationId++;

            String memberId = request.memberId().isEmpty()
                    ? request.groupId() + "-" + UUID.randomUUID()
                    : request.memberId();
            MemberEntry member = new MemberEntry();
            member.memberId = memberId;
            member.lastHeartbeatMs = now();
            member.sessionTimeoutMs = request.sessionTimeoutMs();
            group.members.put(memberId, member);
            if (group.leaderMemberId == null
                    || group.leaderMemberId.isBlank()
                    || !group.members.containsKey(group.leaderMemberId)) {
                group.leaderMemberId = group.members.keySet().stream().sorted().findFirst().orElse(memberId);
            }

            List<String> members = group.members.keySet().stream().sorted().collect(Collectors.toList());
            return new JoinGroupResponse(group.generationId, memberId, group.leaderMemberId, members);
        });
    }

    // leader 提交完整分配后，所有成员按自己的 memberId 取回对应分区列表。
    public synchronized SyncGroupResponse syncGroup(SyncGroupRequest request) {
        return clusterController.updateMetadata(metadata -> {
            ConsumerGroupEntry group = metadata.consumerGroups.get(request.groupId());
            if (group == null) {
                return new SyncGroupResponse(AssignmentSerde.encodePartitions(List.of()));
            }
            evictStaleMembers(group, true);
            if (group.generationId != request.generationId()
                    || !group.members.containsKey(request.memberId())) {
                return new SyncGroupResponse(AssignmentSerde.encodePartitions(List.of()));
            }

            byte[] leaderAssignment = request.assignment();
            if (request.memberId().equals(group.leaderMemberId)
                    && leaderAssignment != null
                    && leaderAssignment.length > 0) {
                applyLeaderAssignment(group, leaderAssignment);
                group.rebalancing = false;
            }

            MemberEntry member = group.members.get(request.memberId());
            byte[] assignment = AssignmentSerde.encodePartitions(
                    member != null ? member.assignedPartitions : List.of());
            return new SyncGroupResponse(assignment);
        });
    }

    // 心跳刷新成员存活时间；超时成员在心跳、commit、sync 和 join 路径上被踢出。
    public synchronized ErrorCode heartbeat(HeartbeatRequest request) {
        return clusterController.updateMetadata(metadata -> {
            ConsumerGroupEntry group = metadata.consumerGroups.get(request.groupId());
            if (group != null) {
                evictStaleMembers(group, true);
            }
            ErrorCode membership = requireActiveMember(group, request.memberId(), request.generationId());
            refreshHeartbeatIfPresent(group, request.memberId());
            return membership;
        });
    }

    public synchronized ErrorCode leaveGroup(LeaveGroupRequest request) {
        return clusterController.updateMetadata(metadata -> {
            ConsumerGroupEntry group = metadata.consumerGroups.get(request.groupId());
            if (group != null) {
                evictStaleMembers(group, true);
            }
            if (group == null || !group.members.containsKey(request.memberId())) {
                return ErrorCode.NONE;
            }
            group.members.remove(request.memberId());
            if (group.leaderMemberId == null
                    || group.leaderMemberId.isBlank()
                    || !group.members.containsKey(group.leaderMemberId)) {
                group.leaderMemberId = group.members.keySet().stream().sorted().findFirst().orElse("");
            }
            if (!group.members.isEmpty()) {
                group.generationId++;
                group.rebalancing = true;
            }
            return ErrorCode.NONE;
        });
    }

    public synchronized ErrorCode offsetCommit(OffsetCommitRequest request) {
        return clusterController.updateMetadata(metadata -> {
            ConsumerGroupEntry group = metadata.consumerGroups.get(request.groupId());
            if (group != null) {
                evictStaleMembers(group, true);
            }
            ErrorCode membership = requireActiveMember(group, request.memberId(), request.generationId());
            if (membership != ErrorCode.NONE) {
                return membership;
            }
            if (!request.topicName().equals(group.topicName)) {
                return ErrorCode.INVALID_REQUEST;
            }
            group.committedOffsets.put(offsetKey(request.topicName(), request.partitionId()), request.offset());
            refreshHeartbeatIfPresent(group, request.memberId());
            return ErrorCode.NONE;
        });
    }

    public synchronized OffsetFetchResponse offsetFetch(OffsetFetchRequest request) {
        ClusterMetadata metadata = clusterController.snapshot();
        ConsumerGroupEntry group = metadata.consumerGroups.get(request.groupId());
        if (group == null) {
            return new OffsetFetchResponse(-1);
        }
        Long offset = group.committedOffsets.get(offsetKey(request.topicName(), request.partitionId()));
        return new OffsetFetchResponse(offset != null ? offset : -1);
    }

    public byte[] computeLeaderAssignment(String groupId, List<String> members) {
        ClusterMetadata metadata = clusterController.snapshot();
        ConsumerGroupEntry group = metadata.consumerGroups.get(groupId);
        if (group == null) {
            return AssignmentSerde.encodePartitions(List.of());
        }
        TopicEntry topic = metadata.topics.get(group.topicName);
        int partitionCount = topic != null ? topic.partitions.size() : 0;
        Map<String, List<Integer>> assignment = RangeAssignor.assign(members, partitionCount);
        return AssignmentSerde.encodeFullAssignment(assignment);
    }

    private static ErrorCode requireActiveMember(
            ConsumerGroupEntry group, String memberId, int generationId) {
        if (group == null || !group.members.containsKey(memberId)) {
            return ErrorCode.GROUP_NOT_FOUND;
        }
        if (group.generationId != generationId) {
            return ErrorCode.REBALANCE_IN_PROGRESS;
        }
        return ErrorCode.NONE;
    }

    private long now() {
        return clockMillis.getAsLong();
    }

    private void refreshHeartbeatIfPresent(ConsumerGroupEntry group, String memberId) {
        if (group == null) {
            return;
        }
        MemberEntry member = group.members.get(memberId);
        if (member != null) {
            member.lastHeartbeatMs = now();
        }
    }

    /**
     * 踢出超过各自 session timeout 未心跳的成员。缺省超时仍是 {@link #HEARTBEAT_TIMEOUT_MS}。
     * join 路径传入 {@code bumpGenerationIfEvicted=false}，因为 join 本身会递增 generation。
     */
    private boolean evictStaleMembers(ConsumerGroupEntry group, boolean bumpGenerationIfEvicted) {
        long nowMs = now();
        boolean evicted = group.members.entrySet().removeIf(
                e -> nowMs - e.getValue().lastHeartbeatMs > e.getValue().effectiveSessionTimeoutMs());
        if (!evicted) {
            return false;
        }
        if (group.leaderMemberId == null
                || group.leaderMemberId.isBlank()
                || !group.members.containsKey(group.leaderMemberId)) {
            group.leaderMemberId = group.members.keySet().stream().sorted().findFirst().orElse("");
        }
        if (bumpGenerationIfEvicted) {
            group.generationId++;
            group.rebalancing = true;
        }
        return true;
    }

    private void applyLeaderAssignment(ConsumerGroupEntry group, byte[] assignmentBytes) {
        Map<String, List<Integer>> assignment = AssignmentSerde.decodeFullAssignment(assignmentBytes);
        for (MemberEntry member : group.members.values()) {
            member.assignedPartitions = new ArrayList<>(assignment.getOrDefault(member.memberId, List.of()));
        }
    }

    private static String offsetKey(String topic, int partitionId) {
        return topic + ":" + partitionId;
    }
}
