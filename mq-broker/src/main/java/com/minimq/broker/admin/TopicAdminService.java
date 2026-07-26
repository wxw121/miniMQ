package com.minimq.broker.admin;

import com.minimq.broker.controller.ClusterController;
import com.minimq.broker.metadata.ClusterMetadata;
import com.minimq.broker.metadata.ClusterMetadata.PartitionEntry;
import com.minimq.broker.metadata.ClusterMetadata.TopicEntry;
import com.minimq.common.error.ErrorCode;
import com.minimq.protocol.message.ProtocolMessages.ConfigureTopicReliabilityRequest;
import com.minimq.protocol.message.ProtocolMessages.ConfigureTopicReliabilityResponse;
import com.minimq.protocol.message.ProtocolMessages.CreateTopicAdminRequest;
import com.minimq.protocol.message.ProtocolMessages.CreateTopicAdminResponse;
import com.minimq.protocol.message.ProtocolMessages.DeleteTopicRequest;
import com.minimq.protocol.message.ProtocolMessages.DeleteTopicResponse;
import com.minimq.protocol.message.ProtocolMessages.DescribeTopicRequest;
import com.minimq.protocol.message.ProtocolMessages.DescribeTopicResponse;
import com.minimq.protocol.message.ProtocolMessages.IncreasePartitionsRequest;
import com.minimq.protocol.message.ProtocolMessages.IncreasePartitionsResponse;
import com.minimq.protocol.message.ProtocolMessages.ListTopicsRequest;
import com.minimq.protocol.message.ProtocolMessages.ListTopicsResponse;
import com.minimq.protocol.message.ProtocolMessages.TopicPartitionInfo;
import com.minimq.protocol.message.ProtocolMessages.TopicTombstoneInfo;
import com.minimq.protocol.message.ProtocolMessages.V4BoundedList;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Protocol-facing topic administration facade.  Metadata mutation remains in
 * {@link ClusterController}; this class only validates the request boundary,
 * applies response bounds, and converts snapshots into V4 records.
 */
public final class TopicAdminService {
    private final ClusterController controller;
    private final Clock clock;

    public TopicAdminService(ClusterController controller) {
        this(controller, Clock.systemUTC());
    }

    TopicAdminService(ClusterController controller, Clock clock) {
        this.controller = controller;
        this.clock = clock;
    }

    public Result<ListTopicsResponse> list(ListTopicsRequest request) {
        List<String> names = controller.listTopicNames();
        return Result.success(new ListTopicsResponse(bounded(names, request.limit())));
    }

    public Result<DescribeTopicResponse> describe(DescribeTopicRequest request) {
        ClusterMetadata snapshot = controller.snapshot();
        TopicEntry topic = snapshot.topics.get(request.topicName());
        if (topic != null) {
            List<TopicPartitionInfo> partitions = topic.partitions.stream()
                    .sorted(Comparator.comparingInt(p -> p.partitionId))
                    .map(this::toPartitionInfo)
                    .toList();
            String retryTopic = snapshot.retryTopicBySource.get(request.topicName());
            String deadLetterTopic = snapshot.deadLetterTopicBySource.get(request.topicName());
            return Result.success(new DescribeTopicResponse(
                    request.topicName(),
                    snapshot.metadataVersion,
                    true,
                    retryTopic,
                    deadLetterTopic,
                    bounded(partitions, request.partitionLimit()),
                    null));
        }
        ClusterMetadata.TopicTombstone tombstone = snapshot.topicTombstones.get(request.topicName());
        if (tombstone == null || !request.includeDeleted()) {
            return Result.failure(ErrorCode.UNKNOWN_TOPIC);
        }
        return Result.success(new DescribeTopicResponse(
                request.topicName(),
                snapshot.metadataVersion,
                false,
                null,
                null,
                new V4BoundedList<>(0, false, List.of()),
                toTombstoneInfo(tombstone)));
    }

    public Result<IncreasePartitionsResponse> increase(IncreasePartitionsRequest request) {
        ClusterController.IncreasePartitionsResult result = controller.increasePartitions(
                request.topicName(), request.targetPartitionCount());
        if (result.errorCode() != ErrorCode.NONE) {
            return Result.failure(result.errorCode());
        }
        return Result.success(new IncreasePartitionsResponse(
                request.topicName(),
                result.previousPartitionCount(),
                result.newPartitionCount(),
                result.metadataVersion()));
    }

    public Result<CreateTopicAdminResponse> create(CreateTopicAdminRequest request) {
        ErrorCode error = controller.createTopicAdmin(request);
        if (error != ErrorCode.NONE) {
            return Result.failure(error);
        }
        return Result.success(new CreateTopicAdminResponse(
                request.topicName(), controller.metadataVersion()));
    }

    public Result<ConfigureTopicReliabilityResponse> configureReliability(
            ConfigureTopicReliabilityRequest request) {
        ClusterController.ReliabilityConfigResult result =
                controller.configureTopicReliability(request);
        if (result.errorCode() != ErrorCode.NONE) {
            return Result.failure(result.errorCode());
        }
        return Result.success(new ConfigureTopicReliabilityResponse(
                request.sourceTopicName(),
                result.retryTopicName(),
                result.deadLetterTopicName(),
                result.metadataVersion()));
    }

    /**
     * Deletion is intentionally idempotent.  A repeated request returns the
     * current tombstone state without allocating another metadata version.
     */
    public Result<DeleteTopicResponse> delete(DeleteTopicRequest request) {
        ErrorCode error = controller.deleteTopic(request.topicName(), clock.millis());
        if (error != ErrorCode.NONE) {
            return Result.failure(error);
        }
        ClusterMetadata.TopicTombstone tombstone = controller.getTopicTombstone(request.topicName());
        if (tombstone == null) {
            return Result.failure(ErrorCode.UNKNOWN_TOPIC);
        }
        return Result.success(toDeleteResponse(tombstone));
    }

    private TopicPartitionInfo toPartitionInfo(PartitionEntry partition) {
        List<Integer> replicas = partition.replicaBrokerIds == null
                ? List.of() : new ArrayList<>(partition.replicaBrokerIds);
        List<Integer> isr = partition.isrBrokerIds == null
                ? List.of() : new ArrayList<>(partition.isrBrokerIds);
        replicas.sort(Integer::compareTo);
        isr.sort(Integer::compareTo);
        String readiness = partition.frozen
                ? "FROZEN" : partition.leaderReady ? "READY" : "LEADER_NOT_READY";
        return new TopicPartitionInfo(
                partition.partitionId,
                partition.leaderBrokerId,
                partition.leaderEpoch,
                new V4BoundedList<>(replicas.size(), false, replicas),
                new V4BoundedList<>(isr.size(), false, isr),
                readiness,
                partition.frozen);
    }

    private TopicTombstoneInfo toTombstoneInfo(ClusterMetadata.TopicTombstone tombstone) {
        List<Integer> required = sorted(tombstone.requiredBrokerIds);
        List<Integer> acknowledged = sorted(tombstone.acknowledgedBrokerIds);
        List<Integer> pending = required.stream().filter(id -> !acknowledged.contains(id)).toList();
        return new TopicTombstoneInfo(
                tombstone.state,
                tombstone.tombstoneVersion,
                tombstone.deletedAtEpochMs,
                new V4BoundedList<>(required.size(), false, required),
                new V4BoundedList<>(acknowledged.size(), false, acknowledged),
                new V4BoundedList<>(pending.size(), false, pending));
    }

    private DeleteTopicResponse toDeleteResponse(ClusterMetadata.TopicTombstone tombstone) {
        TopicTombstoneInfo info = toTombstoneInfo(tombstone);
        return new DeleteTopicResponse(
                tombstone.topicName,
                info.state(),
                info.tombstoneVersion(),
                info.deletedAtEpochMs(),
                info.requiredBrokerIds(),
                info.acknowledgedBrokerIds(),
                info.pendingBrokerIds());
    }

    private static List<Integer> sorted(java.util.Collection<Integer> values) {
        return values == null ? List.of() : values.stream().sorted().toList();
    }

    private static <T> V4BoundedList<T> bounded(List<T> values, int limit) {
        int total = values.size();
        int count = Math.min(total, limit);
        return new V4BoundedList<>(total, count < total, values.subList(0, count));
    }

    public record Result<T>(ErrorCode errorCode, T response) {
        public static <T> Result<T> success(T response) {
            return new Result<>(ErrorCode.NONE, response);
        }

        public static <T> Result<T> failure(ErrorCode errorCode) {
            return new Result<>(errorCode, null);
        }
    }
}
