package com.minimq.protocol.message;

import com.minimq.common.acks.MinInSyncReplicas;
import com.minimq.common.acks.ProduceAcks;
import com.minimq.common.group.GroupSessionTimeouts;
import com.minimq.common.validation.TopicNames;
import com.minimq.protocol.MqFrame;

import java.util.List;
import java.util.Map;

/**
 * 协议消息集合，集中定义各 API 的请求、响应和元数据 record。
 */
public final class ProtocolMessages {
    private ProtocolMessages() {
    }

    private static void requireNonNegative(long value, String fieldName) {
        if (value < 0) {
            throw new IllegalArgumentException(fieldName + " must be non-negative");
        }
    }

    private static void requireRange(int value, int min, int max, String fieldName) {
        if (value < min || value > max) {
            throw new IllegalArgumentException(
                    fieldName + " must be in " + min + ".." + max + ": " + value);
        }
    }

    private static void requireNonEmpty(String value, String fieldName) {
        if (value == null || value.isEmpty()) {
            throw new IllegalArgumentException(fieldName + " must not be empty");
        }
    }

    private static void requireTopicName(String value, String fieldName) {
        try {
            TopicNames.requireValidReference(value);
        } catch (IllegalArgumentException invalid) {
            throw new IllegalArgumentException(fieldName + " is not a valid topic reference", invalid);
        }
    }

    private static void requireNewTopicName(String value, String fieldName) {
        try {
            TopicNames.requireValid(value);
        } catch (IllegalArgumentException invalid) {
            throw new IllegalArgumentException(fieldName + " is not a valid new topic name", invalid);
        }
    }

    private static void requireOptionalTopicName(String value, String fieldName) {
        if (value != null) {
            requireTopicName(value, fieldName);
        }
    }

    public record CreateTopicRequest(
            String topicName, int numPartitions, int replicationFactor, int minInSyncReplicas) {
        public CreateTopicRequest {
            requireNewTopicName(topicName, "topicName");
            MinInSyncReplicas.requireRange(minInSyncReplicas);
        }

        public CreateTopicRequest(String topicName, int numPartitions, int replicationFactor) {
            this(topicName, numPartitions, replicationFactor, MinInSyncReplicas.DEFAULT);
        }

        public CreateTopicRequest(String topicName, int numPartitions) {
            this(topicName, numPartitions, 1);
        }
    }

    public record CreateTopicResponse(String topicName) {
    }

    public record MetadataRequest(String topicName) {
        public MetadataRequest {
            if (topicName == null) {
                throw new IllegalArgumentException("topicName must not be null");
            }
            if (!topicName.isEmpty()) {
                requireTopicName(topicName, "topicName");
            }
        }
    }

    public record BrokerMetadata(int brokerId, String host, int port) {
    }

    public record ControllerMetadata(int brokerId, String host, int port, long controllerEpoch) {
        public ControllerMetadata {
            requireNonNegative(controllerEpoch, "controllerEpoch");
        }
    }

    public record PartitionMetadata(
            int partitionId,
            int leaderBrokerId,
            List<Integer> replicaBrokerIds,
            List<Integer> isrBrokerIds,
            Map<Integer, Long> replicaLogEndOffsets,
            boolean frozen,
            long leaderEpoch,
            long leaderEpochStartOffset,
            boolean leaderReady) {
        public PartitionMetadata {
            requireNonNegative(leaderEpoch, "leaderEpoch");
            requireNonNegative(leaderEpochStartOffset, "leaderEpochStartOffset");
        }
        public PartitionMetadata(int partitionId, int leaderBrokerId) {
            this(partitionId, leaderBrokerId, List.of(leaderBrokerId), List.of(leaderBrokerId), Map.of(),
                    false, 0L, 0L, true);
        }

        public PartitionMetadata(
                int partitionId,
                int leaderBrokerId,
                List<Integer> replicaBrokerIds,
                List<Integer> isrBrokerIds) {
            this(partitionId, leaderBrokerId, replicaBrokerIds, isrBrokerIds, Map.of(), false, 0L, 0L, true);
        }

        public PartitionMetadata(
                int partitionId,
                int leaderBrokerId,
                List<Integer> replicaBrokerIds,
                List<Integer> isrBrokerIds,
                Map<Integer, Long> replicaLogEndOffsets) {
            this(partitionId, leaderBrokerId, replicaBrokerIds, isrBrokerIds, replicaLogEndOffsets,
                    false, 0L, 0L, true);
        }

        public PartitionMetadata(
                int partitionId,
                int leaderBrokerId,
                List<Integer> replicaBrokerIds,
                List<Integer> isrBrokerIds,
                Map<Integer, Long> replicaLogEndOffsets,
                boolean frozen) {
            this(partitionId, leaderBrokerId, replicaBrokerIds, isrBrokerIds, replicaLogEndOffsets,
                    frozen, 0L, 0L, !frozen);
        }
    }

    public record TopicMetadata(String topicName, List<PartitionMetadata> partitions) {
    }

    public record MetadataResponse(
            List<BrokerMetadata> brokers,
            List<TopicMetadata> topics,
            ControllerMetadata controller,
            long metadataVersion) {
        public MetadataResponse {
            requireNonNegative(metadataVersion, "metadataVersion");
        }
        public MetadataResponse(List<BrokerMetadata> brokers, List<TopicMetadata> topics) {
            this(brokers, topics, null, 0L);
        }
    }

    public record ProduceRecord(byte[] key, byte[] value) {
    }

    public record ProduceRequest(
            String topicName,
            int partitionId,
            long leaderEpoch,
            List<ProduceRecord> records,
            short acks,
            int ackTimeoutMs) {
        public ProduceRequest {
            requireTopicName(topicName, "topicName");
            requireNonNegative(leaderEpoch, "leaderEpoch");
            ProduceAcks.requireValid(acks);
            ProduceAcks.requireTimeoutMs(ackTimeoutMs);
        }

        public ProduceRequest(String topicName, int partitionId, long leaderEpoch, List<ProduceRecord> records) {
            this(topicName, partitionId, leaderEpoch, records, ProduceAcks.LEADER.code(), ProduceAcks.DEFAULT_TIMEOUT_MS);
        }

        public ProduceRequest(String topicName, int partitionId, List<ProduceRecord> records) {
            this(topicName, partitionId, 0L, records);
        }
    }

    public record ProduceResponse(String topicName, int partitionId, long baseOffset) {
    }

    public record FetchRequest(String topicName, int partitionId, long leaderEpoch, long fetchOffset, int maxBytes) {
        public FetchRequest {
            requireTopicName(topicName, "topicName");
            requireNonNegative(leaderEpoch, "leaderEpoch");
        }
        public FetchRequest(String topicName, int partitionId, long fetchOffset, int maxBytes) {
            this(topicName, partitionId, 0L, fetchOffset, maxBytes);
        }
    }

    public record FetchedRecord(long offset, byte[] key, byte[] value) {
    }

    public record FetchResponse(
            String topicName,
            int partitionId,
            long highWatermark,
            List<FetchedRecord> records) {
    }

    public record JoinGroupRequest(
            String groupId, String memberId, String topicName, int sessionTimeoutMs) {
        public JoinGroupRequest {
            requireTopicName(topicName, "topicName");
            GroupSessionTimeouts.requireValid(sessionTimeoutMs);
        }

        public JoinGroupRequest(String groupId, String memberId, String topicName) {
            this(groupId, memberId, topicName, GroupSessionTimeouts.DEFAULT_MS);
        }
    }

    public record JoinGroupResponse(
            int generationId,
            String memberId,
            String leaderMemberId,
            List<String> members) {
    }

    public record SyncGroupRequest(String groupId, int generationId, String memberId, byte[] assignment) {
    }

    public record SyncGroupResponse(byte[] assignment) {
    }

    public record HeartbeatRequest(String groupId, String memberId, int generationId) {
        public HeartbeatRequest {
            if (memberId == null) {
                throw new IllegalArgumentException("memberId must not be null");
            }
            requireNonNegative(generationId, "generationId");
        }

        public HeartbeatRequest(String groupId, String memberId) {
            this(groupId, memberId, 0);
        }
    }

    public record HeartbeatResponse() {
    }

    public record LeaveGroupRequest(String groupId, String memberId, int generationId) {
        public LeaveGroupRequest {
            if (memberId == null) {
                throw new IllegalArgumentException("memberId must not be null");
            }
            requireNonNegative(generationId, "generationId");
        }
    }

    public record LeaveGroupResponse() {
    }

    public record OffsetCommitRequest(
            String groupId,
            String topicName,
            int partitionId,
            long offset,
            String memberId,
            int generationId) {
        public OffsetCommitRequest {
            requireTopicName(topicName, "topicName");
            if (memberId == null) {
                throw new IllegalArgumentException("memberId must not be null");
            }
            requireNonNegative(generationId, "generationId");
        }

        public OffsetCommitRequest(String groupId, String topicName, int partitionId, long offset) {
            this(groupId, topicName, partitionId, offset, "", 0);
        }
    }

    public record OffsetCommitResponse() {
    }

    public record OffsetFetchRequest(String groupId, String topicName, int partitionId) {
        public OffsetFetchRequest {
            requireTopicName(topicName, "topicName");
        }
    }

    public record OffsetFetchResponse(long offset) {
    }

    public record RegisterBrokerRequest(int brokerId, String host, int port, long controllerEpoch) {
        public RegisterBrokerRequest {
            requireNonNegative(controllerEpoch, "controllerEpoch");
        }
        public RegisterBrokerRequest(int brokerId, String host, int port) {
            this(brokerId, host, port, 0L);
        }
    }

    public record RegisterBrokerResponse(long brokerEpoch, long leaseTimeoutMs) {
        public RegisterBrokerResponse() {
            this(0L, 0L);
        }
    }

    public record BrokerHeartbeatRequest(int brokerId, long brokerEpoch, long controllerEpoch) {
        public BrokerHeartbeatRequest {
            requireNonNegative(brokerEpoch, "brokerEpoch");
            requireNonNegative(controllerEpoch, "controllerEpoch");
        }
        public BrokerHeartbeatRequest(int brokerId, long brokerEpoch) {
            this(brokerId, brokerEpoch, 0L);
        }
    }

    public record BrokerHeartbeatResponse() {
    }

    public record ReplicaFetchRequest(
            String topicName,
            int partitionId,
            int replicaBrokerId,
            long leaderEpoch,
            long fetchOffset,
            int maxBytes) {
        public ReplicaFetchRequest {
            requireTopicName(topicName, "topicName");
            requireNonNegative(leaderEpoch, "leaderEpoch");
        }
        public ReplicaFetchRequest(
                String topicName, int partitionId, int replicaBrokerId, long fetchOffset, int maxBytes) {
            this(topicName, partitionId, replicaBrokerId, 0L, fetchOffset, maxBytes);
        }
    }

    public record ReplicaFetchResponse(
            String topicName,
            int partitionId,
            long highWatermark,
            long leaderEpoch,
            long leaderLogEndOffset,
            List<FetchedRecord> records) {
        public ReplicaFetchResponse {
            requireNonNegative(leaderEpoch, "leaderEpoch");
        }
        public ReplicaFetchResponse(
                String topicName, int partitionId, long highWatermark, List<FetchedRecord> records) {
            this(topicName, partitionId, highWatermark, 0L, highWatermark, records);
        }
    }

    public record MarkBrokerOfflineRequest(int brokerId) {
    }

    public record MarkBrokerOfflineResponse() {
    }

    public record UpdateReplicaLeoRequest(
            String topicName,
            int partitionId,
            int replicaBrokerId,
            long brokerEpoch,
            long controllerEpoch,
            long logEndOffset) {
        public UpdateReplicaLeoRequest {
            requireTopicName(topicName, "topicName");
            requireNonNegative(brokerEpoch, "brokerEpoch");
            requireNonNegative(controllerEpoch, "controllerEpoch");
        }
        public UpdateReplicaLeoRequest(
                String topicName, int partitionId, int replicaBrokerId, long logEndOffset) {
            this(topicName, partitionId, replicaBrokerId, 0L, 0L, logEndOffset);
        }
    }

    public record UpdateReplicaLeoResponse() {
    }

    public record DiscoverControllerRequest() {
    }

    public record DiscoverControllerResponse(ControllerMetadata controller, long metadataVersion) {
        public DiscoverControllerResponse {
            requireNonNegative(metadataVersion, "metadataVersion");
        }
    }

    public record LeaderReadyRequest(
            String topicName,
            int partitionId,
            int brokerId,
            long brokerEpoch,
            long controllerEpoch,
            long leaderEpoch,
            long logEndOffset) {
        public LeaderReadyRequest {
            requireTopicName(topicName, "topicName");
            requireNonNegative(brokerEpoch, "brokerEpoch");
            requireNonNegative(controllerEpoch, "controllerEpoch");
            requireNonNegative(leaderEpoch, "leaderEpoch");
        }
    }

    public record LeaderReadyResponse() {
    }

    public record MaintenanceReportRequest(String adminToken) {
        public MaintenanceReportRequest() {
            this("");
        }
    }

    public record MaintenanceRunRequest(String adminToken) {
        public MaintenanceRunRequest() {
            this("");
        }
    }

    public record MaintenanceMetricsRequest(String adminToken) {
        public MaintenanceMetricsRequest() {
            this("");
        }
    }

    public record RuntimeMetricsRequest(String adminToken) {
        public RuntimeMetricsRequest() {
            this("");
        }
    }

    public record MaintenanceSegmentInfo(
            String partition,
            String action,
            long baseOffset,
            long firstOffset,
            long lastOffset,
            long sizeBytes) {
    }

    public record MaintenanceReportResponse(
            boolean dryRun,
            String status,
            List<MaintenanceSegmentInfo> segmentReports,
            long skippedDueToReaders,
            long cleanupFailures,
            long deletedBytes,
            long deletedSegments,
            long compactedBytes,
            long compactedSegments) {
    }

    public record MaintenanceMetricsResponse(
            long runs,
            long dryRuns,
            long deletedBytes,
            long deletedSegments,
            long compactedBytes,
            long compactedSegments,
            long skippedDueToReaders,
            long cleanupFailures,
            String lastRunStatus) {
    }

    public record RuntimeMetricCount(short apiKey, long count) {
        public RuntimeMetricCount {
            requireNonNegative(count, "count");
        }
    }

    public record RuntimeErrorCount(short errorCode, long count) {
        public RuntimeErrorCount {
            requireNonNegative(count, "count");
        }
    }

    public record RuntimeLatencySummary(short apiKey, long count, long totalLatencyMs, double averageLatencyMs) {
        public RuntimeLatencySummary {
            requireNonNegative(count, "count");
            requireNonNegative(totalLatencyMs, "totalLatencyMs");
            if (averageLatencyMs < 0.0) {
                throw new IllegalArgumentException("averageLatencyMs must be non-negative");
            }
        }
    }

    public record RuntimeMetricsResponse(
            List<RuntimeMetricCount> requestsByApiKey,
            List<RuntimeErrorCount> responsesByErrorCode,
            List<RuntimeLatencySummary> latencyByApiKey,
            List<RuntimeMetricCount> throttledByApiKey) {
    }

    public record OpsStatusRequest(String credential, int brokerLimit, int partitionLimit) {
        public OpsStatusRequest {
            requireNonEmpty(credential, "credential");
            requireRange(brokerLimit, 1, 1_000, "brokerLimit");
            requireRange(partitionLimit, 1, 1_000, "partitionLimit");
        }
    }

    public record GroupStatusRequest(
            String credential,
            String groupId,
            String topicFilter,
            int memberLimit,
            int partitionLimit) {
        public GroupStatusRequest {
            requireNonEmpty(credential, "credential");
            requireNonEmpty(groupId, "groupId");
            requireOptionalTopicName(topicFilter, "topicFilter");
            requireRange(memberLimit, 1, 1_000, "memberLimit");
            requireRange(partitionLimit, 1, 1_000, "partitionLimit");
        }
    }

    public record ListTopicsRequest(String credential, int limit) {
        public ListTopicsRequest {
            requireNonEmpty(credential, "credential");
            requireRange(limit, 1, 1_000, "limit");
        }
    }

    public record DescribeTopicRequest(
            String credential, String topicName, int partitionLimit, boolean includeDeleted) {
        public DescribeTopicRequest {
            requireNonEmpty(credential, "credential");
            requireTopicName(topicName, "topicName");
            requireRange(partitionLimit, 1, 1_000, "partitionLimit");
        }
    }

    public record DeleteTopicRequest(String credential, String topicName) {
        public DeleteTopicRequest {
            requireNonEmpty(credential, "credential");
            requireTopicName(topicName, "topicName");
        }
    }

    public record IncreasePartitionsRequest(
            String credential, String topicName, int targetPartitionCount) {
        public IncreasePartitionsRequest {
            requireNonEmpty(credential, "credential");
            requireTopicName(topicName, "topicName");
            requireRange(targetPartitionCount, 1, 10_000, "targetPartitionCount");
        }
    }

    public record FailoverAuditRequest(String credential, int limit) {
        public FailoverAuditRequest {
            requireNonEmpty(credential, "credential");
            requireRange(limit, 1, 1_000, "limit");
        }
    }

    public record AuthStatusRequest(String credential) {
        public AuthStatusRequest {
            requireNonEmpty(credential, "credential");
        }
    }

    public record ConfigureTopicReliabilityRequest(
            String credential,
            String sourceTopicName,
            boolean retryTopicSupplied,
            String retryTopicName,
            boolean clearRetryTopic,
            boolean deadLetterTopicSupplied,
            String deadLetterTopicName,
            boolean clearDeadLetterTopic) {
        public ConfigureTopicReliabilityRequest {
            requireNonEmpty(credential, "credential");
            requireTopicName(sourceTopicName, "sourceTopicName");
            if (retryTopicSupplied == clearRetryTopic) {
                if (retryTopicSupplied) {
                    throw new IllegalArgumentException("retry topic cannot be supplied and cleared");
                }
            }
            if (deadLetterTopicSupplied == clearDeadLetterTopic) {
                if (deadLetterTopicSupplied) {
                    throw new IllegalArgumentException(
                            "dead-letter topic cannot be supplied and cleared");
                }
            }
            if (retryTopicSupplied) {
                requireTopicName(retryTopicName, "retryTopicName");
            } else if (retryTopicName != null) {
                throw new IllegalArgumentException(
                        "retryTopicName requires retryTopicSupplied");
            }
            if (deadLetterTopicSupplied) {
                requireTopicName(deadLetterTopicName, "deadLetterTopicName");
            } else if (deadLetterTopicName != null) {
                throw new IllegalArgumentException(
                        "deadLetterTopicName requires deadLetterTopicSupplied");
            }
        }
    }

    public record MarkBrokerOfflineAdminRequest(String credential, int brokerId) {
        public MarkBrokerOfflineAdminRequest {
            requireNonEmpty(credential, "credential");
        }
    }

    public record CreateTopicAdminRequest(
            String credential,
            String topicName,
            int numPartitions,
            int replicationFactor,
            int minInSyncReplicas) {
        public CreateTopicAdminRequest {
            requireNonEmpty(credential, "credential");
            requireNewTopicName(topicName, "topicName");
            requireRange(numPartitions, 1, 10_000, "numPartitions");
            requireRange(replicationFactor, 1, 10_000, "replicationFactor");
            MinInSyncReplicas.requireRange(minInSyncReplicas);
        }

        public CreateTopicAdminRequest(
                String credential, String topicName, int numPartitions, int replicationFactor) {
            this(credential, topicName, numPartitions, replicationFactor, MinInSyncReplicas.DEFAULT);
        }
    }

    public record ScheduledProduceRecord(byte[] key, byte[] value, long deliverAtEpochMs) {
        public ScheduledProduceRecord {
            requireNonNegative(deliverAtEpochMs, "deliverAtEpochMs");
        }
    }

    public record ProduceV2Request(
            String topicName,
            int partitionId,
            long leaderEpoch,
            V4BoundedList<ScheduledProduceRecord> records,
            short acks,
            int ackTimeoutMs) {
        public ProduceV2Request {
            requireTopicName(topicName, "topicName");
            requireNonNegative(leaderEpoch, "leaderEpoch");
            if (records == null) {
                throw new IllegalArgumentException("records must not be null");
            }
            ProduceAcks.requireValid(acks);
            ProduceAcks.requireTimeoutMs(ackTimeoutMs);
        }

        public ProduceV2Request(
                String topicName,
                int partitionId,
                long leaderEpoch,
                V4BoundedList<ScheduledProduceRecord> records) {
            this(topicName, partitionId, leaderEpoch, records, ProduceAcks.LEADER.code(), ProduceAcks.DEFAULT_TIMEOUT_MS);
        }
    }

    public record ReplicaFetchV2Request(
            String clusterCredential,
            String topicName,
            int partitionId,
            int replicaBrokerId,
            long brokerEpoch,
            long controllerEpoch,
            long leaderEpoch,
            long fetchOffset,
            int maxBytes) {
        public ReplicaFetchV2Request {
            requireNonEmpty(clusterCredential, "clusterCredential");
            requireTopicName(topicName, "topicName");
            requireNonNegative(brokerEpoch, "brokerEpoch");
            requireNonNegative(controllerEpoch, "controllerEpoch");
            requireNonNegative(leaderEpoch, "leaderEpoch");
            requireNonNegative(fetchOffset, "fetchOffset");
            requireRange(maxBytes, 1, MqFrame.MAX_FRAME_BYTES, "maxBytes");
        }
    }

    public record TopicDeleteAckRequest(
            String clusterCredential,
            String topicName,
            long tombstoneVersion,
            int brokerId,
            long brokerEpoch,
            long controllerEpoch) {
        public TopicDeleteAckRequest {
            requireNonEmpty(clusterCredential, "clusterCredential");
            requireTopicName(topicName, "topicName");
            requireNonNegative(tombstoneVersion, "tombstoneVersion");
            requireNonNegative(brokerEpoch, "brokerEpoch");
            requireNonNegative(controllerEpoch, "controllerEpoch");
        }
    }

    public record FetchV2Request(
            String topicName, int partitionId, long leaderEpoch, long fetchOffset, int maxBytes) {
        public FetchV2Request {
            requireTopicName(topicName, "topicName");
            requireNonNegative(leaderEpoch, "leaderEpoch");
            requireNonNegative(fetchOffset, "fetchOffset");
            requireRange(maxBytes, 1, MqFrame.MAX_FRAME_BYTES, "maxBytes");
        }
    }

    public record MetadataV2Request(
            String topicFilter,
            String topicStartAfter,
            String tombstoneStartAfter,
            int topicLimit,
            int partitionLimit) {
        public MetadataV2Request(String topicFilter, int topicLimit, int partitionLimit) {
            this(topicFilter, null, null, topicLimit, partitionLimit);
        }

        public MetadataV2Request {
            requireOptionalTopicName(topicFilter, "topicFilter");
            requireOptionalTopicName(topicStartAfter, "topicStartAfter");
            requireOptionalTopicName(tombstoneStartAfter, "tombstoneStartAfter");
            if (topicFilter != null && (topicStartAfter != null || tombstoneStartAfter != null)) {
                throw new IllegalArgumentException("metadata cursors require an absent topic filter");
            }
            requireRange(topicLimit, 1, 10_000, "topicLimit");
            requireRange(partitionLimit, 1, 10_000, "partitionLimit");
        }
    }

    public record RuntimeMetricsV2Request(
            String credential, int counterLimit, int latencyLimit, int lagLimit) {
        public RuntimeMetricsV2Request {
            requireNonEmpty(credential, "credential");
            requireRange(counterLimit, 1, 1_000, "counterLimit");
            requireRange(latencyLimit, 1, 1_000, "latencyLimit");
            requireRange(lagLimit, 1, 1_000, "lagLimit");
        }
    }

    public record UpdateReplicaStateV2Request(
            String clusterCredential,
            String topicName,
            int partitionId,
            int replicaBrokerId,
            long brokerEpoch,
            long controllerEpoch,
            long leaderEpoch,
            long physicalLogEndOffset,
            long visibleLogEndOffset,
            long nextVisibleAtEpochMs) {
        public UpdateReplicaStateV2Request {
            requireNonEmpty(clusterCredential, "clusterCredential");
            requireTopicName(topicName, "topicName");
            requireNonNegative(brokerEpoch, "brokerEpoch");
            requireNonNegative(controllerEpoch, "controllerEpoch");
            requireNonNegative(leaderEpoch, "leaderEpoch");
            requireNonNegative(physicalLogEndOffset, "physicalLogEndOffset");
            requireNonNegative(visibleLogEndOffset, "visibleLogEndOffset");
            if (visibleLogEndOffset > physicalLogEndOffset) {
                throw new IllegalArgumentException(
                        "visibleLogEndOffset cannot exceed physicalLogEndOffset");
            }
            if (visibleLogEndOffset == physicalLogEndOffset) {
                if (nextVisibleAtEpochMs != -1L) {
                    throw new IllegalArgumentException(
                            "nextVisibleAtEpochMs must be -1 when visible equals physical");
                }
            } else if (nextVisibleAtEpochMs < 0L) {
                throw new IllegalArgumentException(
                        "nextVisibleAtEpochMs must be non-negative when visibility is blocked");
            }
        }
    }

    public record V4BoundedList<T>(int total, boolean truncated, List<T> items) {
        public V4BoundedList {
            if (total < 0) {
                throw new IllegalArgumentException("total must be non-negative");
            }
            if (items == null || items.size() > 10_000) {
                throw new IllegalArgumentException("items must contain at most 10000 entries");
            }
            items = List.copyOf(items);
            if (total < items.size()) {
                throw new IllegalArgumentException("total cannot be smaller than encoded item count");
            }
            if (truncated && total <= items.size()) {
                throw new IllegalArgumentException(
                        "truncated list total must exceed encoded item count");
            }
            if (!truncated && total != items.size()) {
                throw new IllegalArgumentException(
                        "non-truncated list total must equal encoded item count");
            }
        }
    }

    public record OpsBrokerInfo(
            int brokerId,
            String host,
            int port,
            String state,
            long brokerEpoch,
            long leaseDeadlineEpochMs) {
    }

    public record OpsPartitionInfo(
            String topicName,
            int partitionId,
            int leaderId,
            long leaderEpoch,
            long physicalHighWatermark,
            long visibleHighWatermark,
            long nextVisibleAtEpochMs,
            String readiness,
            boolean frozen,
            V4BoundedList<Integer> replicaIds,
            V4BoundedList<Integer> isrIds) {
    }

    public record GroupAssignmentInfo(String topicName, int partitionId) {
    }

    public record GroupMemberInfo(
            String memberId,
            boolean active,
            long lastHeartbeatEpochMs,
            V4BoundedList<GroupAssignmentInfo> assignments) {
    }

    public record PartitionLagInfo(
            String topicName,
            int partitionId,
            long committedOffset,
            String commitState,
            long physicalHighWatermark,
            long physicalLag,
            long visibleHighWatermark,
            long visibleLag,
            long nextVisibleAtEpochMs,
            String readiness,
            boolean frozen) {
    }

    public record TopicPartitionInfo(
            int partitionId,
            int leaderId,
            long leaderEpoch,
            V4BoundedList<Integer> replicaIds,
            V4BoundedList<Integer> isrIds,
            String readiness,
            boolean frozen) {
    }

    public record TopicTombstoneInfo(
            String state,
            long tombstoneVersion,
            long deletedAtEpochMs,
            V4BoundedList<Integer> requiredBrokerIds,
            V4BoundedList<Integer> acknowledgedBrokerIds,
            V4BoundedList<Integer> pendingBrokerIds) {
    }

    public record TopicTombstoneSummary(String topicName, TopicTombstoneInfo tombstone) {
        public TopicTombstoneSummary {
            requireTopicName(topicName, "topicName");
            if (tombstone == null) {
                throw new IllegalArgumentException("tombstone must not be null");
            }
        }
    }

    public record AuditEventInfo(
            long sequence,
            long timestampEpochMs,
            String eventType,
            Integer brokerId,
            String topicName,
            Integer partitionId,
            String oldValue,
            String newValue,
            String detail) {
    }

    public record TopicSummary(
            String topicName,
            int partitionCount,
            int replicationFactor,
            String retryTopic,
            String deadLetterTopic) {
    }

    public record ScheduledReplicaRecord(
            long offset, byte[] key, byte[] value, long deliverAtEpochMs) {
        public ScheduledReplicaRecord {
            requireNonNegative(offset, "offset");
            requireNonNegative(deliverAtEpochMs, "deliverAtEpochMs");
        }
    }

    public record RuntimeLatencyV2Summary(
            short apiKey, long count, long totalNanos, double maxMillis) {
        public RuntimeLatencyV2Summary {
            requireNonNegative(count, "count");
            requireNonNegative(totalNanos, "totalNanos");
            if (maxMillis < 0.0) {
                throw new IllegalArgumentException("maxMillis must be non-negative");
            }
        }
    }

    public record OpsStatusResponse(
            int targetBrokerId,
            long startedAtEpochMs,
            long snapshotAtEpochMs,
            long metadataVersion,
            int metadataFormatVersion,
            String role,
            long brokerEpoch,
            long controllerEpoch,
            String transportMode,
            String maintenanceState,
            V4BoundedList<OpsBrokerInfo> brokers,
            V4BoundedList<OpsPartitionInfo> partitions) {
    }

    public record GroupStatusResponse(
            String groupId,
            String topicName,
            int generationId,
            V4BoundedList<GroupMemberInfo> members,
            V4BoundedList<PartitionLagInfo> partitionLags) {
    }

    public record ListTopicsResponse(V4BoundedList<String> topicNames) {
    }

    public record DescribeTopicResponse(
            String topicName,
            long metadataVersion,
            boolean active,
            String retryTopic,
            String deadLetterTopic,
            V4BoundedList<TopicPartitionInfo> partitions,
            TopicTombstoneInfo tombstone,
            int minInSyncReplicas) {
        public DescribeTopicResponse {
            MinInSyncReplicas.requireRange(minInSyncReplicas);
        }

        public DescribeTopicResponse(
                String topicName,
                long metadataVersion,
                boolean active,
                String retryTopic,
                String deadLetterTopic,
                V4BoundedList<TopicPartitionInfo> partitions,
                TopicTombstoneInfo tombstone) {
            this(
                    topicName,
                    metadataVersion,
                    active,
                    retryTopic,
                    deadLetterTopic,
                    partitions,
                    tombstone,
                    MinInSyncReplicas.DEFAULT);
        }
    }

    public record DeleteTopicResponse(
            String topicName,
            String tombstoneState,
            long tombstoneVersion,
            long deletedAtEpochMs,
            V4BoundedList<Integer> requiredBrokerIds,
            V4BoundedList<Integer> acknowledgedBrokerIds,
            V4BoundedList<Integer> pendingBrokerIds) {
    }

    public record IncreasePartitionsResponse(
            String topicName,
            int previousPartitionCount,
            int newPartitionCount,
            long metadataVersion) {
    }

    public record FailoverAuditResponse(
            int brokerId,
            long brokerStartedAtEpochMs,
            long overwrittenCount,
            V4BoundedList<AuditEventInfo> events) {
    }

    public record AuthStatusResponse(int targetBrokerId, V4BoundedList<String> scopes) {
    }

    public record ConfigureTopicReliabilityResponse(
            String sourceTopicName,
            String retryTopicName,
            String deadLetterTopicName,
            long metadataVersion) {
    }

    public record MarkBrokerOfflineAdminResponse(int brokerId, long metadataVersion) {
    }

    public record CreateTopicAdminResponse(String topicName, long metadataVersion) {
    }

    public record ProduceV2Response(String topicName, int partitionId, long baseOffset) {
    }

    public record ReplicaFetchV2Response(
            String topicName,
            int partitionId,
            long physicalHighWatermark,
            long visibleHighWatermark,
            long nextVisibleAtEpochMs,
            long leaderEpoch,
            long leaderLogEndOffset,
            V4BoundedList<ScheduledReplicaRecord> records) {
    }

    public record TopicDeleteAckResponse() {
    }

    public record FetchV2Response(
            String topicName,
            int partitionId,
            long physicalHighWatermark,
            long visibleHighWatermark,
            long nextVisibleAtEpochMs,
            V4BoundedList<FetchedRecord> records) {
    }

    public record MetadataV2Response(
            V4BoundedList<OpsBrokerInfo> brokers,
            V4BoundedList<TopicSummary> topics,
            V4BoundedList<TopicTombstoneSummary> tombstones,
            ControllerMetadata controller,
            long metadataVersion,
            int metadataFormatVersion) {
    }

    public record RuntimeMetricsV2Response(
            long startedAtEpochMs,
            long snapshotAtEpochMs,
            long uptimeMs,
            V4BoundedList<RuntimeMetricCount> requestsByApiKey,
            V4BoundedList<RuntimeErrorCount> responsesByErrorCode,
            V4BoundedList<RuntimeLatencyV2Summary> latencyByApiKey,
            V4BoundedList<RuntimeMetricCount> throttledByApiKey,
            long produceBytes,
            long fetchBytes,
            long auditOverwrittenCount,
            V4BoundedList<PartitionLagInfo> lagMetrics) {
    }

    public record UpdateReplicaStateV2Response() {
    }
}
