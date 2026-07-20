package com.minimq.protocol.message;

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

    public record CreateTopicRequest(String topicName, int numPartitions, int replicationFactor) {
        public CreateTopicRequest(String topicName, int numPartitions) {
            this(topicName, numPartitions, 1);
        }
    }

    public record CreateTopicResponse(String topicName) {
    }

    public record MetadataRequest(String topicName) {
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

    public record ProduceRequest(String topicName, int partitionId, long leaderEpoch, List<ProduceRecord> records) {
        public ProduceRequest {
            requireNonNegative(leaderEpoch, "leaderEpoch");
        }
        public ProduceRequest(String topicName, int partitionId, List<ProduceRecord> records) {
            this(topicName, partitionId, 0L, records);
        }
    }

    public record ProduceResponse(String topicName, int partitionId, long baseOffset) {
    }

    public record FetchRequest(String topicName, int partitionId, long leaderEpoch, long fetchOffset, int maxBytes) {
        public FetchRequest {
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

    public record JoinGroupRequest(String groupId, String memberId, String topicName) {
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

    public record HeartbeatRequest(String groupId, String memberId) {
    }

    public record HeartbeatResponse() {
    }

    public record OffsetCommitRequest(String groupId, String topicName, int partitionId, long offset) {
    }

    public record OffsetCommitResponse() {
    }

    public record OffsetFetchRequest(String groupId, String topicName, int partitionId) {
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
}
