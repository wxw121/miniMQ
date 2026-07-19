package com.minimq.protocol.message;

import java.util.List;
import java.util.Map;

/**
 * 协议消息集合，集中定义各 API 的请求、响应和元数据 record。
 */
public final class ProtocolMessages {
    private ProtocolMessages() {
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

    public record PartitionMetadata(
            int partitionId,
            int leaderBrokerId,
            List<Integer> replicaBrokerIds,
            List<Integer> isrBrokerIds,
            Map<Integer, Long> replicaLogEndOffsets) {
        public PartitionMetadata(int partitionId, int leaderBrokerId) {
            this(partitionId, leaderBrokerId, List.of(leaderBrokerId), List.of(leaderBrokerId), Map.of());
        }

        public PartitionMetadata(
                int partitionId,
                int leaderBrokerId,
                List<Integer> replicaBrokerIds,
                List<Integer> isrBrokerIds) {
            this(partitionId, leaderBrokerId, replicaBrokerIds, isrBrokerIds, Map.of());
        }
    }

    public record TopicMetadata(String topicName, List<PartitionMetadata> partitions) {
    }

    public record MetadataResponse(List<BrokerMetadata> brokers, List<TopicMetadata> topics) {
    }

    public record ProduceRecord(byte[] key, byte[] value) {
    }

    public record ProduceRequest(String topicName, int partitionId, List<ProduceRecord> records) {
    }

    public record ProduceResponse(String topicName, int partitionId, long baseOffset) {
    }

    public record FetchRequest(String topicName, int partitionId, long fetchOffset, int maxBytes) {
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

    public record RegisterBrokerRequest(int brokerId, String host, int port) {
    }

    public record RegisterBrokerResponse() {
    }

    public record ReplicaFetchRequest(
            String topicName,
            int partitionId,
            int replicaBrokerId,
            long fetchOffset,
            int maxBytes) {
    }

    public record ReplicaFetchResponse(
            String topicName,
            int partitionId,
            long highWatermark,
            List<FetchedRecord> records) {
    }

    public record MarkBrokerOfflineRequest(int brokerId) {
    }

    public record MarkBrokerOfflineResponse() {
    }

    public record UpdateReplicaLeoRequest(
            String topicName,
            int partitionId,
            int replicaBrokerId,
            long logEndOffset) {
    }

    public record UpdateReplicaLeoResponse() {
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
