package com.minimq.protocol.message;

import java.util.List;

/**
 * 协议消息集合，集中定义各 API 的请求、响应和元数据 record。
 */
public final class ProtocolMessages {
    private ProtocolMessages() {
    }

    public record CreateTopicRequest(String topicName, int numPartitions) {
    }

    public record CreateTopicResponse(String topicName) {
    }

    public record MetadataRequest(String topicName) {
    }

    public record BrokerMetadata(int brokerId, String host, int port) {
    }

    public record PartitionMetadata(int partitionId, int leaderBrokerId) {
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
}
