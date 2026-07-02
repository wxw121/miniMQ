package com.minimq.broker.controller;

import com.minimq.broker.metadata.ClusterMetadata;
import com.minimq.broker.metadata.ClusterMetadata.BrokerEntry;
import com.minimq.broker.metadata.ClusterMetadata.PartitionEntry;
import com.minimq.broker.metadata.ClusterMetadata.TopicEntry;
import com.minimq.broker.metadata.MetadataStore;
import com.minimq.common.error.ErrorCode;
import com.minimq.protocol.message.ProtocolMessages.BrokerMetadata;
import com.minimq.protocol.message.ProtocolMessages.CreateTopicRequest;
import com.minimq.protocol.message.ProtocolMessages.MetadataRequest;
import com.minimq.protocol.message.ProtocolMessages.MetadataResponse;
import com.minimq.protocol.message.ProtocolMessages.PartitionMetadata;
import com.minimq.protocol.message.ProtocolMessages.RegisterBrokerRequest;
import com.minimq.protocol.message.ProtocolMessages.TopicMetadata;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ClusterController {
    private final MetadataStore metadataStore;
    private ClusterMetadata metadata;

    public ClusterController(MetadataStore metadataStore) {
        this.metadataStore = metadataStore;
        this.metadata = metadataStore.load();
    }

    public synchronized ErrorCode registerBroker(RegisterBrokerRequest request) {
        BrokerEntry entry = metadata.brokers.computeIfAbsent(request.brokerId(), id -> new BrokerEntry());
        entry.brokerId = request.brokerId();
        entry.host = request.host();
        entry.port = request.port();
        entry.online = true;
        metadataStore.save(metadata);
        return ErrorCode.NONE;
    }

    public synchronized ErrorCode createTopic(CreateTopicRequest request) {
        if (request.topicName() == null || request.topicName().isBlank() || request.numPartitions() <= 0) {
            return ErrorCode.INVALID_REQUEST;
        }
        if (metadata.topics.containsKey(request.topicName())) {
            return ErrorCode.INVALID_REQUEST;
        }
        List<BrokerEntry> onlineBrokers = metadata.brokers.values().stream()
                .filter(b -> b.online)
                .sorted(Comparator.comparingInt(b -> b.brokerId))
                .collect(Collectors.toList());
        if (onlineBrokers.isEmpty()) {
            return ErrorCode.INVALID_REQUEST;
        }
        TopicEntry topic = new TopicEntry();
        topic.topicName = request.topicName();
        for (int i = 0; i < request.numPartitions(); i++) {
            PartitionEntry partition = new PartitionEntry();
            partition.partitionId = i;
            partition.leaderBrokerId = onlineBrokers.get(i % onlineBrokers.size()).brokerId;
            topic.partitions.add(partition);
        }
        metadata.topics.put(request.topicName(), topic);
        metadataStore.save(metadata);
        return ErrorCode.NONE;
    }

    public synchronized MetadataResponse metadata(MetadataRequest request) {
        List<BrokerMetadata> brokers = metadata.brokers.values().stream()
                .filter(b -> b.online)
                .sorted(Comparator.comparingInt(b -> b.brokerId))
                .map(b -> new BrokerMetadata(b.brokerId, b.host, b.port))
                .collect(Collectors.toList());

        List<TopicMetadata> topics = new ArrayList<>();
        for (TopicEntry topic : metadata.topics.values()) {
            if (!request.topicName().isEmpty() && !topic.topicName.equals(request.topicName())) {
                continue;
            }
            List<PartitionMetadata> partitions = topic.partitions.stream()
                    .map(p -> new PartitionMetadata(p.partitionId, p.leaderBrokerId))
                    .collect(Collectors.toList());
            topics.add(new TopicMetadata(topic.topicName, partitions));
        }
        return new MetadataResponse(brokers, topics);
    }

    public synchronized TopicEntry getTopic(String topicName) {
        TopicEntry topic = metadata.topics.get(topicName);
        return topic != null ? copyTopic(topic) : null;
    }

    public synchronized int getLeaderBrokerId(String topicName, int partitionId) {
        TopicEntry topic = metadata.topics.get(topicName);
        if (topic == null) {
            return -1;
        }
        return topic.partitions.stream()
                .filter(p -> p.partitionId == partitionId)
                .map(p -> p.leaderBrokerId)
                .findFirst()
                .orElse(-1);
    }

    public synchronized ClusterMetadata snapshot() {
        return copyMetadata(metadata);
    }

    public synchronized void replaceMetadata(ClusterMetadata metadata) {
        this.metadata = copyMetadata(metadata);
        metadataStore.save(this.metadata);
    }

    public synchronized <T> T updateMetadata(Function<ClusterMetadata, T> updater) {
        T result = updater.apply(metadata);
        metadataStore.save(metadata);
        return result;
    }

    private static ClusterMetadata copyMetadata(ClusterMetadata source) {
        ClusterMetadata copy = new ClusterMetadata();
        source.brokers.forEach((id, broker) -> copy.brokers.put(id, copyBroker(broker)));
        source.topics.forEach((name, topic) -> copy.topics.put(name, copyTopic(topic)));
        source.consumerGroups.forEach((id, group) -> copy.consumerGroups.put(id, copyConsumerGroup(group)));
        return copy;
    }

    private static BrokerEntry copyBroker(BrokerEntry source) {
        BrokerEntry copy = new BrokerEntry();
        copy.brokerId = source.brokerId;
        copy.host = source.host;
        copy.port = source.port;
        copy.online = source.online;
        return copy;
    }

    private static TopicEntry copyTopic(TopicEntry source) {
        TopicEntry copy = new TopicEntry();
        copy.topicName = source.topicName;
        for (PartitionEntry partition : source.partitions) {
            PartitionEntry partitionCopy = new PartitionEntry();
            partitionCopy.partitionId = partition.partitionId;
            partitionCopy.leaderBrokerId = partition.leaderBrokerId;
            copy.partitions.add(partitionCopy);
        }
        return copy;
    }

    private static ClusterMetadata.ConsumerGroupEntry copyConsumerGroup(
            ClusterMetadata.ConsumerGroupEntry source) {
        ClusterMetadata.ConsumerGroupEntry copy = new ClusterMetadata.ConsumerGroupEntry();
        copy.groupId = source.groupId;
        copy.topicName = source.topicName;
        copy.generationId = source.generationId;
        copy.leaderMemberId = source.leaderMemberId;
        copy.rebalancing = source.rebalancing;
        copy.committedOffsets.putAll(source.committedOffsets);
        source.members.forEach((id, member) -> copy.members.put(id, copyMember(member)));
        return copy;
    }

    private static ClusterMetadata.MemberEntry copyMember(ClusterMetadata.MemberEntry source) {
        ClusterMetadata.MemberEntry copy = new ClusterMetadata.MemberEntry();
        copy.memberId = source.memberId;
        copy.lastHeartbeatMs = source.lastHeartbeatMs;
        copy.assignedPartitions = new ArrayList<>(source.assignedPartitions);
        return copy;
    }
}
