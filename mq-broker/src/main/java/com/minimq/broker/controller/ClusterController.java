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
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * 集群控制器，维护 Broker、Topic、分区 leader 和消费组元数据的一致视图。
 */
public class ClusterController {
    private final MetadataStore metadataStore;
    private ClusterMetadata metadata;

    public ClusterController(MetadataStore metadataStore) {
        this.metadataStore = metadataStore;
        this.metadata = metadataStore.load();
        normalizeMetadata();
    }

    // 控制器的公开方法统一加 synchronized，保证内存态元数据和落盘顺序一致。
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
        if (request.topicName() == null || request.topicName().isBlank()
                || request.numPartitions() <= 0 || request.replicationFactor() <= 0) {
            return ErrorCode.INVALID_REQUEST;
        }
        if (metadata.topics.containsKey(request.topicName())) {
            return ErrorCode.INVALID_REQUEST;
        }
        List<BrokerEntry> onlineBrokers = metadata.brokers.values().stream()
                .filter(b -> b.online)
                .sorted(Comparator.comparingInt(b -> b.brokerId))
                .collect(Collectors.toList());
        if (onlineBrokers.isEmpty() || request.replicationFactor() > onlineBrokers.size()) {
            return ErrorCode.INVALID_REQUEST;
        }
        TopicEntry topic = new TopicEntry();
        topic.topicName = request.topicName();
        for (int i = 0; i < request.numPartitions(); i++) {
            PartitionEntry partition = new PartitionEntry();
            partition.partitionId = i;
            int firstReplicaIndex = i % onlineBrokers.size();
            List<Integer> replicas = new ArrayList<>();
            for (int replica = 0; replica < request.replicationFactor(); replica++) {
                replicas.add(onlineBrokers.get((firstReplicaIndex + replica) % onlineBrokers.size()).brokerId);
            }
            partition.replicaBrokerIds = replicas;
            partition.isrBrokerIds = new ArrayList<>(replicas);
            partition.leaderBrokerId = replicas.get(0);
            topic.partitions.add(partition);
        }
        metadata.topics.put(request.topicName(), topic);
        metadataStore.save(metadata);
        return ErrorCode.NONE;
    }

    public synchronized ErrorCode markBrokerOffline(int brokerId) {
        BrokerEntry broker = metadata.brokers.get(brokerId);
        if (broker == null) {
            return ErrorCode.INVALID_REQUEST;
        }
        Map<PartitionEntry, Integer> replacementLeaders = new IdentityHashMap<>();
        for (TopicEntry topic : metadata.topics.values()) {
            for (PartitionEntry partition : topic.partitions) {
                normalizePartition(partition);
                if (partition.leaderBrokerId == brokerId) {
                    Integer replacementLeader = preferredSurvivingLeader(partition, brokerId);
                    if (replacementLeader == null) {
                        return ErrorCode.INVALID_REQUEST;
                    }
                    replacementLeaders.put(partition, replacementLeader);
                }
            }
        }
        broker.online = false;
        for (TopicEntry topic : metadata.topics.values()) {
            for (PartitionEntry partition : topic.partitions) {
                partition.isrBrokerIds.removeIf(id -> id == brokerId);
                Integer replacementLeader = replacementLeaders.get(partition);
                if (replacementLeader != null) {
                    partition.leaderBrokerId = replacementLeader;
                }
            }
        }
        metadataStore.save(metadata);
        return ErrorCode.NONE;
    }

    private Integer preferredSurvivingLeader(PartitionEntry partition, int offlineBrokerId) {
        for (int replicaId : partition.replicaBrokerIds) {
            BrokerEntry replica = metadata.brokers.get(replicaId);
            if (replicaId != offlineBrokerId
                    && partition.isrBrokerIds.contains(replicaId)
                    && replica != null
                    && replica.online) {
                return replicaId;
            }
        }
        return null;
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
                    .map(this::toPartitionMetadata)
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
        PartitionEntry partition = getPartitionEntry(topicName, partitionId);
        return partition != null ? partition.leaderBrokerId : -1;
    }

    public synchronized List<Integer> getReplicaBrokerIds(String topicName, int partitionId) {
        PartitionEntry partition = getPartitionEntry(topicName, partitionId);
        if (partition == null) {
            return List.of();
        }
        normalizePartition(partition);
        return List.copyOf(partition.replicaBrokerIds);
    }

    public synchronized List<Integer> getIsrBrokerIds(String topicName, int partitionId) {
        PartitionEntry partition = getPartitionEntry(topicName, partitionId);
        if (partition == null) {
            return List.of();
        }
        normalizePartition(partition);
        return List.copyOf(partition.isrBrokerIds);
    }

    public synchronized boolean isReplica(String topicName, int partitionId, int brokerId) {
        return getReplicaBrokerIds(topicName, partitionId).contains(brokerId);
    }

    public synchronized BrokerEntry getBroker(int brokerId) {
        BrokerEntry broker = metadata.brokers.get(brokerId);
        return broker != null ? copyBroker(broker) : null;
    }

    public synchronized ClusterMetadata snapshot() {
        return copyMetadata(metadata);
    }

    public synchronized void replaceMetadata(ClusterMetadata metadata) {
        this.metadata = copyMetadata(metadata);
        normalizeMetadata();
        metadataStore.save(this.metadata);
    }

    public synchronized <T> T updateMetadata(Function<ClusterMetadata, T> updater) {
        T result = updater.apply(metadata);
        normalizeMetadata();
        metadataStore.save(metadata);
        return result;
    }

    private PartitionEntry getPartitionEntry(String topicName, int partitionId) {
        TopicEntry topic = metadata.topics.get(topicName);
        if (topic == null) {
            return null;
        }
        return topic.partitions.stream()
                .filter(partition -> partition.partitionId == partitionId)
                .findFirst()
                .orElse(null);
    }

    private PartitionMetadata toPartitionMetadata(PartitionEntry partition) {
        normalizePartition(partition);
        return new PartitionMetadata(
                partition.partitionId,
                partition.leaderBrokerId,
                List.copyOf(partition.replicaBrokerIds),
                List.copyOf(partition.isrBrokerIds));
    }

    private void normalizeMetadata() {
        for (TopicEntry topic : metadata.topics.values()) {
            for (PartitionEntry partition : topic.partitions) {
                normalizePartition(partition);
            }
        }
    }

    private void normalizePartition(PartitionEntry partition) {
        if (partition.replicaBrokerIds == null) {
            partition.replicaBrokerIds = new ArrayList<>();
        }
        if (partition.isrBrokerIds == null) {
            partition.isrBrokerIds = new ArrayList<>();
        }
        if (partition.replicaBrokerIds.isEmpty()) {
            partition.replicaBrokerIds.add(partition.leaderBrokerId);
        }
        if (partition.isrBrokerIds.isEmpty()) {
            for (int replicaId : partition.replicaBrokerIds) {
                BrokerEntry replica = metadata.brokers.get(replicaId);
                if (replica != null && replica.online) {
                    partition.isrBrokerIds.add(replicaId);
                }
            }
            if (partition.isrBrokerIds.isEmpty()) {
                partition.isrBrokerIds.add(partition.leaderBrokerId);
            }
        }
        partition.isrBrokerIds.removeIf(replicaId -> {
            BrokerEntry replica = metadata.brokers.get(replicaId);
            return replica == null || !replica.online;
        });
        if (!partition.replicaBrokerIds.contains(partition.leaderBrokerId)) {
            partition.leaderBrokerId = partition.replicaBrokerIds.get(0);
        }
        if (!partition.isrBrokerIds.contains(partition.leaderBrokerId)) {
            partition.isrBrokerIds.add(0, partition.leaderBrokerId);
        }
    }

    // 返回快照时做深拷贝，避免调用方直接修改控制器内部状态。
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
            partitionCopy.replicaBrokerIds = new ArrayList<>(partition.replicaBrokerIds);
            partitionCopy.isrBrokerIds = new ArrayList<>(partition.isrBrokerIds);
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
