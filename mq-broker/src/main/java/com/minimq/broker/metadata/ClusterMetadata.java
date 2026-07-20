package com.minimq.broker.metadata;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 可持久化的集群元数据模型，描述 Broker、Topic、分区和消费组状态。
 */
public class ClusterMetadata {
    public int metadataFormatVersion = 3;
    public long controllerEpoch;
    public long metadataVersion;
    public final Map<Integer, BrokerEntry> brokers = new HashMap<>();
    public final Map<String, TopicEntry> topics = new HashMap<>();
    public final Map<String, ConsumerGroupEntry> consumerGroups = new HashMap<>();

    public static class BrokerEntry {
        public int brokerId;
        public String host;
        public int port;
        public boolean online;
        public long brokerEpoch;
    }

    public static class TopicEntry {
        public String topicName;
        public List<PartitionEntry> partitions = new ArrayList<>();
    }

    public static class PartitionEntry {
        public int partitionId;
        public int leaderBrokerId;
        public List<Integer> replicaBrokerIds = new ArrayList<>();
        public List<Integer> isrBrokerIds = new ArrayList<>();
        public Map<Integer, Long> replicaLogEndOffsets = new HashMap<>();
        public boolean frozen;
        public long leaderEpoch;
        public long leaderEpochStartOffset;
        public boolean leaderReady = true;
    }

    public static class ConsumerGroupEntry {
        public String groupId;
        public String topicName;
        public int generationId;
        public String leaderMemberId;
        public Map<String, MemberEntry> members = new HashMap<>();
        public Map<String, Long> committedOffsets = new HashMap<>();
        public boolean rebalancing;
    }

    public static class MemberEntry {
        public String memberId;
        public long lastHeartbeatMs;
        public List<Integer> assignedPartitions = new ArrayList<>();
    }
}
