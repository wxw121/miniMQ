package com.minimq.broker.metadata;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * 可持久化的集群元数据模型，描述 Broker、Topic、分区和消费组状态。
 */
public class ClusterMetadata {
    public static final int CURRENT_FORMAT_VERSION = 4;

    public int metadataFormatVersion = CURRENT_FORMAT_VERSION;
    public long controllerEpoch;
    public long metadataVersion;
    public final Map<Integer, BrokerEntry> brokers = new HashMap<>();
    public final Map<String, TopicEntry> topics = new HashMap<>();
    /**
     * Deleted topic names remain reserved in this map.  Keeping tombstones in
     * the same CAS document as active topics prevents a stale broker from
     * recreating a name after it rejoins the cluster.
     */
    public final Map<String, TopicTombstone> topicTombstones = new HashMap<>();
    /** Explicit V4 reliability links; names carry no implicit semantics. */
    public final Map<String, String> retryTopicBySource = new HashMap<>();
    public final Map<String, String> deadLetterTopicBySource = new HashMap<>();
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
        /** 0 means missing/legacy metadata; treat as {@code min.insync.replicas=1}. */
        public int minInSyncReplicas;

        public int effectiveMinInSyncReplicas() {
            return minInSyncReplicas <= 0 ? 1 : minInSyncReplicas;
        }
    }

    public static class TopicTombstone {
        public String topicName;
        public String state = "DELETING";
        public long tombstoneVersion;
        public long deletedAtEpochMs;
        public Set<Integer> requiredBrokerIds = new TreeSet<>();
        public Set<Integer> acknowledgedBrokerIds = new TreeSet<>();
    }

    public static class PartitionEntry {
        public int partitionId;
        public int leaderBrokerId;
        public List<Integer> replicaBrokerIds = new ArrayList<>();
        public List<Integer> isrBrokerIds = new ArrayList<>();
        public Map<Integer, Long> replicaLogEndOffsets = new HashMap<>();
        public Map<Integer, ReplicaProgressEntry> replicaProgress = new HashMap<>();
        public WatermarkSnapshot lastCompleteWatermark = new WatermarkSnapshot();
        public boolean frozen;
        /**
         * True when {@link #frozen} was set because Leader activation timed out.
         * Restore must keep {@link #leaderReady} false so truncation runs again.
         * No-ISR lease-expiry freeze leaves this false: the original ready Leader
         * may return with the same data directory and become ready immediately.
         */
        public boolean activationRequired;
        public long leaderEpoch;
        public long leaderEpochStartOffset;
        public boolean leaderReady = true;
    }

    public static class ReplicaProgressEntry {
        public int brokerId;
        public long physicalLogEndOffset;
        public long visibleLogEndOffset;
        public long nextVisibleAtEpochMs = -1L;
        public long brokerEpoch;
        public long controllerEpoch;
        public long leaderEpoch;
    }

    public static class WatermarkSnapshot {
        public long physicalHighWatermark = -1L;
        public long visibleHighWatermark = -1L;
        public long nextVisibleAtEpochMs = -1L;
        public long controllerEpoch = -1L;
        public long leaderEpoch = -1L;
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
        /** 0 means missing/legacy metadata; treat as the 10s default session timeout. */
        public int sessionTimeoutMs;

        public int effectiveSessionTimeoutMs() {
            return sessionTimeoutMs <= 0 ? 10_000 : sessionTimeoutMs;
        }
    }
}
