package com.minimq.broker.replication;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tracks in-memory replica log-end offsets on the partition leader.
 */
public final class ReplicaStateManager {
    private final Map<String, Map<Integer, Long>> replicaLeos = new ConcurrentHashMap<>();

    public void updateReplicaLeo(String topicName, int partitionId, int brokerId, long logEndOffset) {
        if (logEndOffset < 0) {
            throw new IllegalArgumentException("logEndOffset must be non-negative");
        }
        replicaLeos
                .computeIfAbsent(partitionKey(topicName, partitionId), ignored -> new ConcurrentHashMap<>())
                .put(brokerId, logEndOffset);
    }

    public long replicationHighWatermark(
            String topicName,
            int partitionId,
            long leaderLogEndOffset,
            List<Integer> isrBrokerIds,
            int leaderBrokerId) {
        if (isrBrokerIds == null || isrBrokerIds.isEmpty()) {
            return leaderLogEndOffset;
        }
        long highWatermark = leaderLogEndOffset;
        for (int brokerId : isrBrokerIds) {
            long replicaLeo = brokerId == leaderBrokerId
                    ? leaderLogEndOffset
                    : replicaLeo(topicName, partitionId, brokerId);
            highWatermark = Math.min(highWatermark, replicaLeo);
        }
        return highWatermark;
    }

    public long replicaLeo(String topicName, int partitionId, int brokerId) {
        Map<Integer, Long> partitionReplicas = replicaLeos.get(partitionKey(topicName, partitionId));
        if (partitionReplicas == null) {
            return 0;
        }
        return partitionReplicas.getOrDefault(brokerId, 0L);
    }

    private static String partitionKey(String topicName, int partitionId) {
        return topicName + "-" + partitionId;
    }
}
