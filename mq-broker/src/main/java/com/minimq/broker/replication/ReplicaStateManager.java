package com.minimq.broker.replication;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tracks in-memory replica log-end offsets on the partition leader.
 */
public final class ReplicaStateManager {
    private final Map<String, Map<Integer, Long>> replicaLeos = new ConcurrentHashMap<>();
    private final Map<String, Integer> partitionLeaders = new ConcurrentHashMap<>();
    private final Map<String, Long> leaderLeoAtElection = new ConcurrentHashMap<>();

    public void updateReplicaLeo(String topicName, int partitionId, int brokerId, long logEndOffset) {
        if (logEndOffset < 0) {
            throw new IllegalArgumentException("logEndOffset must be non-negative");
        }
        replicaLeos
                .computeIfAbsent(partitionKey(topicName, partitionId), ignored -> new ConcurrentHashMap<>())
                .merge(brokerId, logEndOffset, Math::max);
    }

    /**
     * Records a leadership transition for the partition and resets follower LEO state.
     */
    public void noteLeadership(
            String topicName,
            int partitionId,
            int leaderBrokerId,
            long leaderLogEndOffset) {
        String key = partitionKey(topicName, partitionId);
        Integer previousLeader = partitionLeaders.get(key);
        if (previousLeader == null || previousLeader != leaderBrokerId) {
            leaderLeoAtElection.put(key, leaderLogEndOffset);
            replicaLeos.remove(key);
        }
        partitionLeaders.put(key, leaderBrokerId);
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
        String key = partitionKey(topicName, partitionId);
        Long leoAtElection = leaderLeoAtElection.get(key);
        boolean strictReplication = leoAtElection != null && leaderLogEndOffset > leoAtElection;

        long highWatermark = leaderLogEndOffset;
        for (int brokerId : isrBrokerIds) {
            if (brokerId == leaderBrokerId) {
                continue;
            }
            long followerLeo;
            if (hasReportedReplica(topicName, partitionId, brokerId)) {
                followerLeo = replicaLeo(topicName, partitionId, brokerId);
            } else if (strictReplication) {
                followerLeo = 0L;
            } else {
                followerLeo = leaderLogEndOffset;
            }
            highWatermark = Math.min(highWatermark, followerLeo);
        }

        if (leoAtElection != null
                && (strictReplication || allFollowersReported(topicName, partitionId, isrBrokerIds, leaderBrokerId))) {
            leaderLeoAtElection.remove(key);
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

    boolean hasReportedReplica(String topicName, int partitionId, int brokerId) {
        Map<Integer, Long> partitionReplicas = replicaLeos.get(partitionKey(topicName, partitionId));
        return partitionReplicas != null && partitionReplicas.containsKey(brokerId);
    }

    private boolean allFollowersReported(
            String topicName,
            int partitionId,
            List<Integer> isrBrokerIds,
            int leaderBrokerId) {
        for (int brokerId : isrBrokerIds) {
            if (brokerId == leaderBrokerId) {
                continue;
            }
            if (!hasReportedReplica(topicName, partitionId, brokerId)) {
                return false;
            }
        }
        return true;
    }

    private static String partitionKey(String topicName, int partitionId) {
        return topicName + "-" + partitionId;
    }
}
