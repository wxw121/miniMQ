package com.minimq.broker.replication;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.IntSupplier;
import java.util.function.Supplier;

/**
 * Tracks in-memory replica log-end offsets on the partition leader.
 */
public final class ReplicaStateManager {
    private static final long WAIT_SLICE_MS = 50L;

    private final Map<String, Map<Integer, Long>> replicaLeos = new ConcurrentHashMap<>();
    private final Map<String, Integer> partitionLeaders = new ConcurrentHashMap<>();
    private final Map<String, Long> leaderLeoAtElection = new ConcurrentHashMap<>();

    public enum ReplicationWaitResult {
        REPLICATED,
        TIMED_OUT,
        NOT_LEADER,
        NOT_ENOUGH_REPLICAS
    }

    public void updateReplicaLeo(String topicName, int partitionId, int brokerId, long logEndOffset) {
        if (logEndOffset < 0) {
            throw new IllegalArgumentException("logEndOffset must be non-negative");
        }
        synchronized (this) {
            replicaLeos
                    .computeIfAbsent(partitionKey(topicName, partitionId), ignored -> new ConcurrentHashMap<>())
                    .merge(brokerId, logEndOffset, Math::max);
            notifyAll();
        }
    }

    /**
     * Records a leadership transition for the partition and resets follower LEO state.
     */
    public synchronized void noteLeadership(
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
        notifyAll();
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

        // Clear the election marker only after every follower has reported. Clearing it on the
        // first strict (post-election write) computation would treat later unreported followers
        // as caught up and make acks=ALL return success before the ISR has the record.
        if (leoAtElection != null
                && allFollowersReported(topicName, partitionId, isrBrokerIds, leaderBrokerId)) {
            leaderLeoAtElection.remove(key);
        }
        return highWatermark;
    }

    /**
     * Blocks until the current ISR high watermark is at least {@code requiredHighWatermark},
     * or {@code timeoutMs} elapses. Returns {@code false} on timeout or interrupt.
     */
    public boolean awaitReplicationHighWatermark(
            String topicName,
            int partitionId,
            long requiredHighWatermark,
            List<Integer> isrBrokerIds,
            int leaderBrokerId,
            long timeoutMs) {
        return awaitReplicationHighWatermark(
                topicName,
                partitionId,
                requiredHighWatermark,
                leaderBrokerId,
                timeoutMs,
                () -> leaderBrokerId,
                () -> isrBrokerIds,
                1) == ReplicationWaitResult.REPLICATED;
    }

    /**
     * Waits for the live ISR high watermark, aborting if this node loses leadership or the
     * current ISR drops below {@code minInSyncReplicas}. ISR changes are rechecked every
     * {@value #WAIT_SLICE_MS} ms because metadata updates do not notify this monitor.
     */
    public ReplicationWaitResult awaitReplicationHighWatermark(
            String topicName,
            int partitionId,
            long requiredHighWatermark,
            int expectedLeaderBrokerId,
            long timeoutMs,
            IntSupplier currentLeaderId,
            Supplier<List<Integer>> currentIsr,
            int minInSyncReplicas) {
        if (requiredHighWatermark < 0) {
            throw new IllegalArgumentException("requiredHighWatermark must be non-negative");
        }
        if (timeoutMs < 0) {
            throw new IllegalArgumentException("timeoutMs must be non-negative");
        }
        long deadline = System.currentTimeMillis() + timeoutMs;
        synchronized (this) {
            while (true) {
                if (currentLeaderId.getAsInt() != expectedLeaderBrokerId) {
                    return ReplicationWaitResult.NOT_LEADER;
                }
                Integer recordedLeader = partitionLeaders.get(partitionKey(topicName, partitionId));
                if (recordedLeader != null && recordedLeader != expectedLeaderBrokerId) {
                    return ReplicationWaitResult.NOT_LEADER;
                }
                List<Integer> isrBrokerIds = currentIsr.get();
                if (isrBrokerIds == null || isrBrokerIds.isEmpty() || isrBrokerIds.size() < minInSyncReplicas) {
                    return ReplicationWaitResult.NOT_ENOUGH_REPLICAS;
                }
                long highWatermark = replicationHighWatermark(
                        topicName,
                        partitionId,
                        requiredHighWatermark,
                        isrBrokerIds,
                        expectedLeaderBrokerId);
                if (highWatermark >= requiredHighWatermark) {
                    return ReplicationWaitResult.REPLICATED;
                }
                long remaining = deadline - System.currentTimeMillis();
                if (remaining <= 0) {
                    return ReplicationWaitResult.TIMED_OUT;
                }
                try {
                    wait(Math.min(WAIT_SLICE_MS, remaining));
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    return ReplicationWaitResult.TIMED_OUT;
                }
            }
        }
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
