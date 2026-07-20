package com.minimq.broker.replication;

import com.minimq.broker.PartitionManager;
import com.minimq.common.error.ErrorCode;

import java.util.Objects;

/** Applies the recovery cut before declaring a newly elected local Leader ready. */
public final class LeaderActivationService {
    private final PartitionManager partitions;
    private final LeaderEpochStateStore stateStore;

    public LeaderActivationService(PartitionManager partitions, LeaderEpochStateStore stateStore) {
        this.partitions = Objects.requireNonNull(partitions, "partitions");
        this.stateStore = Objects.requireNonNull(stateStore, "stateStore");
    }

    public synchronized ErrorCode activate(
            String topic, int partitionId, long leaderEpoch, long recoveryOffset) {
        var existing = stateStore.load(topic, partitionId);
        if (existing.isPresent() && existing.get().leaderEpoch() > leaderEpoch) {
            return ErrorCode.STALE_LEADER_EPOCH;
        }
        long localEndOffset = partitions.highWatermark(topic, partitionId);
        if (localEndOffset < recoveryOffset) {
            return ErrorCode.LEADER_NOT_READY;
        }
        if (existing.isPresent()
                && existing.get().leaderEpoch() == leaderEpoch
                && existing.get().recoveryOffset() == recoveryOffset
                && existing.get().ready()) {
            return ErrorCode.NONE;
        }
        stateStore.save(topic, partitionId,
                new LeaderEpochState(leaderEpoch, recoveryOffset, false));
        if (localEndOffset > recoveryOffset) {
            partitions.truncateTo(topic, partitionId, recoveryOffset);
        }
        stateStore.save(topic, partitionId,
                new LeaderEpochState(leaderEpoch, recoveryOffset, true));
        return ErrorCode.NONE;
    }
}
