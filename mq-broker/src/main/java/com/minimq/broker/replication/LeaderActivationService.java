package com.minimq.broker.replication;

import com.minimq.broker.PartitionManager;
import com.minimq.broker.audit.FailoverAuditEventType;
import com.minimq.broker.audit.FailoverAuditLog;
import com.minimq.common.error.ErrorCode;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/** Applies the recovery cut before declaring a newly elected local Leader ready. */
public final class LeaderActivationService {
    private final PartitionManager partitions;
    private final LeaderEpochStateStore stateStore;
    private final FailoverAuditLog auditLog;
    private final Set<String> notReadyEpochs = new HashSet<>();

    public LeaderActivationService(PartitionManager partitions, LeaderEpochStateStore stateStore) {
        this(partitions, stateStore, null);
    }

    public LeaderActivationService(
            PartitionManager partitions,
            LeaderEpochStateStore stateStore,
            FailoverAuditLog auditLog) {
        this.partitions = Objects.requireNonNull(partitions, "partitions");
        this.stateStore = Objects.requireNonNull(stateStore, "stateStore");
        this.auditLog = auditLog;
    }

    public synchronized ErrorCode activate(
            String topic, int partitionId, long leaderEpoch, long recoveryOffset) {
        var existing = stateStore.load(topic, partitionId);
        if (existing.isPresent() && existing.get().leaderEpoch() > leaderEpoch) {
            audit(
                    FailoverAuditEventType.STALE_EPOCH_REJECTED,
                    topic,
                    partitionId,
                    String.valueOf(leaderEpoch),
                    String.valueOf(existing.get().leaderEpoch()),
                    "local leader activation used a stale epoch");
            return ErrorCode.STALE_LEADER_EPOCH;
        }
        long localEndOffset = partitions.highWatermark(topic, partitionId);
        if (localEndOffset < recoveryOffset) {
            String epochKey = topic + "\u0000" + partitionId + "\u0000"
                    + leaderEpoch + "\u0000" + recoveryOffset;
            String partitionPrefix = topic + "\u0000" + partitionId + "\u0000";
            notReadyEpochs.removeIf(key -> key.startsWith(partitionPrefix) && !key.equals(epochKey));
            if (notReadyEpochs.add(epochKey)) {
                audit(
                        FailoverAuditEventType.LEADER_NOT_READY,
                        topic,
                        partitionId,
                        String.valueOf(localEndOffset),
                        String.valueOf(recoveryOffset),
                        "local log has not reached the recovery offset");
            }
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
        notReadyEpochs.removeIf(key -> key.startsWith(topic + "\u0000" + partitionId + "\u0000"));
        audit(
                FailoverAuditEventType.LEADER_READY,
                topic,
                partitionId,
                "false",
                "true",
                "local leader activation completed at offset " + recoveryOffset);
        return ErrorCode.NONE;
    }

    private void audit(
            FailoverAuditEventType eventType,
            String topic,
            int partitionId,
            String oldValue,
            String newValue,
            String detail) {
        if (auditLog == null) {
            return;
        }
        try {
            auditLog.append(eventType, null, topic, partitionId, oldValue, newValue, detail);
        } catch (RuntimeException ignored) {
            // A diagnostic append must never change leader activation semantics.
        }
    }
}
