package com.minimq.broker.replication;

public record LeaderEpochState(long leaderEpoch, long recoveryOffset, boolean ready) {
    public LeaderEpochState {
        if (leaderEpoch < 0 || recoveryOffset < 0) {
            throw new IllegalArgumentException("leader epoch state must be non-negative");
        }
    }
}
