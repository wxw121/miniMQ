package com.minimq.broker.replication;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ReplicaStateManagerTest {

    @Test
    void replicationHighWatermarkUsesMinimumIsrReplicaLeo() {
        ReplicaStateManager manager = new ReplicaStateManager();
        manager.noteLeadership("orders", 0, 0, 5);
        manager.updateReplicaLeo("orders", 0, 0, 5);
        manager.updateReplicaLeo("orders", 0, 1, 3);
        manager.updateReplicaLeo("orders", 0, 2, 4);

        long highWatermark = manager.replicationHighWatermark(
                "orders", 0, 5, List.of(0, 1, 2), 0);

        assertEquals(3, highWatermark);
    }

    @Test
    void staleReplicaFetchOffsetDoesNotLowerHighWatermark() {
        ReplicaStateManager manager = new ReplicaStateManager();
        manager.noteLeadership("orders", 0, 0, 5);
        manager.updateReplicaLeo("orders", 0, 1, 5);
        manager.updateReplicaLeo("orders", 0, 1, 2);

        long highWatermark = manager.replicationHighWatermark(
                "orders", 0, 5, List.of(0, 1), 0);

        assertEquals(5, highWatermark);
    }

    @Test
    void unreportedFollowerDoesNotBlockReadsBeforeNewWritesAfterLeaderChange() {
        ReplicaStateManager manager = new ReplicaStateManager();
        manager.noteLeadership("orders", 0, 0, 3);

        long highWatermark = manager.replicationHighWatermark(
                "orders", 0, 3, List.of(0, 1, 2), 0);

        assertEquals(3, highWatermark);
    }

    @Test
    void newWritesRequireFollowerCatchUpAfterLeaderChange() {
        ReplicaStateManager manager = new ReplicaStateManager();
        manager.noteLeadership("orders", 0, 0, 3);
        manager.updateReplicaLeo("orders", 0, 1, 3);

        long highWatermark = manager.replicationHighWatermark(
                "orders", 0, 5, List.of(0, 1), 0);

        assertEquals(3, highWatermark);
    }

    @Test
    void newWritesBlockUnreportedFollowersAfterLeaderChange() {
        ReplicaStateManager manager = new ReplicaStateManager();
        manager.noteLeadership("orders", 0, 0, 3);

        long highWatermark = manager.replicationHighWatermark(
                "orders", 0, 5, List.of(0, 1), 0);

        assertEquals(0, highWatermark);
    }

    @Test
    void leadershipChangeResetsFollowerLeoState() {
        ReplicaStateManager manager = new ReplicaStateManager();
        manager.noteLeadership("orders", 0, 0, 3);
        manager.updateReplicaLeo("orders", 0, 1, 3);

        manager.noteLeadership("orders", 0, 1, 3);

        long highWatermark = manager.replicationHighWatermark(
                "orders", 0, 3, List.of(0, 1), 1);

        assertEquals(3, highWatermark);
    }
}
