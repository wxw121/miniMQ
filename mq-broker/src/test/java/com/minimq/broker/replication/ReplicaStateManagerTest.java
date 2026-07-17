package com.minimq.broker.replication;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ReplicaStateManagerTest {

    @Test
    void replicationHighWatermarkUsesMinimumIsrReplicaLeo() {
        ReplicaStateManager manager = new ReplicaStateManager();
        manager.updateReplicaLeo("orders", 0, 0, 5);
        manager.updateReplicaLeo("orders", 0, 1, 3);
        manager.updateReplicaLeo("orders", 0, 2, 4);

        long highWatermark = manager.replicationHighWatermark(
                "orders", 0, 5, List.of(0, 1, 2), 0);

        assertEquals(3, highWatermark);
    }
}
