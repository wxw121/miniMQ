package com.minimq.broker.replication;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

    @Test
    void repeatedHighWatermarkStillBlocksUnreportedFollowerAfterNewWrites() {
        ReplicaStateManager manager = new ReplicaStateManager();
        manager.noteLeadership("orders", 0, 0, 3);

        assertEquals(0, manager.replicationHighWatermark("orders", 0, 5, List.of(0, 1), 0));
        assertEquals(0, manager.replicationHighWatermark("orders", 0, 5, List.of(0, 1), 0));
    }

    @Test
    void awaitReturnsImmediatelyWhenIsrAlreadyCaughtUp() {
        ReplicaStateManager manager = new ReplicaStateManager();
        manager.noteLeadership("orders", 0, 0, 5);
        manager.updateReplicaLeo("orders", 0, 1, 5);

        assertTrue(manager.awaitReplicationHighWatermark("orders", 0, 5, List.of(0, 1), 0, 50));
    }

    @Test
    void awaitTimesOutWhenFollowerStaysBehind() {
        ReplicaStateManager manager = new ReplicaStateManager();
        manager.noteLeadership("orders", 0, 0, 3);
        manager.updateReplicaLeo("orders", 0, 1, 3);

        assertFalse(manager.awaitReplicationHighWatermark("orders", 0, 5, List.of(0, 1), 0, 20));
    }

    @Test
    void awaitSucceedsWhenFollowerCatchesUp() throws Exception {
        ReplicaStateManager manager = new ReplicaStateManager();
        manager.noteLeadership("orders", 0, 0, 3);
        manager.updateReplicaLeo("orders", 0, 1, 3);
        CountDownLatch started = new CountDownLatch(1);
        AtomicBoolean replicated = new AtomicBoolean(false);
        Thread waiter = new Thread(() -> {
            started.countDown();
            replicated.set(manager.awaitReplicationHighWatermark("orders", 0, 5, List.of(0, 1), 0, 1_000));
        });
        waiter.start();
        assertTrue(started.await(1, TimeUnit.SECONDS));
        Thread.sleep(20);
        manager.updateReplicaLeo("orders", 0, 1, 5);
        waiter.join(1_000);
        assertTrue(replicated.get());
    }
}
