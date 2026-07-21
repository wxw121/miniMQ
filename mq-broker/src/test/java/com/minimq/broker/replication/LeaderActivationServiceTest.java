package com.minimq.broker.replication;

import com.minimq.broker.PartitionManager;
import com.minimq.common.error.ErrorCode;
import com.minimq.storage.LogManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class LeaderActivationServiceTest {
    @TempDir Path tempDir;

    @Test
    void truncatesBeforePersistingReadyState() {
        PartitionManager partitions = new PartitionManager(new LogManager(tempDir.resolve("logs")));
        for (int i = 0; i < 6; i++) {
            partitions.append("orders", 0, null, new byte[]{(byte) i});
        }
        LeaderEpochStateStore store = new LeaderEpochStateStore(tempDir.resolve("epochs"));
        LeaderActivationService service = new LeaderActivationService(partitions, store);

        assertEquals(ErrorCode.NONE, service.activate("orders", 0, 4, 3));
        assertEquals(3, partitions.highWatermark("orders", 0));
        assertEquals(new LeaderEpochState(4, 3, true), store.load("orders", 0).orElseThrow());
        partitions.close();
    }

    @Test
    void refusesActivationWhenLocalLogHasARecoveryHole() {
        PartitionManager partitions = new PartitionManager(new LogManager(tempDir.resolve("short")));
        partitions.append("orders", 0, null, new byte[]{1});
        LeaderActivationService service = new LeaderActivationService(
                partitions, new LeaderEpochStateStore(tempDir.resolve("epochs")));
        assertEquals(ErrorCode.LEADER_NOT_READY, service.activate("orders", 0, 2, 5));
        partitions.close();
    }

    @Test
    void reusesPersistedFollowerTruncationProofDuringPromotion() {
        PartitionManager partitions = new PartitionManager(new LogManager(tempDir.resolve("promoted")));
        partitions.append("orders", 0, null, new byte[]{1});
        LeaderEpochStateStore store = new LeaderEpochStateStore(tempDir.resolve("promoted-epochs"));
        store.save("orders", 0, new LeaderEpochState(2, 1, true));
        LeaderActivationService service = new LeaderActivationService(partitions, store);

        assertEquals(ErrorCode.NONE, service.activate("orders", 0, 2, 1));
        assertEquals(new LeaderEpochState(2, 1, true), store.load("orders", 0).orElseThrow());
        partitions.close();
    }
}
