package com.minimq.broker.replication;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class LeaderEpochStateStoreTest {
    @TempDir Path tempDir;

    @Test
    void persistsEpochAndRejectsOlderState() {
        LeaderEpochStateStore store = new LeaderEpochStateStore(tempDir);
        LeaderEpochState state = new LeaderEpochState(3, 8, false);
        store.save("orders", 0, state);
        assertEquals(state, new LeaderEpochStateStore(tempDir).load("orders", 0).orElseThrow());
        assertThrows(IllegalStateException.class,
                () -> store.save("orders", 0, new LeaderEpochState(2, 7, true)));
        store.save("orders", 0, state);
    }
}
