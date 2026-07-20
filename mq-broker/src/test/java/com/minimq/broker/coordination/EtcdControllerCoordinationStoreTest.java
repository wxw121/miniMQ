package com.minimq.broker.coordination;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class EtcdControllerCoordinationStoreTest {
    @Test
    void identityRequiresStableSessionAndValidEndpoint() {
        assertThrows(IllegalArgumentException.class,
                () -> new ControllerIdentity(1, "", 9001, 1, "s"));
        assertThrows(IllegalArgumentException.class,
                () -> new ControllerIdentity(1, "host", 0, 1, "s"));
        assertThrows(IllegalArgumentException.class,
                () -> new ControllerIdentity(1, "host", 9001, -1, "s"));
        assertEquals("/minimq/c/controller/leader",
                EtcdControllerCoordinationStore.leaderKey("c"));
    }
}
