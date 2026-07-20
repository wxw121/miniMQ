package com.minimq.broker.coordination;

import com.minimq.broker.support.EtcdTestContainer;
import io.etcd.jetcd.Client;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers(disabledWithoutDocker = false)
class EtcdControllerCoordinationStoreIT {
    @Container
    static final EtcdTestContainer ETCD = new EtcdTestContainer();

    @Test
    void epochIncreasesAcrossLeaseBoundLeadershipSessions() {
        try (Client client = ETCD.newClient()) {
            EtcdControllerCoordinationStore store =
                    new EtcdControllerCoordinationStore(client, "election-it");
            ControllerCoordinationStore.Leadership first = store.campaign(1, "host1", 9001, 3);
            assertEquals(1, first.identity().controllerEpoch());
            assertEquals(1, store.currentLeader().brokerId());
            first.close();
            assertNull(store.currentLeader());

            ControllerCoordinationStore.Leadership second = store.campaign(2, "host2", 9002, 3);
            assertEquals(2, second.identity().controllerEpoch());
            assertEquals(2, store.currentLeader().brokerId());
            store.close();
            assertFalse(second.valid());
        }
    }
}
