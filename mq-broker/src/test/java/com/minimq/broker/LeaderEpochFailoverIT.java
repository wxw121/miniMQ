package com.minimq.broker;

import com.minimq.broker.coordination.ControllerCoordinationStore;
import com.minimq.broker.coordination.EtcdControllerCoordinationStore;
import com.minimq.broker.metadata.ClusterMetadata;
import com.minimq.broker.metadata.EtcdClusterMetadataRepository;
import com.minimq.broker.support.EtcdTestContainer;
import com.minimq.common.error.ErrorCode;
import io.etcd.jetcd.Client;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Testcontainers(disabledWithoutDocker = false)
class LeaderEpochFailoverIT {
    @Container
    static final EtcdTestContainer ETCD = new EtcdTestContainer();

    @Test
    void staleControllerCannotPublishLeaderEpochAfterTakeover() {
        try (Client client = ETCD.newClient()) {
            EtcdControllerCoordinationStore coordination =
                    new EtcdControllerCoordinationStore(client, "leader-epoch-failover-it");
            ControllerCoordinationStore.Leadership first =
                    coordination.campaign(0, "host0", 19090, 3);
            EtcdClusterMetadataRepository stale = new EtcdClusterMetadataRepository(
                    client.getKVClient(), "leader-epoch-failover-it", first);
            ClusterMetadata initial = new ClusterMetadata();
            initial.controllerEpoch = first.identity().controllerEpoch();
            var initialized = stale.initialize(initial);
            assertEquals(ErrorCode.NONE, initialized.errorCode());

            first.close();
            ControllerCoordinationStore.Leadership second =
                    coordination.campaign(1, "host1", 19091, 3);
            EtcdClusterMetadataRepository active = new EtcdClusterMetadataRepository(
                    client.getKVClient(), "leader-epoch-failover-it", second);
            ClusterMetadata next = active.read().metadata();
            next.controllerEpoch = second.identity().controllerEpoch();
            assertEquals(ErrorCode.NONE,
                    active.compareAndSet(active.read().revision(), next).errorCode());
            assertEquals(ErrorCode.CONTROLLER_FENCED,
                    stale.compareAndSet(active.read().revision(), next).errorCode());
            coordination.close();
        }
    }
}
