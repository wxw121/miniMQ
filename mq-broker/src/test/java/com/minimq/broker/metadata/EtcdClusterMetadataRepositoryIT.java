package com.minimq.broker.metadata;

import com.minimq.broker.coordination.ControllerCoordinationStore;
import com.minimq.broker.coordination.EtcdControllerCoordinationStore;
import com.minimq.broker.support.EtcdTestContainer;
import com.minimq.common.error.ErrorCode;
import io.etcd.jetcd.Client;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers(disabledWithoutDocker = false)
class EtcdClusterMetadataRepositoryIT {
    @Container
    static final EtcdTestContainer ETCD = new EtcdTestContainer();

    @Test
    void rejectsOldIdentityAndWrongSnapshotRevision() {
        try (Client client = ETCD.newClient()) {
            EtcdControllerCoordinationStore coordination =
                    new EtcdControllerCoordinationStore(client, "metadata-it");
            ControllerCoordinationStore.Leadership first = coordination.campaign(1, "host1", 9001, 3);
            EtcdClusterMetadataRepository stale =
                    new EtcdClusterMetadataRepository(client.getKVClient(), "metadata-it", first);
            ClusterMetadata initial = new ClusterMetadata();
            initial.controllerEpoch = first.identity().controllerEpoch();
            ClusterMetadataRepository.UpdateResult initialized = stale.initialize(initial);
            assertEquals(ErrorCode.NONE, initialized.errorCode());
            long revision = initialized.value().revision();

            first.close();
            ControllerCoordinationStore.Leadership second = coordination.campaign(2, "host2", 9002, 3);
            EtcdClusterMetadataRepository current =
                    new EtcdClusterMetadataRepository(client.getKVClient(), "metadata-it", second);

            assertEquals(ErrorCode.CONTROLLER_FENCED,
                    stale.compareAndSet(revision, initial).errorCode());
            assertEquals(ErrorCode.CONTROLLER_FENCED,
                    current.compareAndSet(revision + 99, initial).errorCode());

            ClusterMetadata next = current.read().metadata();
            next.controllerEpoch = second.identity().controllerEpoch();
            assertEquals(ErrorCode.NONE,
                    current.compareAndSet(revision, next).errorCode());
            coordination.close();
        }
    }
}
