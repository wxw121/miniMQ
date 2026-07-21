package com.minimq.broker;

import com.minimq.broker.controller.ClusterController;
import com.minimq.broker.coordination.ControllerCoordinationStore;
import com.minimq.broker.coordination.ControllerElectionService;
import com.minimq.broker.coordination.ControllerIdentity;
import com.minimq.broker.coordination.ControllerState;
import com.minimq.broker.coordination.EtcdControllerCoordinationStore;
import com.minimq.broker.metadata.ClusterMetadata;
import com.minimq.broker.metadata.ClusterMetadataRepository;
import com.minimq.broker.metadata.MetadataWatchService;
import com.minimq.broker.metadata.SwitchableClusterMetadataRepository;
import com.minimq.broker.metadata.VersionedClusterMetadata;
import com.minimq.common.error.ErrorCode;
import io.etcd.jetcd.Client;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.inOrder;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class ControllerRuntimeTest {
    @Test
    void activatesAgainstExistingSnapshotAndDelegatesRole() {
        Fixture f = new Fixture(false);
        VersionedClusterMetadata shared = version(8);
        when(f.fenced.read()).thenReturn(shared);
        when(f.election.state()).thenReturn(ControllerState.ACTIVE);
        ControllerIdentity identity = new ControllerIdentity(1, "host", 9092, 4, "session");
        when(f.leadership.identity()).thenReturn(identity);
        when(f.coordination.currentLeader()).thenReturn(identity);

        f.runtime.campaignOnce();

        assertTrue(f.runtime.isActive());
        assertEquals(identity, f.runtime.currentLeader());
        assertEquals(shared, f.switchable.read());
        assertEquals(1, f.activations.get());
        verify(f.controller).installMetadata(shared);
        verify(f.controller).beginRecovery(f.config.controllerFailoverGraceMs());
        verify(f.election).activate();
        verify(f.fenced, never()).initialize(any());
    }

    @Test
    void initializesEmptySnapshotOnlyWithExplicitFlag() {
        Fixture f = new Fixture(true);
        when(f.fenced.read()).thenReturn(version(0));
        when(f.fenced.initialize(any())).thenReturn(
                new ClusterMetadataRepository.UpdateResult(ErrorCode.NONE, version(1)));

        f.runtime.campaignOnce();

        verify(f.fenced).initialize(any());
        assertEquals(1, f.activations.get());

        Fixture missingFlag = new Fixture(false);
        when(missingFlag.fenced.read()).thenReturn(version(0));
        IllegalStateException error = assertThrows(
                IllegalStateException.class, missingFlag.runtime::campaignOnce);
        assertTrue(error.getMessage().contains("explicit cluster initialization"));

        Fixture failed = new Fixture(true);
        when(failed.fenced.read()).thenReturn(version(0));
        when(failed.fenced.initialize(any())).thenReturn(
                new ClusterMetadataRepository.UpdateResult(ErrorCode.CONTROLLER_FENCED, version(0)));
        assertThrows(IllegalStateException.class, failed.runtime::campaignOnce);
    }

    @Test
    void startsWatchAndClosesEveryOwnedResourceOnce() {
        Fixture f = new Fixture(false);
        java.util.concurrent.CountDownLatch attempts = new java.util.concurrent.CountDownLatch(2);
        when(f.election.campaign()).thenAnswer(ignored -> {
            attempts.countDown();
            throw new IllegalStateException("etcd unavailable");
        });

        f.runtime.start();
        assertDoesNotThrow(() -> assertTrue(
                attempts.await(2, java.util.concurrent.TimeUnit.SECONDS)));
        f.runtime.close();
        f.runtime.close();

        verify(f.watch).start();
        verify(f.election, atLeastOnce()).close();
        verify(f.watch).close();
        verify(f.coordination).close();
        verify(f.client).close();
        assertFalse(f.runtime.isActive());
    }

    @Test
    void productionCompositionCreatesAndClosesJetcdResources() {
        BrokerConfig config = BrokerConfig.parse(
                new String[] {"--admin-token", "test-token"});
        ClusterMetadataRepository initial = mock(ClusterMetadataRepository.class);
        SwitchableClusterMetadataRepository switchable =
                new SwitchableClusterMetadataRepository(initial);
        ClusterController controller = mock(ClusterController.class);

        ControllerRuntime runtime = new ControllerRuntime(
                config, controller, switchable, () -> { });

        assertFalse(runtime.isActive());
        runtime.close();
    }

    @Test
    void activeCampaignThreadStopsWhenRuntimeCloses() throws Exception {
        Fixture f = new Fixture(false);
        when(f.fenced.read()).thenReturn(version(5));
        when(f.election.state()).thenReturn(ControllerState.ACTIVE);

        f.runtime.start();
        long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(2);
        while (f.activations.get() == 0 && System.nanoTime() < deadline) {
            Thread.sleep(1);
        }
        assertEquals(1, f.activations.get());

        f.runtime.close();
        assertFalse(f.runtime.isActive());
    }

    @Test
    void closePreservesCallerInterruption() {
        Fixture f = new Fixture(false);
        Thread.currentThread().interrupt();
        try {
            f.runtime.close();
            assertTrue(Thread.currentThread().isInterrupted());
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    void closesClientBeforeElectionSoBlockingCampaignCanUnblock() {
        Fixture f = new Fixture(false);

        f.runtime.close();

        var order = inOrder(f.client, f.election);
        order.verify(f.client).close();
        order.verify(f.election).close();
    }

    private static VersionedClusterMetadata version(long revision) {
        ClusterMetadata metadata = new ClusterMetadata();
        metadata.metadataVersion = revision;
        return new VersionedClusterMetadata(metadata, revision);
    }

    private static final class Fixture {
        final BrokerConfig config;
        final ClusterController controller = mock(ClusterController.class);
        final ClusterMetadataRepository initial = mock(ClusterMetadataRepository.class);
        final ClusterMetadataRepository fenced = mock(ClusterMetadataRepository.class);
        final SwitchableClusterMetadataRepository switchable =
                new SwitchableClusterMetadataRepository(initial);
        final Client client = mock(Client.class);
        final EtcdControllerCoordinationStore coordination = mock(EtcdControllerCoordinationStore.class);
        final ControllerElectionService election = mock(ControllerElectionService.class);
        final MetadataWatchService watch = mock(MetadataWatchService.class);
        final ControllerCoordinationStore.Leadership leadership =
                mock(ControllerCoordinationStore.Leadership.class);
        final AtomicInteger activations = new AtomicInteger();
        final ControllerRuntime runtime;

        Fixture(boolean initialize) {
            config = BrokerConfig.parse(initialize
                    ? new String[] {"--admin-token", "test-token", "--initialize-empty-cluster"}
                    : new String[] {"--admin-token", "test-token"});
            when(election.campaign()).thenReturn(leadership);
            when(leadership.identity()).thenReturn(
                    new ControllerIdentity(0, "localhost", 9092, 1, "session"));
            when(initial.read()).thenReturn(version(0));
            when(controller.updateMetadata(any())).thenAnswer(invocation -> {
                java.util.function.Function<ClusterMetadata, ?> updater = invocation.getArgument(0);
                return updater.apply(new ClusterMetadata());
            });
            runtime = new ControllerRuntime(
                    config, controller, switchable, client, coordination, election, watch,
                    ignored -> fenced, activations::incrementAndGet);
        }
    }
}
