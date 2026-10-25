package com.minimq.broker.admin;

import com.minimq.broker.BrokerAvailabilityGuard;
import com.minimq.broker.BrokerConfig;
import com.minimq.broker.BrokerLeaseService;
import com.minimq.broker.ControllerRole;
import com.minimq.broker.PartitionManager;
import com.minimq.broker.controller.ClusterController;
import com.minimq.broker.metadata.ClusterMetadata;
import com.minimq.broker.metadata.ClusterMetadataRepository;
import com.minimq.broker.metadata.MetadataStore;
import com.minimq.broker.metadata.VersionedClusterMetadata;
import com.minimq.common.error.ErrorCode;
import com.minimq.protocol.message.ProtocolMessages.CreateTopicRequest;
import com.minimq.protocol.message.ProtocolMessages.RegisterBrokerRequest;
import com.minimq.storage.LogManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TopicDeletionReconcilerTest {

    @TempDir
    Path tempDir;

    @Test
    void skipsTombstoneAlreadyAcknowledgedByThisBroker() {
        BrokerConfig config = new BrokerConfig(
                0, "127.0.0.1", 19092, tempDir.resolve("broker"),
                true, "127.0.0.1", 19092, false, "cluster-token");
        ClusterController controller = new ClusterController(
                new MetadataStore(tempDir.resolve("controller")));
        assertEquals(ErrorCode.NONE, controller.registerBroker(
                new RegisterBrokerRequest(0, "127.0.0.1", 19092)).errorCode());
        assertEquals(ErrorCode.NONE,
                controller.createTopic(new CreateTopicRequest("orders", 1, 1)));

        CountingPartitionManager partitions = new CountingPartitionManager(
                new LogManager(tempDir.resolve("broker-data")));
        partitions.append(
                "orders", 0,
                "key".getBytes(StandardCharsets.UTF_8),
                "value".getBytes(StandardCharsets.UTF_8));
        assertEquals(ErrorCode.NONE, controller.deleteTopic("orders", 1_000L));

        try (TopicDeletionReconciler reconciler = new TopicDeletionReconciler(
                config,
                controller,
                partitions,
                new BrokerAvailabilityGuard(),
                ControllerRole.legacy(config))) {
            assertFalse(reconciler.isCaughtUp());
            assertEquals(1, reconciler.reconcileOnce());
            assertTrue(reconciler.isCaughtUp());
            assertEquals(0, reconciler.reconcileOnce());
            assertEquals(1, partitions.deleteCalls);
        } finally {
            partitions.close();
        }
    }

    @Test
    void preAcknowledgedTombstoneStillRequiresLocalCleanupBeforeCaughtUp() {
        BrokerConfig config = new BrokerConfig(
                0, "127.0.0.1", 19092, tempDir.resolve("restored-broker"),
                true, "127.0.0.1", 19092, false, "cluster-token");
        ClusterController controller = new ClusterController(
                new MetadataStore(tempDir.resolve("restored-controller")));
        assertEquals(ErrorCode.NONE, controller.registerBroker(
                new RegisterBrokerRequest(0, "127.0.0.1", 19092)).errorCode());
        assertEquals(ErrorCode.NONE,
                controller.createTopic(new CreateTopicRequest("orders", 1, 1)));

        CountingPartitionManager partitions = new CountingPartitionManager(
                new LogManager(tempDir.resolve("restored-data")));
        partitions.append(
                "orders", 0,
                "key".getBytes(StandardCharsets.UTF_8),
                "value".getBytes(StandardCharsets.UTF_8));
        assertEquals(ErrorCode.NONE, controller.deleteTopic("orders", 1_000L));
        ClusterMetadata.BrokerEntry broker = controller.getBroker(0);
        ClusterMetadata.TopicTombstone tombstone = controller.getTopicTombstone("orders");
        assertEquals(ErrorCode.NONE, controller.acknowledgeTopicDeletion(
                "orders",
                tombstone.tombstoneVersion,
                0,
                broker.brokerEpoch,
                controller.snapshot().controllerEpoch));

        try (TopicDeletionReconciler reconciler = new TopicDeletionReconciler(
                config,
                controller,
                partitions,
                new BrokerAvailabilityGuard(),
                ControllerRole.legacy(config))) {
            assertFalse(reconciler.isCaughtUp());
            assertEquals(0, reconciler.reconcileOnce());
            assertEquals(1, partitions.deleteCalls);
            assertTrue(reconciler.isCaughtUp());
            assertEquals(0, reconciler.reconcileOnce());
            assertEquals(1, partitions.deleteCalls);
        } finally {
            partitions.close();
        }
    }

    @Test
    void localCleanupFailureCreatesStickyStorageFence() {
        BrokerConfig config = new BrokerConfig(
                0, "127.0.0.1", 19092, tempDir.resolve("failed-broker"),
                true, "127.0.0.1", 19092, false, "cluster-token");
        ClusterController controller = new ClusterController(
                new MetadataStore(tempDir.resolve("failed-controller")));
        assertEquals(ErrorCode.NONE, controller.registerBroker(
                new RegisterBrokerRequest(0, "127.0.0.1", 19092)).errorCode());
        assertEquals(ErrorCode.NONE,
                controller.createTopic(new CreateTopicRequest("orders", 1, 1)));
        assertEquals(ErrorCode.NONE, controller.deleteTopic("orders", 1_000L));
        BrokerAvailabilityGuard guard = new BrokerAvailabilityGuard();
        guard.activate();
        FailingPartitionManager partitions = new FailingPartitionManager(
                new LogManager(tempDir.resolve("failed-data")));

        try (TopicDeletionReconciler reconciler = new TopicDeletionReconciler(
                config,
                controller,
                partitions,
                guard,
                ControllerRole.legacy(config))) {
            assertEquals(0, reconciler.reconcileOnce());
            assertFalse(reconciler.isCaughtUp());

            guard.startRecovering();
            guard.activate();
            assertEquals(BrokerAvailabilityGuard.State.FENCED, guard.state());
            assertTrue(guard.hasStickyStorageFence());
        } finally {
            partitions.close();
        }
    }

    @Test
    void controllerFenceDuringAckRetryDoesNotBlockHeldActivation() throws Exception {
        BrokerConfig config = new BrokerConfig(
                0, "127.0.0.1", 19092, tempDir.resolve("retry-broker"),
                true, "127.0.0.1", 19092, false, "cluster-token");
        FailOnceMetadataRepository repository = new FailOnceMetadataRepository(
                new MetadataStore(tempDir.resolve("retry-controller")));
        ClusterController controller = new ClusterController(repository);
        BrokerAvailabilityGuard guard = new BrokerAvailabilityGuard();
        BrokerLeaseService leaseService = new BrokerLeaseService(config, controller, guard);
        leaseService.holdActivation();
        leaseService.initialize();
        assertEquals(ErrorCode.NONE,
                controller.createTopic(new CreateTopicRequest("orders", 1, 1)));
        assertEquals(ErrorCode.NONE, controller.deleteTopic("orders", 1_000L));
        repository.failNextCompareAndSet(ErrorCode.CONTROLLER_FENCED);
        PartitionManager partitions = new PartitionManager(
                new LogManager(tempDir.resolve("retry-data")));

        try (TopicDeletionReconciler reconciler = new TopicDeletionReconciler(
                config,
                controller,
                partitions,
                guard,
                ControllerRole.legacy(config))) {
            assertEquals(0, reconciler.reconcileOnce());
            assertEquals(BrokerAvailabilityGuard.State.STARTING, guard.state());

            assertEquals(1, reconciler.reconcileOnce());
            assertTrue(reconciler.isCaughtUp());
            leaseService.activateAfterStartupBarrier();
            assertEquals(BrokerAvailabilityGuard.State.ACTIVE, guard.state());
        } finally {
            leaseService.close();
            partitions.close();
        }
    }

    @Test
    void runtimeFailureDuringAckRetryDoesNotBlockHeldActivation() throws Exception {
        BrokerConfig config = new BrokerConfig(
                0, "127.0.0.1", 19092, tempDir.resolve("runtime-retry-broker"),
                true, "127.0.0.1", 19092, false, "cluster-token");
        FailOnceMetadataRepository repository = new FailOnceMetadataRepository(
                new MetadataStore(tempDir.resolve("runtime-retry-controller")));
        ClusterController controller = new ClusterController(repository);
        BrokerAvailabilityGuard guard = new BrokerAvailabilityGuard();
        BrokerLeaseService leaseService = new BrokerLeaseService(config, controller, guard);
        leaseService.holdActivation();
        leaseService.initialize();
        assertEquals(ErrorCode.NONE,
                controller.createTopic(new CreateTopicRequest("orders", 1, 1)));
        assertEquals(ErrorCode.NONE, controller.deleteTopic("orders", 1_000L));
        repository.throwNextCompareAndSet(
                new IllegalStateException("simulated acknowledgement transport failure"));
        PartitionManager partitions = new PartitionManager(
                new LogManager(tempDir.resolve("runtime-retry-data")));

        try (TopicDeletionReconciler reconciler = new TopicDeletionReconciler(
                config,
                controller,
                partitions,
                guard,
                ControllerRole.legacy(config))) {
            assertEquals(0, reconciler.reconcileOnce());
            assertEquals(BrokerAvailabilityGuard.State.STARTING, guard.state());

            assertEquals(1, reconciler.reconcileOnce());
            assertTrue(reconciler.isCaughtUp());
            leaseService.activateAfterStartupBarrier();
            assertEquals(BrokerAvailabilityGuard.State.ACTIVE, guard.state());
        } finally {
            leaseService.close();
            partitions.close();
        }
    }

    @Test
    void staleDeleteAckEpochDoesNotFenceReregisteredBroker() {
        BrokerConfig config = new BrokerConfig(
                0, "127.0.0.1", 19092, tempDir.resolve("epoch-race-broker"),
                true, "127.0.0.1", 19092, false, "cluster-token");
        ClusterController controller = new ClusterController(
                new MetadataStore(tempDir.resolve("epoch-race-controller")));
        assertEquals(ErrorCode.NONE, controller.registerBroker(
                new RegisterBrokerRequest(0, "127.0.0.1", 19092)).errorCode());
        assertEquals(ErrorCode.NONE,
                controller.createTopic(new CreateTopicRequest("orders", 1, 1)));
        assertEquals(ErrorCode.NONE, controller.deleteTopic("orders", 1_000L));
        BrokerAvailabilityGuard guard = new BrokerAvailabilityGuard();
        guard.activate();
        ReregisteringPartitionManager partitions = new ReregisteringPartitionManager(
                new LogManager(tempDir.resolve("epoch-race-data")), controller, config);

        try (TopicDeletionReconciler reconciler = new TopicDeletionReconciler(
                config,
                controller,
                partitions,
                guard,
                ControllerRole.legacy(config))) {
            assertEquals(0, reconciler.reconcileOnce());
            assertEquals(2L, controller.getBroker(0).brokerEpoch);
            assertEquals(BrokerAvailabilityGuard.State.ACTIVE, guard.state());

            assertEquals(1, reconciler.reconcileOnce());
            assertTrue(reconciler.isCaughtUp());
            assertEquals(BrokerAvailabilityGuard.State.ACTIVE, guard.state());
        } finally {
            partitions.close();
        }
    }

    @Test
    void deadlineStopsPassBeforeProcessingAnotherTombstone() {
        BrokerConfig config = new BrokerConfig(
                0, "127.0.0.1", 19092, tempDir.resolve("deadline-broker"),
                true, "127.0.0.1", 19092, false, "cluster-token");
        ClusterController controller = new ClusterController(
                new MetadataStore(tempDir.resolve("deadline-controller")));
        assertEquals(ErrorCode.NONE, controller.registerBroker(
                new RegisterBrokerRequest(0, "127.0.0.1", 19092)).errorCode());
        assertEquals(ErrorCode.NONE,
                controller.createTopic(new CreateTopicRequest("first", 1, 1)));
        assertEquals(ErrorCode.NONE,
                controller.createTopic(new CreateTopicRequest("second", 1, 1)));
        assertEquals(ErrorCode.NONE, controller.deleteTopic("first", 1_000L));
        assertEquals(ErrorCode.NONE, controller.deleteTopic("second", 1_001L));
        AtomicLong nanoTime = new AtomicLong(100L);
        DeadlineAdvancingPartitionManager partitions = new DeadlineAdvancingPartitionManager(
                new LogManager(tempDir.resolve("deadline-data")), nanoTime, 200L);

        try (TopicDeletionReconciler reconciler = new TopicDeletionReconciler(
                config,
                controller,
                partitions,
                new BrokerAvailabilityGuard(),
                ControllerRole.legacy(config),
                new ScheduledThreadPoolExecutor(1),
                nanoTime::get)) {
            assertEquals(1, reconciler.reconcileOnce(150L));
            assertEquals("DELETED", controller.getTopicTombstone("first").state);
            assertEquals("DELETING", controller.getTopicTombstone("second").state);
        } finally {
            partitions.close();
        }
    }

    private static final class CountingPartitionManager extends PartitionManager {
        private int deleteCalls;

        private CountingPartitionManager(LogManager logManager) {
            super(logManager);
        }

        @Override
        public void deleteTopic(String topic, long tombstoneVersion) {
            deleteCalls++;
            super.deleteTopic(topic, tombstoneVersion);
        }
    }

    private static final class FailingPartitionManager extends PartitionManager {
        private FailingPartitionManager(LogManager logManager) {
            super(logManager);
        }

        @Override
        public void deleteTopic(String topic, long tombstoneVersion) {
            throw new IllegalStateException("simulated storage cleanup failure");
        }
    }

    private static final class ReregisteringPartitionManager extends PartitionManager {
        private final ClusterController controller;
        private final BrokerConfig config;
        private boolean reregistered;

        private ReregisteringPartitionManager(
                LogManager logManager, ClusterController controller, BrokerConfig config) {
            super(logManager);
            this.controller = controller;
            this.config = config;
        }

        @Override
        public void deleteTopic(String topic, long tombstoneVersion) {
            super.deleteTopic(topic, tombstoneVersion);
            if (reregistered) {
                return;
            }
            ErrorCode offline = controller.markBrokerOffline(config.brokerId());
            if (offline != ErrorCode.NONE) {
                throw new IllegalStateException("failed to offline broker: " + offline);
            }
            ClusterController.BrokerRegistration registration = controller.registerBroker(
                    new RegisterBrokerRequest(config.brokerId(), config.host(), config.port()));
            if (registration.errorCode() != ErrorCode.NONE) {
                throw new IllegalStateException(
                        "failed to re-register broker: " + registration.errorCode());
            }
            reregistered = true;
        }
    }

    private static final class DeadlineAdvancingPartitionManager extends PartitionManager {
        private final AtomicLong nanoTime;
        private final long afterFirstDeleteNanos;

        private DeadlineAdvancingPartitionManager(
                LogManager logManager, AtomicLong nanoTime, long afterFirstDeleteNanos) {
            super(logManager);
            this.nanoTime = nanoTime;
            this.afterFirstDeleteNanos = afterFirstDeleteNanos;
        }

        @Override
        public void deleteTopic(String topic, long tombstoneVersion) {
            super.deleteTopic(topic, tombstoneVersion);
            nanoTime.set(afterFirstDeleteNanos);
        }
    }

    private static final class FailOnceMetadataRepository implements ClusterMetadataRepository {
        private final MetadataStore delegate;
        private ErrorCode nextCompareAndSetError;
        private RuntimeException nextCompareAndSetFailure;

        private FailOnceMetadataRepository(MetadataStore delegate) {
            this.delegate = delegate;
        }

        private void failNextCompareAndSet(ErrorCode errorCode) {
            nextCompareAndSetError = errorCode;
        }

        private void throwNextCompareAndSet(RuntimeException failure) {
            nextCompareAndSetFailure = failure;
        }

        @Override
        public VersionedClusterMetadata read() {
            return delegate.read();
        }

        @Override
        public VersionedClusterMetadata read(java.time.Duration timeout) {
            return delegate.read(timeout);
        }

        @Override
        public UpdateResult initialize(ClusterMetadata metadata) {
            return delegate.initialize(metadata);
        }

        @Override
        public UpdateResult initialize(ClusterMetadata metadata, java.time.Duration timeout) {
            return delegate.initialize(metadata, timeout);
        }

        @Override
        public UpdateResult compareAndSet(long expectedRevision, ClusterMetadata metadata) {
            if (nextCompareAndSetFailure != null) {
                RuntimeException failure = nextCompareAndSetFailure;
                nextCompareAndSetFailure = null;
                throw failure;
            }
            if (nextCompareAndSetError != null) {
                ErrorCode errorCode = nextCompareAndSetError;
                nextCompareAndSetError = null;
                return new UpdateResult(errorCode, delegate.read());
            }
            return delegate.compareAndSet(expectedRevision, metadata);
        }

        @Override
        public UpdateResult compareAndSet(
                long expectedRevision, ClusterMetadata metadata, java.time.Duration timeout) {
            if (nextCompareAndSetFailure != null) {
                RuntimeException failure = nextCompareAndSetFailure;
                nextCompareAndSetFailure = null;
                throw failure;
            }
            if (nextCompareAndSetError != null) {
                ErrorCode errorCode = nextCompareAndSetError;
                nextCompareAndSetError = null;
                return new UpdateResult(errorCode, delegate.read(timeout));
            }
            return delegate.compareAndSet(expectedRevision, metadata, timeout);
        }
    }
}
