package com.minimq.broker.admin;

import com.minimq.broker.BrokerAvailabilityGuard;
import com.minimq.broker.BrokerConfig;
import com.minimq.broker.ControllerClient;
import com.minimq.broker.ControllerRole;
import com.minimq.broker.MetadataSync;
import com.minimq.broker.PartitionManager;
import com.minimq.broker.controller.ClusterController;
import com.minimq.broker.metadata.ClusterMetadata;
import com.minimq.broker.replication.LeaderEpochStateStore;
import com.minimq.common.error.ErrorCode;
import com.minimq.protocol.message.ProtocolMessages.TopicDeleteAckRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Applies installed tombstones away from the metadata-watch executor.  A
 * broker acknowledges only after local WAL-backed cleanup and Leader epoch
 * file removal both succeed.
 */
public final class TopicDeletionReconciler implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(TopicDeletionReconciler.class);
    private static final long DEFAULT_RETRY_MS = 250L;

    private final BrokerConfig config;
    private final ClusterController clusterController;
    private final PartitionManager partitionManager;
    private final BrokerAvailabilityGuard availabilityGuard;
    private final ControllerRole controllerRole;
    private final LeaderEpochStateStore epochStateStore;
    private final ScheduledExecutorService executor;
    private final boolean ownsExecutor;
    private volatile boolean closed;

    public TopicDeletionReconciler(
            BrokerConfig config,
            ClusterController clusterController,
            PartitionManager partitionManager,
            BrokerAvailabilityGuard availabilityGuard,
            ControllerRole controllerRole) {
        this(
                config,
                clusterController,
                partitionManager,
                availabilityGuard,
                controllerRole,
                Executors.newSingleThreadScheduledExecutor(runnable -> {
                    Thread thread = new Thread(runnable, "topic-deletion-reconciler-" + config.brokerId());
                    thread.setDaemon(true);
                    return thread;
                }),
                true);
    }

    TopicDeletionReconciler(
            BrokerConfig config,
            ClusterController clusterController,
            PartitionManager partitionManager,
            BrokerAvailabilityGuard availabilityGuard,
            ControllerRole controllerRole,
            ScheduledExecutorService executor) {
        this(config, clusterController, partitionManager, availabilityGuard, controllerRole, executor, false);
    }

    private TopicDeletionReconciler(
            BrokerConfig config,
            ClusterController clusterController,
            PartitionManager partitionManager,
            BrokerAvailabilityGuard availabilityGuard,
            ControllerRole controllerRole,
            ScheduledExecutorService executor,
            boolean ownsExecutor) {
        this.config = config;
        this.clusterController = clusterController;
        this.partitionManager = partitionManager;
        this.availabilityGuard = availabilityGuard;
        this.controllerRole = controllerRole;
        this.epochStateStore = new LeaderEpochStateStore(config.dataDir().resolve("leader-epochs"));
        this.executor = executor;
        this.ownsExecutor = ownsExecutor;
    }

    public void start() {
        executor.scheduleWithFixedDelay(this::runSafely, 0L, DEFAULT_RETRY_MS, TimeUnit.MILLISECONDS);
    }

    /** Runs one bounded reconciliation pass; useful for deterministic tests. */
    public int reconcileOnce() {
        if (closed) {
            return 0;
        }
        ClusterMetadata.BrokerEntry broker = clusterController.getBroker(config.brokerId());
        if (broker == null || !broker.online) {
            return 0;
        }
        int applied = 0;
        var tombstones = clusterController.snapshot().topicTombstones.values().stream()
                .sorted(Comparator.comparing(tombstone -> tombstone.topicName))
                .toList();
        for (ClusterMetadata.TopicTombstone tombstone : tombstones) {
            try {
                partitionManager.deleteTopic(tombstone.topicName, tombstone.tombstoneVersion);
                epochStateStore.deleteTopic(tombstone.topicName);
                try {
                    acknowledge(tombstone, broker);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    return applied;
                }
                applied++;
            } catch (ControllerClient.ControllerRequestException failure) {
                // A stale snapshot is expected during a metadata transition;
                // refresh and retry on the next bounded pass.
                log.debug("Topic deletion ack deferred topic={} error={}",
                        tombstone.topicName, failure.errorCode());
                if (failure.errorCode() == ErrorCode.BROKER_FENCED
                        || failure.errorCode() == ErrorCode.CONTROLLER_FENCED) {
                    availabilityGuard.fence();
                }
            } catch (RuntimeException failure) {
                availabilityGuard.fence();
                log.warn("Topic deletion cleanup failed on broker {} for {}",
                        config.brokerId(), tombstone.topicName, failure);
            }
        }
        return applied;
    }

    private void acknowledge(
            ClusterMetadata.TopicTombstone tombstone,
            ClusterMetadata.BrokerEntry broker) throws InterruptedException {
        long controllerEpoch = clusterController.snapshot().controllerEpoch;
        TopicDeleteAckRequest request = new TopicDeleteAckRequest(
                config.clusterToken(),
                tombstone.topicName,
                tombstone.tombstoneVersion,
                config.brokerId(),
                broker.brokerEpoch,
                controllerEpoch);
        if (controllerRole.isActive()) {
            ErrorCode error = clusterController.acknowledgeTopicDeletion(
                    request.topicName(), request.tombstoneVersion(), request.brokerId(),
                    request.brokerEpoch(), request.controllerEpoch());
            if (error != ErrorCode.NONE) {
                throw new ControllerClient.ControllerRequestException(error);
            }
        } else {
            try (ControllerClient client = new ControllerClient()) {
                var identity = controllerRole.currentLeader();
                if (identity == null) {
                    throw new IllegalStateException("Controller is not available");
                }
                client.connect(identity.host(), identity.port());
                client.acknowledgeTopicDeletion(request);
            }
            MetadataSync.refresh(config, clusterController, controllerRole);
        }
    }

    private void runSafely() {
        if (closed) {
            return;
        }
        try {
            reconcileOnce();
        } catch (RuntimeException failure) {
            log.warn("Topic deletion reconciliation failed on broker {}", config.brokerId(), failure);
        }
    }

    @Override
    public void close() {
        closed = true;
        if (!ownsExecutor) {
            return;
        }
        executor.shutdown();
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            executor.shutdownNow();
        }
    }
}
