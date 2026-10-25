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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * Applies installed tombstones away from the metadata-watch executor.  A
 * broker acknowledges only after local WAL-backed cleanup and Leader epoch
 * file removal both succeed.
 */
public final class TopicDeletionReconciler implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(TopicDeletionReconciler.class);
    private static final long DEFAULT_RETRY_MS = 250L;
    private static final long DEFAULT_PASS_TIMEOUT_MS = 5_000L;

    private final BrokerConfig config;
    private final ClusterController clusterController;
    private final PartitionManager partitionManager;
    private final BrokerAvailabilityGuard availabilityGuard;
    private final ControllerRole controllerRole;
    private final LeaderEpochStateStore epochStateStore;
    private final ScheduledExecutorService executor;
    private final boolean ownsExecutor;
    private final LongSupplier nanoTime;
    private final Supplier<ControllerClient> controllerClientFactory;
    private final Map<String, Long> locallyCleanedTombstones = new ConcurrentHashMap<>();
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
                true,
                System::nanoTime,
                ControllerClient::new);
    }

    TopicDeletionReconciler(
            BrokerConfig config,
            ClusterController clusterController,
            PartitionManager partitionManager,
            BrokerAvailabilityGuard availabilityGuard,
            ControllerRole controllerRole,
            ScheduledExecutorService executor) {
        this(
                config,
                clusterController,
                partitionManager,
                availabilityGuard,
                controllerRole,
                executor,
                false,
                System::nanoTime,
                ControllerClient::new);
    }

    TopicDeletionReconciler(
            BrokerConfig config,
            ClusterController clusterController,
            PartitionManager partitionManager,
            BrokerAvailabilityGuard availabilityGuard,
            ControllerRole controllerRole,
            ScheduledExecutorService executor,
            LongSupplier nanoTime) {
        this(
                config,
                clusterController,
                partitionManager,
                availabilityGuard,
                controllerRole,
                executor,
                false,
                nanoTime,
                ControllerClient::new);
    }

    private TopicDeletionReconciler(
            BrokerConfig config,
            ClusterController clusterController,
            PartitionManager partitionManager,
            BrokerAvailabilityGuard availabilityGuard,
            ControllerRole controllerRole,
            ScheduledExecutorService executor,
            boolean ownsExecutor,
            LongSupplier nanoTime,
            Supplier<ControllerClient> controllerClientFactory) {
        this.config = config;
        this.clusterController = clusterController;
        this.partitionManager = partitionManager;
        this.availabilityGuard = availabilityGuard;
        this.controllerRole = controllerRole;
        this.epochStateStore = new LeaderEpochStateStore(config.dataDir().resolve("leader-epochs"));
        this.executor = executor;
        this.ownsExecutor = ownsExecutor;
        this.nanoTime = nanoTime;
        this.controllerClientFactory = controllerClientFactory;
    }

    public void start() {
        executor.scheduleWithFixedDelay(this::runSafely, 0L, DEFAULT_RETRY_MS, TimeUnit.MILLISECONDS);
    }

    /** Runs one bounded reconciliation pass; useful for deterministic tests. */
    public int reconcileOnce() {
        long now = nanoTime.getAsLong();
        long timeoutNanos = TimeUnit.MILLISECONDS.toNanos(DEFAULT_PASS_TIMEOUT_MS);
        long deadlineNanos = now > Long.MAX_VALUE - timeoutNanos
                ? Long.MAX_VALUE
                : now + timeoutNanos;
        return reconcileOnce(deadlineNanos);
    }

    /** Runs one pass without starting work after the supplied monotonic deadline. */
    public int reconcileOnce(long deadlineNanos) {
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
        ControllerClient remoteClient = null;
        try {
            for (ClusterMetadata.TopicTombstone tombstone : tombstones) {
                if (deadlineReached(deadlineNanos)) {
                    break;
                }
                if (!Long.valueOf(tombstone.tombstoneVersion).equals(
                        locallyCleanedTombstones.get(tombstone.topicName))) {
                    try {
                        partitionManager.deleteTopic(tombstone.topicName, tombstone.tombstoneVersion);
                        epochStateStore.deleteTopic(tombstone.topicName);
                        locallyCleanedTombstones.put(
                                tombstone.topicName, tombstone.tombstoneVersion);
                    } catch (RuntimeException failure) {
                        availabilityGuard.fenceStorageCleanup();
                        log.warn("Topic deletion cleanup failed on broker {} for {}",
                                config.brokerId(), tombstone.topicName, failure);
                        continue;
                    }
                }
                if (tombstone.acknowledgedBrokerIds.contains(config.brokerId())) {
                    continue;
                }
                try {
                    if (!controllerRole.isActive() && remoteClient == null) {
                        remoteClient = connectRemoteController(deadlineNanos);
                    }
                    acknowledge(tombstone, broker, remoteClient, deadlineNanos);
                    applied++;
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    return applied;
                } catch (ControllerClient.ControllerRequestException failure) {
                    // A stale snapshot is expected during a metadata transition;
                    // refresh and retry on the next bounded pass. An ACK's epoch
                    // may have become stale after this pass captured its snapshot;
                    // lease heartbeats remain the authority for self-fencing.
                    log.debug("Topic deletion ack deferred topic={} error={}",
                            tombstone.topicName, failure.errorCode());
                } catch (RuntimeException failure) {
                    log.warn("Topic deletion acknowledgement failed on broker {} for {}",
                            config.brokerId(), tombstone.topicName, failure);
                    remoteClient = closeRemoteClient(remoteClient, deadlineNanos);
                }
            }
        } finally {
            closeRemoteClient(remoteClient, deadlineNanos);
        }
        return applied;
    }

    public boolean isCaughtUp() {
        ClusterMetadata.BrokerEntry broker = clusterController.getBroker(config.brokerId());
        if (broker == null || !broker.online) {
            return false;
        }
        return clusterController.snapshot().topicTombstones.values().stream()
                .allMatch(tombstone -> tombstone.acknowledgedBrokerIds.contains(config.brokerId())
                        && Long.valueOf(tombstone.tombstoneVersion).equals(
                        locallyCleanedTombstones.get(tombstone.topicName)));
    }

    private void acknowledge(
            ClusterMetadata.TopicTombstone tombstone,
            ClusterMetadata.BrokerEntry broker,
            ControllerClient remoteClient,
            long deadlineNanos) throws InterruptedException {
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
            if (remoteClient == null) {
                throw new IllegalStateException("Controller is not available");
            }
            remoteClient.acknowledgeTopicDeletion(
                    request, remainingTimeoutMillis(deadlineNanos));
            if (config.usesLegacyControllerMode()) {
                MetadataSync.refresh(
                        clusterController, remoteClient, deadlineNanos, nanoTime);
            }
        }
    }

    private ControllerClient connectRemoteController(long deadlineNanos) throws InterruptedException {
        var identity = controllerRole.currentLeader();
        if (identity == null) {
            throw new IllegalStateException("Controller is not available");
        }
        ControllerClient client = controllerClientFactory.get();
        try {
            client.connect(identity.host(), identity.port(), remainingTimeoutMillis(deadlineNanos));
            return client;
        } catch (InterruptedException | RuntimeException | Error failure) {
            closeRemoteClient(client, deadlineNanos);
            throw failure;
        }
    }

    private boolean deadlineReached(long deadlineNanos) {
        return nanoTime.getAsLong() >= deadlineNanos;
    }

    private long remainingTimeoutMillis(long deadlineNanos) {
        long remainingNanos = deadlineNanos - nanoTime.getAsLong();
        return Math.max(1L, TimeUnit.NANOSECONDS.toMillis(Math.max(0L, remainingNanos)));
    }

    private ControllerClient closeRemoteClient(ControllerClient client, long deadlineNanos) {
        if (client == null) {
            return null;
        }
        try {
            client.close(remainingTimeoutMillis(deadlineNanos));
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        } catch (RuntimeException failure) {
            log.debug("Controller client cleanup failed during topic deletion reconciliation", failure);
        }
        return null;
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
