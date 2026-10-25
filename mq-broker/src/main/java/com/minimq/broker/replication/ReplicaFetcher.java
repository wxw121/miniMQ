package com.minimq.broker.replication;

import com.minimq.broker.BrokerConfig;
import com.minimq.broker.BrokerAvailabilityGuard;
import com.minimq.broker.MetadataSync;
import com.minimq.broker.PartitionManager;
import com.minimq.broker.ControllerRole;
import com.minimq.broker.controller.ClusterController;
import com.minimq.broker.metadata.ClusterMetadata.BrokerEntry;
import com.minimq.broker.metadata.ClusterMetadata.PartitionEntry;
import com.minimq.broker.metadata.ClusterMetadata.TopicEntry;
import com.minimq.common.error.ErrorCode;
import com.minimq.protocol.message.ProtocolMessages.ReplicaFetchResponse;
import com.minimq.protocol.message.ProtocolMessages.ReplicaFetchV2Response;
import com.minimq.protocol.message.ProtocolMessages.ScheduledReplicaRecord;
import com.minimq.storage.LogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Background follower loop that pulls partition bytes from the current leader broker.
 */
public final class ReplicaFetcher implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(ReplicaFetcher.class);
    private static final int FETCH_MAX_BYTES = 1024 * 1024;
    private static final long SLEEP_MS = 50;

    private final BrokerConfig config;
    private final ClusterController clusterController;
    private final PartitionManager partitionManager;
    private final BrokerAvailabilityGuard availabilityGuard;
    private final ControllerRole controllerRole;
    private final ReplicaStateReporter replicaStateReporter;
    private final boolean ownsReplicaStateReporter;
    private final ReplicaConnectionPool connectionPool = new ReplicaConnectionPool();
    private final LeaderEpochStateStore epochStateStore;
    private final Thread thread;
    private volatile boolean running = true;

    public ReplicaFetcher(
            BrokerConfig config,
            ClusterController clusterController,
            PartitionManager partitionManager) {
        this(config, clusterController, partitionManager, activeGuard(), ControllerRole.legacy(config));
    }

    public ReplicaFetcher(
            BrokerConfig config,
            ClusterController clusterController,
            PartitionManager partitionManager,
            BrokerAvailabilityGuard availabilityGuard) {
        this(config, clusterController, partitionManager, availabilityGuard,
                ControllerRole.legacy(config));
    }

    public ReplicaFetcher(
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
                new ReplicaStateReporter(
                        config, clusterController, availabilityGuard, controllerRole),
                true);
    }

    public ReplicaFetcher(
            BrokerConfig config,
            ClusterController clusterController,
            PartitionManager partitionManager,
            BrokerAvailabilityGuard availabilityGuard,
            ControllerRole controllerRole,
            ReplicaStateReporter replicaStateReporter) {
        this(
                config,
                clusterController,
                partitionManager,
                availabilityGuard,
                controllerRole,
                replicaStateReporter,
                false);
    }

    private ReplicaFetcher(
            BrokerConfig config,
            ClusterController clusterController,
            PartitionManager partitionManager,
            BrokerAvailabilityGuard availabilityGuard,
            ControllerRole controllerRole,
            ReplicaStateReporter replicaStateReporter,
            boolean ownsReplicaStateReporter) {
        this.config = config;
        this.clusterController = clusterController;
        this.partitionManager = partitionManager;
        this.availabilityGuard = availabilityGuard;
        this.controllerRole = controllerRole;
        this.replicaStateReporter = replicaStateReporter;
        this.ownsReplicaStateReporter = ownsReplicaStateReporter;
        this.epochStateStore = new LeaderEpochStateStore(config.dataDir().resolve("leader-epochs"));
        this.thread = new Thread(this::runLoop, "replica-fetcher-" + config.brokerId());
        this.thread.setDaemon(true);
    }

    public void start() {
        thread.start();
    }

    @Override
    public void close() throws InterruptedException {
        running = false;
        try {
            connectionPool.close();
            thread.join(SLEEP_MS * 10);
            if (thread.isAlive()) {
                thread.interrupt();
                thread.join(5_000);
            }
        } finally {
            if (ownsReplicaStateReporter) {
                replicaStateReporter.close();
            }
        }
    }

    private void runLoop() {
        int metadataRefreshCounter = 0;
        int replicaReportCounter = 0;
        while (running) {
            try {
                if (!availabilityGuard.allowsDataPlane()) {
                    Thread.sleep(SLEEP_MS);
                    continue;
                }
                if (!controllerRole.isActive() && metadataRefreshCounter++ % 20 == 0) {
                    MetadataSync.refresh(config, clusterController, controllerRole);
                }
                boolean forceReport = replicaReportCounter++ % 20 == 0;
                fetchAssignedReplicas(forceReport);
                Thread.sleep(SLEEP_MS);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception failure) {
                log.warn("Replica fetch loop failed on broker {}", config.brokerId(), failure);
            }
        }
    }

    private void fetchAssignedReplicas(boolean forceReport) throws InterruptedException {
        for (TopicEntry topic : clusterController.snapshot().topics.values()) {
            for (PartitionEntry partition : topic.partitions) {
                if (!partition.replicaBrokerIds.contains(config.brokerId())) {
                    continue;
                }
                if (partition.leaderBrokerId == config.brokerId()) {
                    if (forceReport) {
                        reportLocalReplicaState(topic.topicName, partition.partitionId);
                    }
                    continue;
                }
                boolean changed = fetchPartition(topic.topicName, partition);
                if (changed || forceReport) {
                    reportLocalReplicaState(topic.topicName, partition.partitionId);
                }
            }
        }
    }

    private boolean fetchPartition(String topicName, PartitionEntry partition)
            throws InterruptedException {
        BrokerEntry leader = clusterController.getBroker(partition.leaderBrokerId);
        if (leader == null || !leader.online) {
            return false;
        }
        boolean changed = false;
        long fetchOffset = partitionManager.highWatermark(topicName, partition.partitionId);
        long appliedEpoch = epochStateStore.load(topicName, partition.partitionId)
                .map(LeaderEpochState::leaderEpoch)
                .orElse(-1L);
        if (partition.leaderEpoch > appliedEpoch) {
            if (fetchOffset > partition.leaderEpochStartOffset) {
                partitionManager.truncateTo(topicName, partition.partitionId, partition.leaderEpochStartOffset);
                fetchOffset = partition.leaderEpochStartOffset;
                changed = true;
            }
            epochStateStore.save(topicName, partition.partitionId,
                    new LeaderEpochState(partition.leaderEpoch, partition.leaderEpochStartOffset, true));
        }
        if (fetchOffset < partition.leaderEpochStartOffset) {
            fetchOffset = partition.leaderEpochStartOffset;
        }
        try {
            LeaderReplicaClient client = connectionPool.client(leader.host, leader.port);
            var snapshot = clusterController.snapshot();
            BrokerEntry self = snapshot.brokers.get(config.brokerId());
            long brokerEpoch = self == null || self.brokerEpoch <= 0 ? 1L : self.brokerEpoch;
            ReplicaFetchV2Response response = client.replicaFetchV2(
                    config.clusterToken(),
                    topicName,
                    partition.partitionId,
                    config.brokerId(),
                    brokerEpoch,
                    snapshot.controllerEpoch,
                    partition.leaderEpoch,
                    fetchOffset,
                    FETCH_MAX_BYTES);
            for (ScheduledReplicaRecord record : response.records().items()) {
                partitionManager.replicateAt(
                        topicName,
                        partition.partitionId,
                        record.offset(),
                        record.key(),
                        record.value(),
                        record.deliverAtEpochMs());
                changed = true;
            }
        } catch (LogManager.ScheduledCapacityExceededException ignored) {
            return changed;
        } catch (LeaderReplicaClient.ReplicaFetchException failure) {
            connectionPool.invalidate(leader.host, leader.port);
            if ((failure.errorCode() == ErrorCode.NOT_LEADER
                    || failure.errorCode() == ErrorCode.BROKER_FENCED)
                    && !controllerRole.isActive()) {
                MetadataSync.refresh(config, clusterController, controllerRole);
            }
            return changed;
        } catch (RuntimeException failure) {
            connectionPool.invalidate(leader.host, leader.port);
            if (!controllerRole.isActive()) {
                MetadataSync.refresh(config, clusterController, controllerRole);
                return changed;
            }
            throw failure;
        }
        return changed;
    }

    private void reportLocalReplicaState(String topicName, int partitionId) {
        LogManager.VisibilitySnapshot visibility =
                partitionManager.visibilitySnapshot(topicName, partitionId);
        ErrorCode error = replicaStateReporter.report(
                topicName,
                partitionId,
                config.brokerId(),
                visibility.physicalHighWatermark(),
                visibility.visibleHighWatermark(),
                visibility.nextVisibleAtEpochMs());
        if (error != ErrorCode.NONE) {
            log.debug(
                    "Deferred local replica state report topic={} partition={} broker={} leo={} error={}",
                    topicName,
                    partitionId,
                    config.brokerId(),
                    visibility.physicalHighWatermark(),
                    error);
        }
    }

    private static BrokerAvailabilityGuard activeGuard() {
        BrokerAvailabilityGuard guard = new BrokerAvailabilityGuard();
        guard.activate();
        return guard;
    }
}
