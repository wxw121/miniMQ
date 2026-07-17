package com.minimq.broker.replication;

import com.minimq.broker.BrokerConfig;
import com.minimq.broker.MetadataSync;
import com.minimq.broker.PartitionManager;
import com.minimq.broker.controller.ClusterController;
import com.minimq.broker.metadata.ClusterMetadata.BrokerEntry;
import com.minimq.broker.metadata.ClusterMetadata.PartitionEntry;
import com.minimq.broker.metadata.ClusterMetadata.TopicEntry;
import com.minimq.common.error.ErrorCode;
import com.minimq.protocol.message.ProtocolMessages.FetchedRecord;
import com.minimq.protocol.message.ProtocolMessages.ReplicaFetchResponse;
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
    private final Thread thread;
    private volatile boolean running = true;

    public ReplicaFetcher(
            BrokerConfig config,
            ClusterController clusterController,
            PartitionManager partitionManager) {
        this.config = config;
        this.clusterController = clusterController;
        this.partitionManager = partitionManager;
        this.thread = new Thread(this::runLoop, "replica-fetcher-" + config.brokerId());
        this.thread.setDaemon(true);
    }

    public void start() {
        thread.start();
    }

    @Override
    public void close() throws InterruptedException {
        running = false;
        thread.interrupt();
        thread.join(5_000);
    }

    private void runLoop() {
        int metadataRefreshCounter = 0;
        while (running) {
            try {
                if (!config.controller() && metadataRefreshCounter++ % 20 == 0) {
                    MetadataSync.registerAndSync(config, clusterController);
                }
                fetchAssignedReplicas();
                Thread.sleep(SLEEP_MS);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception failure) {
                log.warn("Replica fetch loop failed on broker {}", config.brokerId(), failure);
            }
        }
    }

    private void fetchAssignedReplicas() throws InterruptedException {
        for (TopicEntry topic : clusterController.snapshot().topics.values()) {
            for (PartitionEntry partition : topic.partitions) {
                if (!partition.replicaBrokerIds.contains(config.brokerId())) {
                    continue;
                }
                if (partition.leaderBrokerId == config.brokerId()) {
                    continue;
                }
                fetchPartition(topic.topicName, partition);
            }
        }
    }

    private void fetchPartition(String topicName, PartitionEntry partition) throws InterruptedException {
        BrokerEntry leader = clusterController.getBroker(partition.leaderBrokerId);
        if (leader == null || !leader.online) {
            return;
        }
        long fetchOffset = partitionManager.highWatermark(topicName, partition.partitionId);
        try (LeaderReplicaClient client = new LeaderReplicaClient()) {
            client.connect(leader.host, leader.port);
            ReplicaFetchResponse response = client.replicaFetch(
                    topicName,
                    partition.partitionId,
                    config.brokerId(),
                    fetchOffset,
                    FETCH_MAX_BYTES);
            for (FetchedRecord record : response.records()) {
                partitionManager.replicateAt(
                        topicName,
                        partition.partitionId,
                        record.offset(),
                        record.key(),
                        record.value());
            }
        } catch (LeaderReplicaClient.ReplicaFetchException failure) {
            if (failure.errorCode() == ErrorCode.NOT_LEADER && !config.controller()) {
                MetadataSync.registerAndSync(config, clusterController);
            }
        }
    }
}
