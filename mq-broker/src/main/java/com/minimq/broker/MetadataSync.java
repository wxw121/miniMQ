package com.minimq.broker;

import com.minimq.broker.controller.ClusterController;
import com.minimq.broker.metadata.ClusterMetadata.BrokerEntry;
import com.minimq.broker.metadata.ClusterMetadata.PartitionEntry;
import com.minimq.broker.metadata.ClusterMetadata.TopicEntry;
import com.minimq.protocol.message.ProtocolMessages.BrokerMetadata;
import com.minimq.protocol.message.ProtocolMessages.MetadataResponse;
import com.minimq.protocol.message.ProtocolMessages.PartitionMetadata;
import com.minimq.protocol.message.ProtocolMessages.TopicMetadata;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Broker 本地元数据缓存，接收 Controller 快照并提供线程安全读取入口。
 */
public final class MetadataSync {
    private MetadataSync() {
    }

    public static void refresh(BrokerConfig config, ClusterController controller) throws InterruptedException {
        refresh(config, controller, ControllerRole.legacy(config));
    }

    public static void refresh(
            BrokerConfig config, ClusterController controller, ControllerRole controllerRole)
            throws InterruptedException {
        if (controllerRole.isActive()) {
            return;
        }
        if (!config.usesLegacyControllerMode()) {
            // Production followers are installed by MetadataWatchService with the etcd revision.
            return;
        }
        var identity = controllerRole.currentLeader();
        if (identity == null) {
            throw new IllegalStateException("Controller is not available");
        }
        try (ControllerClient client = new ControllerClient()) {
            client.connect(identity.host(), identity.port());
            applyMetadata(controller, client.fetchMetadata());
        }
    }

    static void applyMetadata(ClusterController controller, MetadataResponse metadata) {
        controller.updateMetadata(snapshot -> {
            snapshot.brokers.clear();
            snapshot.topics.clear();
            snapshot.metadataVersion = metadata.metadataVersion();
            if (metadata.controller() != null) {
                snapshot.controllerEpoch = metadata.controller().controllerEpoch();
            }

            for (BrokerMetadata broker : metadata.brokers()) {
                BrokerEntry entry = new BrokerEntry();
                entry.brokerId = broker.brokerId();
                entry.host = broker.host();
                entry.port = broker.port();
                entry.online = true;
                snapshot.brokers.put(entry.brokerId, entry);
            }
            for (TopicMetadata topic : metadata.topics()) {
                TopicEntry entry = new TopicEntry();
                entry.topicName = topic.topicName();
                for (PartitionMetadata partition : topic.partitions()) {
                    PartitionEntry partitionEntry = new PartitionEntry();
                    partitionEntry.partitionId = partition.partitionId();
                    partitionEntry.leaderBrokerId = partition.leaderBrokerId();
                    partitionEntry.replicaBrokerIds = new ArrayList<>(partition.replicaBrokerIds());
                    partitionEntry.isrBrokerIds = new ArrayList<>(partition.isrBrokerIds());
                    partitionEntry.replicaLogEndOffsets =
                            new HashMap<>(partition.replicaLogEndOffsets());
                    partitionEntry.frozen = partition.frozen();
                    partitionEntry.leaderEpoch = partition.leaderEpoch();
                    partitionEntry.leaderEpochStartOffset = partition.leaderEpochStartOffset();
                    partitionEntry.leaderReady = partition.leaderReady();
                    entry.partitions.add(partitionEntry);
                }
                snapshot.topics.put(entry.topicName, entry);
            }
            return null;
        });
    }
}
