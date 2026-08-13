package com.minimq.broker;

import com.minimq.broker.controller.ClusterController;
import com.minimq.broker.metadata.ClusterMetadata;
import com.minimq.broker.metadata.ClusterMetadata.BrokerEntry;
import com.minimq.broker.metadata.ClusterMetadata.PartitionEntry;
import com.minimq.broker.metadata.ClusterMetadata.TopicEntry;
import com.minimq.broker.metadata.VersionedClusterMetadata;
import com.minimq.protocol.message.ProtocolMessages.BrokerMetadata;
import com.minimq.protocol.message.ProtocolMessages.MetadataResponse;
import com.minimq.protocol.message.ProtocolMessages.MetadataV2Request;
import com.minimq.protocol.message.ProtocolMessages.MetadataV2Response;
import com.minimq.protocol.message.ProtocolMessages.OpsBrokerInfo;
import com.minimq.protocol.message.ProtocolMessages.PartitionMetadata;
import com.minimq.protocol.message.ProtocolMessages.TopicTombstoneInfo;
import com.minimq.protocol.message.ProtocolMessages.TopicTombstoneSummary;
import com.minimq.protocol.message.ProtocolMessages.TopicMetadata;
import com.minimq.protocol.message.ProtocolMessages.TopicSummary;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/**
 * Broker 本地元数据缓存，接收 Controller 快照并提供线程安全读取入口。
 */
public final class MetadataSync {
    private static final int MAX_CONSISTENT_FETCH_ATTEMPTS = 3;
    private static final int METADATA_V2_PAGE_LIMIT = 10_000;

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
            // After a remote LEADER_READY RPC, ClusterController.noteCommittedLeaderReady
            // patches local readiness so the same request does not see LEADER_NOT_READY.
            return;
        }
        var identity = controllerRole.currentLeader();
        if (identity == null) {
            throw new IllegalStateException("Controller is not available");
        }
        try (ControllerClient client = new ControllerClient()) {
            client.connect(identity.host(), identity.port());
            refresh(controller, client);
        }
    }

    static void refresh(ClusterController controller, ControllerClient client)
            throws InterruptedException {
        IllegalStateException lastMismatch = null;
        for (int attempt = 0; attempt < MAX_CONSISTENT_FETCH_ATTEMPTS; attempt++) {
            MetadataV2Snapshot metadataV2 = fetchMetadataV2Snapshot(
                    client, () -> TimeUnit.SECONDS.toMillis(5));
            MetadataResponse metadata = fetchLegacyMetadata(
                    client, metadataV2, () -> TimeUnit.SECONDS.toMillis(5));
            if (metadata.metadataVersion() == metadataV2.metadataVersion()) {
                applyMetadata(controller, metadata, metadataV2);
                return;
            }
            lastMismatch = revisionMismatch(metadata, metadataV2);
        }
        throw new IllegalStateException(
                "Unable to fetch a consistent metadata snapshot after "
                        + MAX_CONSISTENT_FETCH_ATTEMPTS + " attempts",
                lastMismatch);
    }

    public static void refresh(
            ClusterController controller,
            ControllerClient client,
            long deadlineNanos,
            LongSupplier nanoTime) throws InterruptedException {
        IllegalStateException lastMismatch = null;
        for (int attempt = 0; attempt < MAX_CONSISTENT_FETCH_ATTEMPTS; attempt++) {
            MetadataV2Snapshot metadataV2 = fetchMetadataV2Snapshot(
                    client, () -> remainingTimeoutMillis(deadlineNanos, nanoTime));
            MetadataResponse metadata = fetchLegacyMetadata(
                    client,
                    metadataV2,
                    () -> remainingTimeoutMillis(deadlineNanos, nanoTime));
            if (metadata.metadataVersion() == metadataV2.metadataVersion()) {
                applyMetadata(controller, metadata, metadataV2);
                return;
            }
            lastMismatch = revisionMismatch(metadata, metadataV2);
        }
        throw new IllegalStateException(
                "Unable to fetch a consistent metadata snapshot after "
                        + MAX_CONSISTENT_FETCH_ATTEMPTS + " attempts",
                lastMismatch);
    }

    private static long remainingTimeoutMillis(long deadlineNanos, LongSupplier nanoTime) {
        long remainingNanos = deadlineNanos - nanoTime.getAsLong();
        if (remainingNanos <= 0L) {
            throw new IllegalStateException("Metadata refresh deadline exceeded");
        }
        return Math.max(1L, TimeUnit.NANOSECONDS.toMillis(remainingNanos));
    }

    static void applyMetadata(ClusterController controller, MetadataResponse metadata) {
        applyMetadata(controller, metadata, (MetadataV2Snapshot) null);
    }

    static void applyMetadata(
            ClusterController controller,
            MetadataResponse metadata,
            MetadataV2Response metadataV2) {
        applyMetadata(controller, metadata, metadataV2 == null ? null : snapshot(metadataV2));
    }

    private static void applyMetadata(
            ClusterController controller,
            MetadataResponse metadata,
            MetadataV2Snapshot metadataV2) {
        if (metadataV2 != null
                && metadata.metadataVersion() != metadataV2.metadataVersion) {
            throw revisionMismatch(metadata, metadataV2);
        }
        ClusterMetadata snapshot = controller.snapshot();
        snapshot.brokers.clear();
        snapshot.topics.clear();
        snapshot.metadataVersion = metadata.metadataVersion();
        if (metadataV2 != null && metadataV2.controller != null) {
            snapshot.controllerEpoch = metadataV2.controller.controllerEpoch();
        } else if (metadata.controller() != null) {
            snapshot.controllerEpoch = metadata.controller().controllerEpoch();
        }

        if (metadataV2 == null) {
            applyLegacyBrokers(snapshot.brokers, metadata.brokers());
        } else {
            applyV2Brokers(snapshot.brokers, metadataV2.brokers);
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
        if (metadataV2 != null) {
            snapshot.topicTombstones.clear();
            applyV2Tombstones(snapshot, metadataV2.tombstones);
            snapshot.retryTopicBySource.clear();
            snapshot.deadLetterTopicBySource.clear();
            applyV2ReliabilityLinks(snapshot, metadataV2.topics);
        }
        controller.installMetadata(
                new VersionedClusterMetadata(snapshot, metadata.metadataVersion()));
    }

    private static MetadataV2Snapshot fetchMetadataV2Snapshot(
            ControllerClient client,
            LongSupplier timeoutMillis) throws InterruptedException {
        List<TopicSummary> topics = new ArrayList<>();
        List<TopicTombstoneSummary> tombstones = new ArrayList<>();
        List<OpsBrokerInfo> brokers = null;
        com.minimq.protocol.message.ProtocolMessages.ControllerMetadata controller = null;
        long metadataVersion = -1L;
        int metadataFormatVersion = -1;
        String topicStartAfter = null;
        String tombstoneStartAfter = null;
        boolean topicsTruncated;
        boolean tombstonesTruncated;
        do {
            MetadataV2Response page = client.fetchMetadataV2(
                    new MetadataV2Request(
                            null,
                            topicStartAfter,
                            tombstoneStartAfter,
                            METADATA_V2_PAGE_LIMIT,
                            METADATA_V2_PAGE_LIMIT),
                    timeoutMillis.getAsLong());
            if (metadataVersion < 0L) {
                if (page.brokers().truncated()) {
                    throw new IllegalStateException("METADATA_V2 broker list is truncated");
                }
                brokers = new ArrayList<>(page.brokers().items());
                controller = page.controller();
                metadataVersion = page.metadataVersion();
                metadataFormatVersion = page.metadataFormatVersion();
            } else if (metadataVersion != page.metadataVersion()
                    || metadataFormatVersion != page.metadataFormatVersion()) {
                throw new IllegalStateException(
                        "Metadata revisions do not match while paging METADATA_V2: first="
                                + metadataVersion + ", page=" + page.metadataVersion());
            }

            topics.addAll(page.topics().items());
            tombstones.addAll(page.tombstones().items());
            topicsTruncated = page.topics().truncated();
            tombstonesTruncated = page.tombstones().truncated();
            if (topicsTruncated) {
                if (page.topics().items().isEmpty()) {
                    throw new IllegalStateException("METADATA_V2 topic page is truncated without items");
                }
                topicStartAfter = page.topics().items()
                        .get(page.topics().items().size() - 1)
                        .topicName();
            } else if (!page.topics().items().isEmpty()) {
                topicStartAfter = page.topics().items()
                        .get(page.topics().items().size() - 1)
                        .topicName();
            }
            if (tombstonesTruncated) {
                if (page.tombstones().items().isEmpty()) {
                    throw new IllegalStateException("METADATA_V2 tombstone page is truncated without items");
                }
                tombstoneStartAfter = page.tombstones().items()
                        .get(page.tombstones().items().size() - 1)
                        .topicName();
            } else if (!page.tombstones().items().isEmpty()) {
                tombstoneStartAfter = page.tombstones().items()
                        .get(page.tombstones().items().size() - 1)
                        .topicName();
            }
        } while (topicsTruncated || tombstonesTruncated);
        return new MetadataV2Snapshot(
                brokers == null ? List.of() : brokers,
                topics,
                tombstones,
                controller,
                metadataVersion,
                metadataFormatVersion);
    }

    private static MetadataResponse fetchLegacyMetadata(
            ControllerClient client,
            MetadataV2Snapshot metadataV2,
            LongSupplier timeoutMillis) throws InterruptedException {
        if (metadataV2.topics.size() <= METADATA_V2_PAGE_LIMIT) {
            return client.fetchMetadata(timeoutMillis.getAsLong());
        }
        List<TopicMetadata> topics = new ArrayList<>(metadataV2.topics.size());
        for (TopicSummary summary : metadataV2.topics) {
            MetadataResponse topicMetadata = client.fetchMetadata(
                    summary.topicName(), timeoutMillis.getAsLong());
            if (topicMetadata.metadataVersion() != metadataV2.metadataVersion) {
                throw revisionMismatch(topicMetadata, metadataV2);
            }
            if (topicMetadata.topics().size() != 1
                    || !summary.topicName().equals(topicMetadata.topics().get(0).topicName())) {
                throw new IllegalStateException(
                        "Exact metadata fetch did not return topic " + summary.topicName());
            }
            topics.add(topicMetadata.topics().get(0));
        }
        return new MetadataResponse(
                legacyBrokers(metadataV2.brokers),
                topics,
                metadataV2.controller,
                metadataV2.metadataVersion);
    }

    private static List<BrokerMetadata> legacyBrokers(List<OpsBrokerInfo> brokers) {
        return brokers.stream()
                .filter(broker -> "ACTIVE".equals(broker.state()) || "ONLINE".equals(broker.state()))
                .map(broker -> new BrokerMetadata(broker.brokerId(), broker.host(), broker.port()))
                .toList();
    }

    private static MetadataV2Snapshot snapshot(MetadataV2Response metadataV2) {
        if (metadataV2.brokers().truncated()) {
            throw new IllegalStateException("METADATA_V2 broker list is truncated");
        }
        if (metadataV2.tombstones().truncated()) {
            throw new IllegalStateException("METADATA_V2 tombstone list is truncated");
        }
        if (metadataV2.topics().truncated()) {
            throw new IllegalStateException("METADATA_V2 topic list is truncated");
        }
        return new MetadataV2Snapshot(
                metadataV2.brokers().items(),
                metadataV2.topics().items(),
                metadataV2.tombstones().items(),
                metadataV2.controller(),
                metadataV2.metadataVersion(),
                metadataV2.metadataFormatVersion());
    }

    private static IllegalStateException revisionMismatch(
            MetadataResponse metadata, MetadataV2Response metadataV2) {
        return new IllegalStateException(
                "Metadata revisions do not match: legacy=" + metadata.metadataVersion()
                        + ", v2=" + metadataV2.metadataVersion());
    }

    private static IllegalStateException revisionMismatch(
            MetadataResponse metadata, MetadataV2Snapshot metadataV2) {
        return new IllegalStateException(
                "Metadata revisions do not match: legacy=" + metadata.metadataVersion()
                        + ", v2=" + metadataV2.metadataVersion);
    }

    private record MetadataV2Snapshot(
            List<OpsBrokerInfo> brokers,
            List<TopicSummary> topics,
            List<TopicTombstoneSummary> tombstones,
            com.minimq.protocol.message.ProtocolMessages.ControllerMetadata controller,
            long metadataVersion,
            int metadataFormatVersion) {
    }

    private static void applyLegacyBrokers(
            java.util.Map<Integer, BrokerEntry> target,
            List<BrokerMetadata> brokers) {
        for (BrokerMetadata broker : brokers) {
            BrokerEntry entry = new BrokerEntry();
            entry.brokerId = broker.brokerId();
            entry.host = broker.host();
            entry.port = broker.port();
            entry.online = true;
            target.put(entry.brokerId, entry);
        }
    }

    private static void applyV2Brokers(
            java.util.Map<Integer, BrokerEntry> target,
            List<OpsBrokerInfo> brokers) {
        for (OpsBrokerInfo broker : brokers) {
            BrokerEntry entry = new BrokerEntry();
            entry.brokerId = broker.brokerId();
            entry.host = broker.host();
            entry.port = broker.port();
            entry.online = "ACTIVE".equals(broker.state()) || "ONLINE".equals(broker.state());
            entry.brokerEpoch = broker.brokerEpoch();
            target.put(entry.brokerId, entry);
        }
    }

    private static void applyV2Tombstones(
            ClusterMetadata snapshot,
            List<TopicTombstoneSummary> tombstones) {
        for (TopicTombstoneSummary summary : tombstones) {
            TopicTombstoneInfo info = summary.tombstone();
            ClusterMetadata.TopicTombstone tombstone = new ClusterMetadata.TopicTombstone();
            tombstone.topicName = summary.topicName();
            tombstone.state = info.state();
            tombstone.tombstoneVersion = info.tombstoneVersion();
            tombstone.deletedAtEpochMs = info.deletedAtEpochMs();
            tombstone.requiredBrokerIds = new TreeSet<>(info.requiredBrokerIds().items());
            tombstone.acknowledgedBrokerIds = new TreeSet<>(info.acknowledgedBrokerIds().items());
            snapshot.topicTombstones.put(tombstone.topicName, tombstone);
        }
    }

    private static void applyV2ReliabilityLinks(
            ClusterMetadata snapshot,
            List<TopicSummary> topics) {
        for (TopicSummary topic : topics) {
            if (topic.retryTopic() != null) {
                snapshot.retryTopicBySource.put(topic.topicName(), topic.retryTopic());
            }
            if (topic.deadLetterTopic() != null) {
                snapshot.deadLetterTopicBySource.put(topic.topicName(), topic.deadLetterTopic());
            }
        }
    }
}
