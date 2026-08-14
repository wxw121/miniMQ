package com.minimq.broker.controller;

import com.minimq.broker.metadata.MetadataStore;
import com.minimq.broker.metadata.ClusterMetadata;
import com.minimq.broker.metadata.ClusterMetadataRepository;
import com.minimq.broker.metadata.SwitchableClusterMetadataRepository;
import com.minimq.broker.metadata.VersionedClusterMetadata;
import com.minimq.common.error.ErrorCode;
import com.minimq.protocol.message.ProtocolMessages.BrokerHeartbeatRequest;
import com.minimq.protocol.message.ProtocolMessages.CreateTopicRequest;
import com.minimq.protocol.message.ProtocolMessages.MetadataRequest;
import com.minimq.protocol.message.ProtocolMessages.PartitionMetadata;
import com.minimq.protocol.message.ProtocolMessages.RegisterBrokerRequest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ClusterControllerTest {

    @TempDir
    Path tempDir;

    @Test
    void repositoryCasFailureDoesNotChangeControllerMemory() {
        RejectingMetadataRepository repository = new RejectingMetadataRepository();
        ClusterController controller = new ClusterController(repository);

        ClusterController.BrokerRegistration result =
                controller.registerBroker(new RegisterBrokerRequest(1, "localhost", 9093));

        assertEquals(ErrorCode.CONTROLLER_FENCED, result.errorCode());
        assertTrue(controller.snapshot().brokers.isEmpty());
    }

    @Test
    void snapshotDeepCopiesLeaderEpochFields() {
        ClusterController controller = new ClusterController(new MetadataStore(tempDir));
        controller.registerBroker(new RegisterBrokerRequest(0, "localhost", 9092));
        controller.createTopic(new CreateTopicRequest("orders", 1));
        controller.updateMetadata(metadata -> {
            var partition = metadata.topics.get("orders").partitions.get(0);
            partition.leaderEpoch = 7L;
            partition.leaderEpochStartOffset = 11L;
            partition.leaderReady = false;
            return null;
        });

        ClusterMetadata first = controller.snapshot();
        first.controllerEpoch = 99L;
        first.topics.get("orders").partitions.get(0).leaderEpoch = 99L;

        ClusterMetadata second = controller.snapshot();
        assertEquals(0L, second.controllerEpoch);
        assertEquals(7L, second.topics.get("orders").partitions.get(0).leaderEpoch);
        assertEquals(11L, second.topics.get("orders").partitions.get(0).leaderEpochStartOffset);
        assertFalse(second.topics.get("orders").partitions.get(0).leaderReady);
        assertFalse(second.topics.get("orders").partitions.get(0).activationRequired);
        assertTrue(controller.metadata(new MetadataRequest("orders")).metadataVersion() > 0L);
    }

    @Test
    void createTopicRoundRobin() {
        ClusterController controller = new ClusterController(new MetadataStore(tempDir));
        controller.registerBroker(new RegisterBrokerRequest(0, "localhost", 9092));
        controller.registerBroker(new RegisterBrokerRequest(1, "localhost", 9093));
        controller.registerBroker(new RegisterBrokerRequest(2, "localhost", 9094));

        assertEquals(ErrorCode.NONE, controller.createTopic(new CreateTopicRequest("orders", 3)));

        var metadata = controller.metadata(new MetadataRequest("orders"));
        assertEquals(3, metadata.topics().get(0).partitions().size());
        assertEquals(0, metadata.topics().get(0).partitions().get(0).leaderBrokerId());
        assertEquals(1, metadata.topics().get(0).partitions().get(1).leaderBrokerId());
        assertEquals(2, metadata.topics().get(0).partitions().get(2).leaderBrokerId());
        assertTrue(metadata.brokers().size() >= 3);
    }

    @Test
    void createTopicRejectsPortableStorageIdentityCollision() {
        ClusterController controller = new ClusterController(new MetadataStore(tempDir));
        controller.registerBroker(new RegisterBrokerRequest(0, "localhost", 9092));

        assertEquals(ErrorCode.NONE, controller.createTopic(new CreateTopicRequest("orders", 1)));
        assertEquals(
                ErrorCode.INVALID_REQUEST,
                controller.createTopic(new CreateTopicRequest("Orders", 1)));
    }

    @Test
    void deleteTopicRejectsInvalidUnknownAndNullTimeoutRequests() {
        ClusterController controller = new ClusterController(new MetadataStore(tempDir));

        assertEquals(ErrorCode.INVALID_REQUEST, controller.deleteTopic("", 1L));
        assertEquals(ErrorCode.INVALID_REQUEST, controller.deleteTopic("orders", -1L));
        assertEquals(ErrorCode.UNKNOWN_TOPIC, controller.deleteTopic("orders", 1L));
        assertThrows(
                IllegalArgumentException.class,
                () -> controller.deleteTopic("orders", 1L, null));
    }

    @Test
    void deleteTopicWithoutOnlineBrokersTransitionsDirectlyToDeleted() {
        ClusterController controller = new ClusterController(new MetadataStore(tempDir));
        assertEquals(ErrorCode.NONE, controller.registerBroker(
                new RegisterBrokerRequest(0, "localhost", 9092)).errorCode());
        assertEquals(ErrorCode.NONE, controller.createTopic(new CreateTopicRequest("orders", 1)));
        controller.updateMetadata(metadata -> {
            metadata.brokers.get(0).online = false;
            return null;
        });

        assertEquals(ErrorCode.NONE, controller.deleteTopic("orders", 123L));

        ClusterMetadata.TopicTombstone tombstone = controller.getTopicTombstone("orders");
        assertEquals("DELETED", tombstone.state);
        assertEquals(123L, tombstone.deletedAtEpochMs);
        assertTrue(tombstone.requiredBrokerIds.isEmpty());
        assertEquals(ErrorCode.NONE, controller.deleteTopic("orders", 456L));
        assertEquals(123L, controller.getTopicTombstone("orders").deletedAtEpochMs);
    }

    @Test
    void deleteTopicRemovesConsumerGroupsAndReliabilityLinks() {
        ClusterController controller = new ClusterController(new MetadataStore(tempDir));
        assertEquals(ErrorCode.NONE, controller.registerBroker(
                new RegisterBrokerRequest(0, "localhost", 9092)).errorCode());
        assertEquals(ErrorCode.NONE, controller.createTopic(new CreateTopicRequest("orders", 1)));
        assertEquals(ErrorCode.NONE, controller.createTopic(new CreateTopicRequest("retry", 1)));
        assertEquals(ErrorCode.NONE, controller.createTopic(new CreateTopicRequest("dlq", 1)));
        assertEquals(ErrorCode.NONE, controller.createTopic(new CreateTopicRequest("retry-source", 1)));
        assertEquals(ErrorCode.NONE, controller.createTopic(new CreateTopicRequest("dlq-source", 1)));
        controller.updateMetadata(metadata -> {
            ClusterMetadata.ConsumerGroupEntry group = new ClusterMetadata.ConsumerGroupEntry();
            group.groupId = "g1";
            group.topicName = "orders";
            metadata.consumerGroups.put(group.groupId, group);
            metadata.retryTopicBySource.put("orders", "retry");
            metadata.deadLetterTopicBySource.put("orders", "dlq");
            metadata.retryTopicBySource.put("retry-source", "orders");
            metadata.deadLetterTopicBySource.put("dlq-source", "orders");
            return null;
        });

        assertEquals(ErrorCode.NONE, controller.deleteTopic("orders", 123L));

        ClusterMetadata snapshot = controller.snapshot();
        assertFalse(snapshot.consumerGroups.containsKey("g1"));
        assertFalse(snapshot.retryTopicBySource.containsKey("orders"));
        assertFalse(snapshot.deadLetterTopicBySource.containsKey("orders"));
        assertFalse(snapshot.retryTopicBySource.containsValue("orders"));
        assertFalse(snapshot.deadLetterTopicBySource.containsValue("orders"));
    }

    @Test
    void topicDeletionAcknowledgementFencesIdentityAndIsIdempotent() {
        ClusterController controller = new ClusterController(new MetadataStore(tempDir));
        assertEquals(ErrorCode.NONE, controller.registerBroker(
                new RegisterBrokerRequest(0, "localhost", 9092)).errorCode());
        assertEquals(ErrorCode.NONE, controller.createTopic(new CreateTopicRequest("orders", 1)));
        assertEquals(ErrorCode.NONE, controller.deleteTopic("orders", 123L));
        ClusterMetadata.TopicTombstone tombstone = controller.getTopicTombstone("orders");
        long brokerEpoch = controller.getBroker(0).brokerEpoch;
        long controllerEpoch = controller.snapshot().controllerEpoch;

        assertEquals(ErrorCode.UNKNOWN_BROKER, controller.acknowledgeTopicDeletion(
                "orders", tombstone.tombstoneVersion, 9, brokerEpoch, controllerEpoch));
        assertEquals(ErrorCode.CONTROLLER_FENCED, controller.acknowledgeTopicDeletion(
                "orders", tombstone.tombstoneVersion, 0, brokerEpoch, controllerEpoch + 1));
        assertEquals(ErrorCode.BROKER_FENCED, controller.acknowledgeTopicDeletion(
                "orders", tombstone.tombstoneVersion, 0, brokerEpoch + 1, controllerEpoch));
        assertEquals(ErrorCode.STALE_TOMBSTONE, controller.acknowledgeTopicDeletion(
                "orders", tombstone.tombstoneVersion + 1, 0, brokerEpoch, controllerEpoch));
        assertEquals(ErrorCode.NONE, controller.acknowledgeTopicDeletion(
                "orders", tombstone.tombstoneVersion, 0, brokerEpoch, controllerEpoch));
        assertEquals("DELETED", controller.getTopicTombstone("orders").state);
        assertEquals(ErrorCode.NONE, controller.acknowledgeTopicDeletion(
                "orders", tombstone.tombstoneVersion, 0, brokerEpoch, controllerEpoch));
    }

    @Test
    void createTopicAssignsReplicasAndIsr() {
        ClusterController controller = new ClusterController(new MetadataStore(tempDir));
        controller.registerBroker(new RegisterBrokerRequest(0, "localhost", 9092));
        controller.registerBroker(new RegisterBrokerRequest(1, "localhost", 9093));
        controller.registerBroker(new RegisterBrokerRequest(2, "localhost", 9094));

        assertEquals(
                ErrorCode.NONE,
                controller.createTopic(new com.minimq.protocol.message.ProtocolMessages.CreateTopicRequest(
                        "orders", 1, 3)));

        PartitionMetadata partition = controller.metadata(new MetadataRequest("orders"))
                .topics().get(0).partitions().get(0);
        assertEquals(0, partition.leaderBrokerId());
        assertEquals(List.of(0, 1, 2), partition.replicaBrokerIds());
        assertEquals(List.of(0, 1, 2), partition.isrBrokerIds());
        assertEquals(Map.of(0, 0L, 1, 0L, 2, 0L),
                controller.snapshot().topics.get("orders").partitions.get(0).replicaLogEndOffsets);
        assertEquals(1, controller.snapshot().topics.get("orders").effectiveMinInSyncReplicas());
    }

    @Test
    void createTopicRejectsMinInSyncGreaterThanReplicationFactor() {
        ClusterController controller = new ClusterController(new MetadataStore(tempDir));
        controller.registerBroker(new RegisterBrokerRequest(0, "localhost", 9092));
        controller.registerBroker(new RegisterBrokerRequest(1, "localhost", 9093));
        assertEquals(
                ErrorCode.INVALID_REQUEST,
                controller.createTopic(new CreateTopicRequest("orders", 1, 2, 3)));
        assertEquals(
                ErrorCode.NONE,
                controller.createTopic(new CreateTopicRequest("orders", 1, 2, 2)));
        assertEquals(2, controller.getTopic("orders").minInSyncReplicas);
    }

    @Test
    void replicaLogEndOffsetUpdatesAreMonotonic() {
        ClusterController controller = controllerWithReplicatedTopic();

        assertEquals(ErrorCode.NONE, controller.updateReplicaLogEndOffset("orders", 0, 1, 8L));
        assertEquals(ErrorCode.NONE, controller.updateReplicaLogEndOffset("orders", 0, 1, 3L));

        assertEquals(8L, controller.replicaLogEndOffset("orders", 0, 1));
    }

    @Test
    void equalReplicaLogEndOffsetDoesNotPersistAgain() {
        CountingMetadataStore store = new CountingMetadataStore(tempDir);
        ClusterController controller = new ClusterController(store);
        controller.registerBroker(new RegisterBrokerRequest(0, "localhost", 9092));
        controller.createTopic(new CreateTopicRequest("orders", 1));
        controller.updateReplicaLogEndOffset("orders", 0, 0, 8L);
        int savesAfterIncrease = store.saveCount;

        assertEquals(ErrorCode.NONE,
                controller.updateReplicaLogEndOffset("orders", 0, 0, 8L));

        assertEquals(savesAfterIncrease, store.saveCount);
    }

    @Test
    void replicaLogEndOffsetRejectsInvalidUpdates() {
        ClusterController controller = controllerWithReplicatedTopic();

        assertEquals(ErrorCode.INVALID_REQUEST,
                controller.updateReplicaLogEndOffset("orders", 0, 1, -1L));
        assertEquals(ErrorCode.INVALID_REQUEST,
                controller.updateReplicaLogEndOffset("missing", 0, 1, 1L));
        assertEquals(ErrorCode.UNKNOWN_BROKER,
                controller.updateReplicaLogEndOffset("orders", 0, 9, 1L));
    }

    @Test
    void replicaStateV2WaitsForAWithinToleranceIsrMemberBeforePublishingWatermark() {
        ClusterController controller = controllerWithReplicatedTopic();

        assertEquals(
                ErrorCode.NONE,
                controller.updateReplicaStateV2(
                        "orders", 0, 0, 1L, 0L, 0L,
                        5L, 5L, -1L));

        ClusterMetadata.PartitionEntry incomplete =
                controller.snapshot().topics.get("orders").partitions.get(0);
        assertEquals(5L, incomplete.replicaProgress.get(0).physicalLogEndOffset);
        assertEquals(5L, incomplete.replicaProgress.get(0).visibleLogEndOffset);
        assertEquals(-1L, incomplete.lastCompleteWatermark.physicalHighWatermark);

        assertEquals(
                ErrorCode.NONE,
                controller.updateReplicaStateV2(
                        "orders", 0, 1, 1L, 0L, 0L,
                        5L, 5L, -1L));

        ClusterMetadata.PartitionEntry complete =
                controller.snapshot().topics.get("orders").partitions.get(0);
        assertEquals(5L, complete.lastCompleteWatermark.physicalHighWatermark);
        assertEquals(5L, complete.lastCompleteWatermark.visibleHighWatermark);
        assertEquals(-1L, complete.lastCompleteWatermark.nextVisibleAtEpochMs);
        assertEquals(0L, complete.lastCompleteWatermark.controllerEpoch);
        assertEquals(0L, complete.lastCompleteWatermark.leaderEpoch);
    }

    @Test
    void replicaStateV2ReconcilesIsrFromPhysicalProgress() {
        ClusterController controller = controllerWithReplicatedTopic();

        assertEquals(
                ErrorCode.NONE,
                controller.updateReplicaStateV2(
                        "orders", 0, 0, 1L, 0L, 0L,
                        20L, 20L, -1L));
        ClusterMetadata.PartitionEntry shrunk =
                controller.snapshot().topics.get("orders").partitions.get(0);
        assertEquals(List.of(0), shrunk.isrBrokerIds);
        assertEquals(20L, shrunk.lastCompleteWatermark.physicalHighWatermark);

        assertEquals(
                ErrorCode.NONE,
                controller.updateReplicaStateV2(
                        "orders", 0, 1, 1L, 0L, 0L,
                        20L, 20L, -1L));
        ClusterMetadata.PartitionEntry expanded =
                controller.snapshot().topics.get("orders").partitions.get(0);
        assertEquals(List.of(0, 1), expanded.isrBrokerIds);
        assertEquals(20L, expanded.lastCompleteWatermark.physicalHighWatermark);
        assertEquals(20L, expanded.lastCompleteWatermark.visibleHighWatermark);
    }

    @Test
    void replicaStateV2FencesUnknownBrokerAndStaleEpochsWithoutMutation() {
        ClusterController controller = controllerWithReplicatedTopic();

        assertEquals(
                ErrorCode.CONTROLLER_FENCED,
                controller.updateReplicaStateV2(
                        "orders", 0, 0, 1L, 1L, 0L,
                        5L, 5L, -1L));
        assertEquals(
                ErrorCode.UNKNOWN_BROKER,
                controller.updateReplicaStateV2(
                        "orders", 0, 99, 1L, 0L, 0L,
                        5L, 5L, -1L));
        assertEquals(
                ErrorCode.BROKER_FENCED,
                controller.updateReplicaStateV2(
                        "orders", 0, 0, 0L, 0L, 0L,
                        5L, 5L, -1L));
        assertEquals(
                ErrorCode.STALE_LEADER_EPOCH,
                controller.updateReplicaStateV2(
                        "orders", 0, 0, 1L, 0L, 1L,
                        5L, 5L, -1L));

        ClusterMetadata.PartitionEntry partition =
                controller.snapshot().topics.get("orders").partitions.get(0);
        assertEquals(-1L, partition.replicaProgress.get(0).visibleLogEndOffset);
        assertEquals(-1L, partition.lastCompleteWatermark.physicalHighWatermark);
    }

    @Test
    void replicaStateV2ValidationChecksIdentityBeforeControllerEpoch() {
        ClusterController controller = controllerWithReplicatedTopic();

        assertEquals(
                ErrorCode.UNKNOWN_BROKER,
                controller.updateReplicaStateV2(
                        "orders", 0, 99, 1L, 99L, 0L,
                        5L, 5L, -1L));
        assertEquals(
                ErrorCode.UNKNOWN_TOPIC,
                controller.updateReplicaStateV2(
                        "missing", 0, 0, 1L, 99L, 0L,
                        5L, 5L, -1L));
        assertEquals(
                ErrorCode.UNKNOWN_PARTITION,
                controller.updateReplicaStateV2(
                        "orders", 99, 0, 1L, 99L, 0L,
                        5L, 5L, -1L));
        assertEquals(
                ErrorCode.BROKER_FENCED,
                controller.updateReplicaStateV2(
                        "orders", 0, 0, 0L, 99L, 0L,
                        5L, 5L, -1L));
    }

    @Test
    void replicaFetchV2FencesIdentityAndEpochs() {
        ClusterController controller = controllerWithReplicatedTopic();
        assertEquals(ErrorCode.NONE,
                controller.registerBroker(new RegisterBrokerRequest(2, "localhost", 9094)).errorCode());

        assertEquals(ErrorCode.NONE,
                controller.validateReplicaFetchV2("orders", 0, 0, 1, 1L, 0L, 0L));
        assertEquals(ErrorCode.UNKNOWN_BROKER,
                controller.validateReplicaFetchV2("orders", 0, 0, 99, 1L, 0L, 0L));
        assertEquals(ErrorCode.UNKNOWN_TOPIC,
                controller.validateReplicaFetchV2("missing", 0, 0, 1, 1L, 0L, 0L));
        assertEquals(ErrorCode.UNKNOWN_PARTITION,
                controller.validateReplicaFetchV2("orders", 99, 0, 1, 1L, 0L, 0L));
        assertEquals(ErrorCode.NOT_LEADER,
                controller.validateReplicaFetchV2("orders", 0, 1, 0, 1L, 0L, 0L));
        assertEquals(ErrorCode.INVALID_REQUEST,
                controller.validateReplicaFetchV2("orders", 0, 0, 0, 1L, 0L, 0L));
        assertEquals(ErrorCode.INVALID_REQUEST,
                controller.validateReplicaFetchV2("orders", 0, 0, 2, 1L, 0L, 0L));
        assertEquals(ErrorCode.BROKER_FENCED,
                controller.validateReplicaFetchV2("orders", 0, 0, 1, 0L, 0L, 0L));
        assertEquals(ErrorCode.CONTROLLER_FENCED,
                controller.validateReplicaFetchV2("orders", 0, 0, 1, 1L, 1L, 0L));
        assertEquals(ErrorCode.STALE_LEADER_EPOCH,
                controller.validateReplicaFetchV2("orders", 0, 0, 1, 1L, 0L, 1L));

        assertEquals(ErrorCode.NONE, controller.markBrokerOffline(1));
        assertEquals(ErrorCode.BROKER_FENCED,
                controller.validateReplicaFetchV2("orders", 0, 0, 1, 1L, 0L, 0L));
    }

    @Test
    void replicaStateV2ClearsWatermarkWhenCurrentIsrReportSetBecomesIncomplete() {
        ClusterController controller = controllerWithReplicatedTopic();
        assertEquals(
                ErrorCode.NONE,
                controller.updateReplicaStateV2(
                        "orders", 0, 1, 1L, 0L, 0L,
                        10L, 10L, -1L));
        assertEquals(
                ErrorCode.NONE,
                controller.updateReplicaStateV2(
                        "orders", 0, 0, 1L, 0L, 0L,
                        12L, 12L, -1L));
        assertEquals(10L, watermark(controller).physicalHighWatermark);

        controller.updateMetadata(metadata -> {
            metadata.brokers.get(1).brokerEpoch = 2L;
            return null;
        });

        assertEquals(
                ErrorCode.NONE,
                controller.updateReplicaStateV2(
                        "orders", 0, 0, 1L, 0L, 0L,
                        13L, 13L, -1L));

        assertInvalidatedWatermark(controller, 10L, 10L, -1L);
    }

    @Test
    void leaderElectionInvalidatesCompleteWatermarkUntilNewLeaderReports() {
        ClusterController controller = controllerWithReplicatedTopic();
        assertEquals(
                ErrorCode.NONE,
                controller.updateReplicaStateV2(
                        "orders", 0, 1, 1L, 0L, 0L,
                        10L, 10L, -1L));
        assertEquals(
                ErrorCode.NONE,
                controller.updateReplicaStateV2(
                        "orders", 0, 0, 1L, 0L, 0L,
                        12L, 12L, -1L));
        assertEquals(10L, watermark(controller).physicalHighWatermark);

        assertEquals(ErrorCode.NONE, controller.markBrokerOffline(0));

        assertInvalidatedWatermark(controller, 10L, 10L, -1L);
    }

    @Test
    void markBrokerOfflineElectsNextIsrLeader() {
        ClusterController controller = new ClusterController(new MetadataStore(tempDir));
        controller.registerBroker(new RegisterBrokerRequest(0, "localhost", 9092));
        controller.registerBroker(new RegisterBrokerRequest(1, "localhost", 9093));
        controller.registerBroker(new RegisterBrokerRequest(2, "localhost", 9094));
        controller.createTopic(new com.minimq.protocol.message.ProtocolMessages.CreateTopicRequest(
                "orders", 1, 3));

        assertEquals(ErrorCode.NONE, controller.markBrokerOffline(0));

        PartitionMetadata partition = controller.metadata(new MetadataRequest("orders"))
                .topics().get(0).partitions().get(0);
        assertEquals(1, partition.leaderBrokerId());
        assertEquals(List.of(1, 2), partition.isrBrokerIds());
    }

    @Test
    void markBrokerOfflineBreaksLeoTiesByLowestBrokerId() {
        ClusterController controller = new ClusterController(new MetadataStore(tempDir));
        controller.registerBroker(new RegisterBrokerRequest(0, "localhost", 9092));
        controller.registerBroker(new RegisterBrokerRequest(1, "localhost", 9093));
        controller.registerBroker(new RegisterBrokerRequest(2, "localhost", 9094));
        controller.createTopic(new CreateTopicRequest("orders", 1, 3));
        controller.updateMetadata(metadata -> {
            var partition = metadata.topics.get("orders").partitions.get(0);
            partition.replicaBrokerIds = new ArrayList<>(List.of(2, 1, 0));
            partition.isrBrokerIds = new ArrayList<>(List.of(2, 1, 0));
            partition.leaderBrokerId = 2;
            return null;
        });

        assertEquals(ErrorCode.NONE, controller.markBrokerOffline(2));

        PartitionMetadata partition = controller.metadata(new MetadataRequest("orders"))
                .topics().get(0).partitions().get(0);
        assertEquals(0, partition.leaderBrokerId());
        assertEquals(List.of(1, 0), partition.isrBrokerIds());
        assertEquals(1L, partition.leaderEpoch());
        assertFalse(partition.leaderReady());
    }

    @Test
    void markBrokerOfflineElectsHighestLeoIsrReplica() {
        ClusterController controller = new ClusterController(new MetadataStore(tempDir));
        controller.registerBroker(new RegisterBrokerRequest(0, "localhost", 9092));
        controller.registerBroker(new RegisterBrokerRequest(1, "localhost", 9093));
        controller.registerBroker(new RegisterBrokerRequest(2, "localhost", 9094));
        controller.createTopic(new CreateTopicRequest("orders", 1, 3));
        controller.updateMetadata(metadata -> {
            var partition = metadata.topics.get("orders").partitions.get(0);
            partition.replicaBrokerIds = new ArrayList<>(List.of(2, 0, 1));
            partition.isrBrokerIds = new ArrayList<>(List.of(2, 0, 1));
            partition.leaderBrokerId = 2;
            return null;
        });
        controller.updateReplicaLogEndOffset("orders", 0, 0, 9L);
        controller.updateReplicaLogEndOffset("orders", 0, 1, 12L);

        assertEquals(ErrorCode.NONE, controller.markBrokerOffline(2));

        PartitionMetadata partition = controller.metadata(new MetadataRequest("orders"))
                .topics().get(0).partitions().get(0);
        assertEquals(1, partition.leaderBrokerId());
    }

    @Test
    void freezesLeaderCandidateThatMissesActivationDeadlineWithoutReelection() {
        AtomicLong now = new AtomicLong();
        ClusterController controller = new ClusterController(
                new MetadataStore(tempDir), 5_000L, now::get);
        controller.registerBroker(new RegisterBrokerRequest(0, "localhost", 9092));
        controller.registerBroker(new RegisterBrokerRequest(1, "localhost", 9093));
        controller.createTopic(new CreateTopicRequest("orders", 1, 2));
        assertEquals(ErrorCode.NONE, controller.markBrokerOffline(0));

        assertEquals(0, controller.expireLeaderActivationTimeouts(2_000));
        now.set(TimeUnit.MILLISECONDS.toNanos(1_999));
        assertEquals(0, controller.expireLeaderActivationTimeouts(2_000));
        now.set(TimeUnit.MILLISECONDS.toNanos(2_001));
        assertEquals(1, controller.expireLeaderActivationTimeouts(2_000));

        PartitionMetadata partition = controller.metadata(new MetadataRequest("orders"))
                .topics().get(0).partitions().get(0);
        assertEquals(1, partition.leaderBrokerId());
        assertTrue(partition.frozen());
        assertFalse(partition.leaderReady());
        assertTrue(controller.snapshot().topics.get("orders").partitions.get(0).activationRequired);
        assertThrows(IllegalArgumentException.class,
                () -> controller.expireLeaderActivationTimeouts(0));
    }

    @Test
    void activationTimeoutFreezeRestoreRequiresLeaderActivation() {
        AtomicLong now = new AtomicLong();
        ClusterController controller = new ClusterController(
                new MetadataStore(tempDir), 5_000L, now::get);
        controller.registerBroker(new RegisterBrokerRequest(0, "localhost", 9092));
        controller.registerBroker(new RegisterBrokerRequest(1, "localhost", 9093));
        controller.createTopic(new CreateTopicRequest("orders", 1, 2));
        assertEquals(ErrorCode.NONE, controller.markBrokerOffline(0));

        assertEquals(0, controller.expireLeaderActivationTimeouts(2_000));
        now.set(TimeUnit.MILLISECONDS.toNanos(2_001));
        assertEquals(1, controller.expireLeaderActivationTimeouts(2_000));
        now.set(TimeUnit.MILLISECONDS.toNanos(7_002));
        assertTrue(controller.expireBrokerLease(1, 1L));

        var recovered = controller.registerBroker(new RegisterBrokerRequest(1, "localhost", 9093));
        assertEquals(ErrorCode.NONE, recovered.errorCode());

        PartitionMetadata restored = controller.metadata(new MetadataRequest("orders"))
                .topics().get(0).partitions().get(0);
        assertFalse(restored.frozen());
        assertFalse(restored.leaderReady());
        assertEquals(List.of(1), restored.isrBrokerIds());
        assertFalse(controller.snapshot().topics.get("orders").partitions.get(0).activationRequired);
    }

    @Test
    void noteCommittedLeaderReadyUnblocksLocalEpochValidation() {
        ClusterController controller = new ClusterController(new MetadataStore(tempDir));
        controller.registerBroker(new RegisterBrokerRequest(0, "localhost", 9092));
        controller.createTopic(new CreateTopicRequest("orders", 1));
        controller.updateMetadata(metadata -> {
            var partition = metadata.topics.get("orders").partitions.get(0);
            partition.leaderReady = false;
            return null;
        });

        assertEquals(ErrorCode.LEADER_NOT_READY, controller.validateLeaderEpoch("orders", 0, 0L));
        controller.noteCommittedLeaderReady("orders", 0, 99L);
        assertEquals(ErrorCode.LEADER_NOT_READY, controller.validateLeaderEpoch("orders", 0, 0L));
        controller.noteCommittedLeaderReady("orders", 0, 0L);
        assertEquals(ErrorCode.NONE, controller.validateLeaderEpoch("orders", 0, 0L));
    }

    @Test
    void noteCommittedLeaderReadyIgnoresFrozenPartitions() {
        ClusterController controller = new ClusterController(new MetadataStore(tempDir));
        controller.registerBroker(new RegisterBrokerRequest(0, "localhost", 9092));
        controller.createTopic(new CreateTopicRequest("orders", 1));
        controller.updateMetadata(metadata -> {
            var partition = metadata.topics.get("orders").partitions.get(0);
            partition.frozen = true;
            partition.leaderReady = false;
            partition.isrBrokerIds = new ArrayList<>();
            return null;
        });

        controller.noteCommittedLeaderReady("orders", 0, 0L);
        assertEquals(ErrorCode.LEADER_NOT_READY, controller.validateLeaderEpoch("orders", 0, 0L));
        assertTrue(controller.snapshot().topics.get("orders").partitions.get(0).frozen);
    }

    @Test
    void failedLeaderActivationFreezesWithLastCompleteWatermark() {
        AtomicLong now = new AtomicLong();
        ClusterController controller = new ClusterController(
                new MetadataStore(tempDir), 5_000L, now::get);
        controller.registerBroker(new RegisterBrokerRequest(0, "localhost", 9092));
        controller.registerBroker(new RegisterBrokerRequest(1, "localhost", 9093));
        controller.createTopic(new CreateTopicRequest("orders", 1, 2));
        assertEquals(ErrorCode.NONE,
                controller.updateReplicaStateV2(
                        "orders", 0, 1, 1L, 0L, 0L, 12L, 12L, -1L));
        assertEquals(ErrorCode.NONE,
                controller.updateReplicaStateV2(
                        "orders", 0, 0, 1L, 0L, 0L, 12L, 12L, -1L));
        assertEquals(12L, watermark(controller).physicalHighWatermark);
        assertEquals(12L, watermark(controller).visibleHighWatermark);

        assertEquals(ErrorCode.NONE, controller.markBrokerOffline(0));
        assertEquals(0, controller.expireLeaderActivationTimeouts(2_000));
        now.set(TimeUnit.MILLISECONDS.toNanos(2_001));
        assertEquals(1, controller.expireLeaderActivationTimeouts(2_000));

        ClusterMetadata.PartitionEntry partition =
                controller.snapshot().topics.get("orders").partitions.get(0);
        assertTrue(partition.frozen);
        assertEquals(12L, partition.lastCompleteWatermark.physicalHighWatermark);
        assertEquals(12L, partition.lastCompleteWatermark.visibleHighWatermark);
        assertEquals(-1L, partition.lastCompleteWatermark.nextVisibleAtEpochMs);
    }

    @Test
    void markBrokerOfflineReExpandsCaughtUpReplicaBeforeElection() {
        ClusterController controller = new ClusterController(new MetadataStore(tempDir));
        controller.registerBroker(new RegisterBrokerRequest(0, "localhost", 9092));
        controller.registerBroker(new RegisterBrokerRequest(1, "localhost", 9093));
        controller.registerBroker(new RegisterBrokerRequest(2, "localhost", 9094));
        controller.createTopic(new CreateTopicRequest("orders", 1, 3));
        controller.updateMetadata(metadata -> {
            var partition = metadata.topics.get("orders").partitions.get(0);
            partition.replicaBrokerIds = new ArrayList<>(List.of(2, 0, 1));
            partition.isrBrokerIds = new ArrayList<>(List.of(2, 0));
            partition.leaderBrokerId = 2;
            return null;
        });
        controller.updateReplicaLogEndOffset("orders", 0, 0, 9L);
        controller.updateReplicaLogEndOffset("orders", 0, 1, 12L);

        assertEquals(ErrorCode.NONE, controller.markBrokerOffline(2));
        assertEquals(1, controller.getLeaderBrokerId("orders", 0));
    }

    @Test
    void laggingLiveReplicaShrinksOutOfIsr() {
        ClusterController controller = controllerWithReplicatedTopic();

        assertEquals(ErrorCode.NONE, controller.updateReplicaLogEndOffset("orders", 0, 0, 20L));
        assertEquals(ErrorCode.NONE, controller.updateReplicaLogEndOffset("orders", 0, 1, 5L));

        PartitionMetadata partition = controller.metadata(new MetadataRequest("orders"))
                .topics().get(0).partitions().get(0);
        assertEquals(List.of(0), partition.isrBrokerIds());
        assertEquals(List.of(0, 1), partition.replicaBrokerIds());
    }

    @Test
    void caughtUpLiveReplicaReExpandsIntoIsrInReplicaOrder() {
        ClusterController controller = controllerWithReplicatedTopic();
        controller.updateMetadata(metadata -> {
            var partition = metadata.topics.get("orders").partitions.get(0);
            partition.isrBrokerIds = new ArrayList<>(List.of(0));
            partition.replicaLogEndOffsets.put(0, 10L);
            partition.replicaLogEndOffsets.put(1, 5L);
            return null;
        });

        assertEquals(ErrorCode.NONE, controller.updateReplicaLogEndOffset("orders", 0, 1, 10L));

        PartitionMetadata partition = controller.metadata(new MetadataRequest("orders"))
                .topics().get(0).partitions().get(0);
        assertEquals(List.of(0, 1), partition.isrBrokerIds());
    }

    @Test
    void rejectsTopicWithoutPositivePartitions() {
        ClusterController controller = new ClusterController(new MetadataStore(tempDir));
        controller.registerBroker(new RegisterBrokerRequest(0, "localhost", 9092));

        assertEquals(ErrorCode.INVALID_REQUEST, controller.createTopic(new CreateTopicRequest("empty", 0)));
        assertTrue(controller.metadata(new MetadataRequest("empty")).topics().isEmpty());
    }

    @Test
    void markBrokerOfflineFreezesLastIsrLeader() {
        ClusterController controller = new ClusterController(new MetadataStore(tempDir));
        controller.registerBroker(new RegisterBrokerRequest(0, "localhost", 9092));
        controller.createTopic(new com.minimq.protocol.message.ProtocolMessages.CreateTopicRequest(
                "orders", 1, 1));

        assertEquals(ErrorCode.NONE, controller.markBrokerOffline(0));
        assertFalse(controller.getBroker(0).online);
        PartitionMetadata frozen = controller.metadata(new MetadataRequest("orders"))
                .topics().get(0).partitions().get(0);
        assertTrue(frozen.frozen());
        assertTrue(frozen.isrBrokerIds().isEmpty());
        assertEquals(0, frozen.leaderBrokerId());
        assertFalse(frozen.leaderReady());
    }

    @Test
    void normalizeDoesNotRestoreOfflineReplicaToIsr() {
        ClusterController controller = new ClusterController(new MetadataStore(tempDir));
        controller.registerBroker(new RegisterBrokerRequest(0, "localhost", 9092));
        controller.registerBroker(new RegisterBrokerRequest(1, "localhost", 9093));
        controller.createTopic(new com.minimq.protocol.message.ProtocolMessages.CreateTopicRequest(
                "orders", 1, 2));

        assertEquals(ErrorCode.NONE, controller.markBrokerOffline(1));
        assertEquals(List.of(0), controller.getIsrBrokerIds("orders", 0));
        controller.metadata(new MetadataRequest("orders"));
        assertEquals(List.of(0), controller.getIsrBrokerIds("orders", 0));
    }

    @Test
    void snapshotDoesNotExposeMutableMetadata() {
        ClusterController controller = new ClusterController(new MetadataStore(tempDir));
        controller.registerBroker(new RegisterBrokerRequest(0, "localhost", 9092));
        controller.createTopic(new CreateTopicRequest("orders", 1));

        var snapshot = controller.snapshot();
        snapshot.topics.get("orders").partitions.get(0).replicaLogEndOffsets.put(0, 99L);
        snapshot.topics.clear();
        snapshot.brokers.clear();

        assertEquals(1, controller.metadata(new MetadataRequest("orders")).topics().size());
        assertEquals(1, controller.metadata(new MetadataRequest("orders")).brokers().size());
        assertEquals(0L, controller.replicaLogEndOffset("orders", 0, 0));
    }

    @Test
    void registrationAssignsEpochAndRejectsDuplicateActiveBroker() {
        AtomicLong now = new AtomicLong();
        ClusterController controller = new ClusterController(new MetadataStore(tempDir), 5_000L, now::get);

        var first = controller.registerBroker(new RegisterBrokerRequest(0, "localhost", 9092));

        assertEquals(ErrorCode.NONE, first.errorCode());
        assertEquals(1L, first.response().brokerEpoch());
        assertEquals(5_000L, first.response().leaseTimeoutMs());
        assertEquals(ErrorCode.BROKER_ID_IN_USE,
                controller.registerBroker(new RegisterBrokerRequest(0, "localhost", 9093)).errorCode());
        assertEquals(ErrorCode.NONE,
                controller.heartbeatBroker(new BrokerHeartbeatRequest(0, 1L)));
        assertEquals(ErrorCode.BROKER_FENCED,
                controller.heartbeatBroker(new BrokerHeartbeatRequest(0, 0L)));
    }

    @Test
    void expiringFollowerRemovesItFromIsrWithoutChangingLeader() {
        AtomicLong now = new AtomicLong();
        ClusterController controller = new ClusterController(new MetadataStore(tempDir), 5_000L, now::get);
        controller.registerBroker(new RegisterBrokerRequest(0, "localhost", 9092));
        controller.registerBroker(new RegisterBrokerRequest(1, "localhost", 9093));
        controller.createTopic(new CreateTopicRequest("orders", 1, 2));
        now.set(TimeUnit.MILLISECONDS.toNanos(4_000));
        assertEquals(ErrorCode.NONE, controller.heartbeatBroker(new BrokerHeartbeatRequest(0, 1L)));

        now.set(TimeUnit.MILLISECONDS.toNanos(5_001));

        assertEquals(1, controller.expireOverdueBrokerLeases());
        assertFalse(controller.getBroker(1).online);
        assertEquals(0, controller.getLeaderBrokerId("orders", 0));
        assertEquals(List.of(0), controller.getIsrBrokerIds("orders", 0));
    }

    @Test
    void expiringLeaderElectsHighestLeoOnlineIsrReplica() {
        AtomicLong now = new AtomicLong();
        ClusterController controller = new ClusterController(new MetadataStore(tempDir), 5_000L, now::get);
        controller.registerBroker(new RegisterBrokerRequest(0, "localhost", 9092));
        controller.registerBroker(new RegisterBrokerRequest(1, "localhost", 9093));
        controller.registerBroker(new RegisterBrokerRequest(2, "localhost", 9094));
        controller.createTopic(new CreateTopicRequest("orders", 1, 3));
        controller.updateMetadata(metadata -> {
            var partition = metadata.topics.get("orders").partitions.get(0);
            partition.replicaBrokerIds = new ArrayList<>(List.of(2, 0, 1));
            partition.isrBrokerIds = new ArrayList<>(List.of(2, 0, 1));
            partition.leaderBrokerId = 2;
            partition.replicaLogEndOffsets.put(2, 12L);
            partition.replicaLogEndOffsets.put(0, 9L);
            partition.replicaLogEndOffsets.put(1, 12L);
            return null;
        });
        now.set(TimeUnit.MILLISECONDS.toNanos(4_000));
        controller.heartbeatBroker(new BrokerHeartbeatRequest(0, 1L));
        controller.heartbeatBroker(new BrokerHeartbeatRequest(1, 1L));

        now.set(TimeUnit.MILLISECONDS.toNanos(5_001));

        assertEquals(1, controller.expireOverdueBrokerLeases());
        assertEquals(1, controller.getLeaderBrokerId("orders", 0));
        assertEquals(List.of(0, 1), controller.getIsrBrokerIds("orders", 0));
    }

    @Test
    void expiringSoleLeaderFreezesUntilOriginalBrokerRegistersAgain() {
        AtomicLong now = new AtomicLong();
        ClusterController controller = new ClusterController(new MetadataStore(tempDir), 5_000L, now::get);
        controller.registerBroker(new RegisterBrokerRequest(0, "localhost", 9092));
        controller.createTopic(new CreateTopicRequest("orders", 1, 1));

        now.set(TimeUnit.MILLISECONDS.toNanos(5_001));

        assertEquals(1, controller.expireOverdueBrokerLeases());
        PartitionMetadata frozen = controller.metadata(new MetadataRequest("orders"))
                .topics().get(0).partitions().get(0);
        assertTrue(frozen.frozen());
        assertTrue(frozen.isrBrokerIds().isEmpty());
        assertEquals(0, frozen.leaderBrokerId());
        assertFalse(controller.getBroker(0).online);

        var recovered = controller.registerBroker(new RegisterBrokerRequest(0, "localhost", 9092));

        assertEquals(ErrorCode.NONE, recovered.errorCode());
        assertEquals(2L, recovered.response().brokerEpoch());
        PartitionMetadata active = controller.metadata(new MetadataRequest("orders"))
                .topics().get(0).partitions().get(0);
        assertFalse(active.frozen());
        assertTrue(active.leaderReady());
        assertEquals(List.of(0), active.isrBrokerIds());
        assertFalse(controller.snapshot().topics.get("orders").partitions.get(0).activationRequired);
    }

    @Test
    void staleEpochExpiryCannotOfflineNewRegistration() {
        AtomicLong now = new AtomicLong();
        ClusterController controller = new ClusterController(new MetadataStore(tempDir), 5_000L, now::get);
        controller.registerBroker(new RegisterBrokerRequest(0, "localhost", 9092));
        now.set(TimeUnit.MILLISECONDS.toNanos(5_001));
        var second = controller.registerBroker(new RegisterBrokerRequest(0, "localhost", 9093));

        assertEquals(2L, second.response().brokerEpoch());
        assertFalse(controller.expireBrokerLease(0, 1L));
        assertTrue(controller.getBroker(0).online);
        assertEquals(2L, controller.getBroker(0).brokerEpoch);
        assertEquals(9093, controller.getBroker(0).port);
    }

    @Test
    void staleExpiryCandidateCannotOverrideHeartbeatRenewal() {
        AtomicLong now = new AtomicLong();
        ClusterController controller = new ClusterController(new MetadataStore(tempDir), 5_000L, now::get);
        controller.registerBroker(new RegisterBrokerRequest(0, "localhost", 9092));
        now.set(TimeUnit.MILLISECONDS.toNanos(4_000));
        assertEquals(ErrorCode.NONE,
                controller.heartbeatBroker(new BrokerHeartbeatRequest(0, 1L)));

        now.set(TimeUnit.MILLISECONDS.toNanos(5_001));

        assertFalse(controller.expireBrokerLease(0, 1L));
        assertTrue(controller.getBroker(0).online);
    }

    @Test
    void loadedOnlineBrokerGetsOneLeaseTimeoutGracePeriod() {
        AtomicLong now = new AtomicLong();
        MetadataStore store = new MetadataStore(tempDir);
        ClusterController first = new ClusterController(store, 5_000L, now::get);
        first.registerBroker(new RegisterBrokerRequest(0, "localhost", 9092));
        now.set(TimeUnit.MILLISECONDS.toNanos(10_000));

        ClusterController restarted = new ClusterController(store, 5_000L, now::get);
        now.set(TimeUnit.MILLISECONDS.toNanos(14_999));
        assertEquals(0, restarted.expireOverdueBrokerLeases());
        assertEquals(ErrorCode.NONE,
                restarted.heartbeatBroker(new BrokerHeartbeatRequest(0, 1L)));
        now.set(TimeUnit.MILLISECONDS.toNanos(20_000));
        assertEquals(1, restarted.expireOverdueBrokerLeases());
        assertFalse(restarted.getBroker(0).online);
    }

    @Test
    void recoveryWindowAllowsReregistrationAndOnlyExpiresMissingBrokersAfterGrace() {
        AtomicLong now = new AtomicLong();
        ClusterController controller = new ClusterController(new MetadataStore(tempDir), 5_000L, now::get);
        controller.registerBroker(new RegisterBrokerRequest(0, "localhost", 9092));
        controller.registerBroker(new RegisterBrokerRequest(1, "localhost", 9093));

        controller.beginRecovery(2_000L);
        assertTrue(controller.isRecovering());
        assertEquals(ErrorCode.CONTROLLER_NOT_READY,
                controller.createTopic(new CreateTopicRequest("blocked", 1, 1)));
        assertEquals(ErrorCode.NONE,
                controller.registerBroker(new RegisterBrokerRequest(0, "localhost", 9092)).errorCode());

        now.set(TimeUnit.MILLISECONDS.toNanos(1_999));
        assertFalse(controller.advanceRecoveryWindow());
        assertTrue(controller.getBroker(0).online);
        assertTrue(controller.getBroker(1).online);

        now.set(TimeUnit.MILLISECONDS.toNanos(2_001));
        assertTrue(controller.advanceRecoveryWindow());
        assertFalse(controller.isRecovering());
        assertTrue(controller.getBroker(0).online);
        assertFalse(controller.getBroker(1).online);
    }

    @Test
    void recoveryWindowRejectsTopicDeletion() {
        ClusterController controller = new ClusterController(new MetadataStore(tempDir));
        controller.registerBroker(new RegisterBrokerRequest(0, "localhost", 9092));
        assertEquals(ErrorCode.NONE, controller.createTopic(new CreateTopicRequest("orders", 1, 1)));

        controller.beginRecovery(2_000L);

        assertEquals(ErrorCode.CONTROLLER_NOT_READY, controller.deleteTopic("orders", 1L));
        assertEquals(1, controller.metadata(new MetadataRequest("orders")).topics().size());
        assertFalse(controller.snapshot().topicTombstones.containsKey("orders"));
    }

    @Test
    void recoveryWindowTreatsExactEpochHeartbeatAsRecoveryConfirmation() {
        AtomicLong now = new AtomicLong();
        ClusterController controller = new ClusterController(
                new MetadataStore(tempDir), 5_000L, now::get);
        controller.registerBroker(new RegisterBrokerRequest(0, "localhost", 9092));
        controller.registerBroker(new RegisterBrokerRequest(1, "localhost", 9093));

        controller.beginRecovery(2_000L);
        assertEquals(ErrorCode.NONE,
                controller.heartbeatBroker(new BrokerHeartbeatRequest(0, 1L)));

        now.set(TimeUnit.MILLISECONDS.toNanos(2_001));
        assertTrue(controller.advanceRecoveryWindow());
        assertTrue(controller.getBroker(0).online);
        assertFalse(controller.getBroker(1).online);
    }

    @Test
    void installedOnlineBrokerCanRenewItsPersistedLeaseDuringControllerRecovery() {
        AtomicLong now = new AtomicLong();
        MetadataStore shared = new MetadataStore(tempDir.resolve("shared"));
        ClusterController previousController = new ClusterController(shared, 5_000L, now::get);
        assertEquals(ErrorCode.NONE, previousController.registerBroker(
                new RegisterBrokerRequest(0, "localhost", 9092)).errorCode());

        SwitchableClusterMetadataRepository repository =
                new SwitchableClusterMetadataRepository(
                        new MetadataStore(tempDir.resolve("empty")));
        ClusterController promotedController = new ClusterController(
                repository, 5_000L, now::get);
        repository.switchTo(shared);
        promotedController.installMetadata(shared.read());
        promotedController.beginRecovery(2_000L);

        assertEquals(ErrorCode.NONE,
                promotedController.heartbeatBroker(new BrokerHeartbeatRequest(0, 1L)));
        now.set(TimeUnit.MILLISECONDS.toNanos(2_001L));
        assertTrue(promotedController.advanceRecoveryWindow());
        assertTrue(promotedController.getBroker(0).online);
    }

    private ClusterController controllerWithReplicatedTopic() {
        ClusterController controller = new ClusterController(new MetadataStore(tempDir));
        controller.registerBroker(new RegisterBrokerRequest(0, "localhost", 9092));
        controller.registerBroker(new RegisterBrokerRequest(1, "localhost", 9093));
        controller.createTopic(new CreateTopicRequest("orders", 1, 2));
        return controller;
    }

    private ClusterMetadata.WatermarkSnapshot watermark(ClusterController controller) {
        return controller.snapshot().topics.get("orders").partitions.get(0).lastCompleteWatermark;
    }

    private void assertInvalidatedWatermark(
            ClusterController controller,
            long expectedPhysical,
            long expectedVisible,
            long expectedNextVisibleAt) {
        ClusterMetadata.WatermarkSnapshot watermark = watermark(controller);
        assertEquals(expectedPhysical, watermark.physicalHighWatermark);
        assertEquals(expectedVisible, watermark.visibleHighWatermark);
        assertEquals(expectedNextVisibleAt, watermark.nextVisibleAtEpochMs);
        assertEquals(-1L, watermark.controllerEpoch);
        assertEquals(-1L, watermark.leaderEpoch);
    }

    private static final class RejectingMetadataRepository implements ClusterMetadataRepository {
        private final VersionedClusterMetadata current =
                new VersionedClusterMetadata(new ClusterMetadata(), 0L);

        @Override
        public VersionedClusterMetadata read() {
            return current;
        }

        @Override
        public VersionedClusterMetadata read(java.time.Duration timeout) {
            return current;
        }

        @Override
        public UpdateResult initialize(ClusterMetadata metadata) {
            return new UpdateResult(ErrorCode.CONTROLLER_FENCED, current);
        }

        @Override
        public UpdateResult initialize(ClusterMetadata metadata, java.time.Duration timeout) {
            return new UpdateResult(ErrorCode.CONTROLLER_FENCED, current);
        }

        @Override
        public UpdateResult compareAndSet(long expectedRevision, ClusterMetadata metadata) {
            return new UpdateResult(ErrorCode.CONTROLLER_FENCED, current);
        }

        @Override
        public UpdateResult compareAndSet(
                long expectedRevision, ClusterMetadata metadata, java.time.Duration timeout) {
            return new UpdateResult(ErrorCode.CONTROLLER_FENCED, current);
        }
    }

    private static final class CountingMetadataStore extends MetadataStore {
        private int saveCount;

        private CountingMetadataStore(Path dataDir) {
            super(dataDir);
        }

        @Override
        public synchronized void save(ClusterMetadata metadata) {
            saveCount++;
            super.save(metadata);
        }
    }
}
