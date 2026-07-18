package com.minimq.broker.controller;

import com.minimq.broker.metadata.MetadataStore;
import com.minimq.broker.metadata.ClusterMetadata;
import com.minimq.common.error.ErrorCode;
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ClusterControllerTest {

    @TempDir
    Path tempDir;

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
        assertEquals(ErrorCode.INVALID_REQUEST,
                controller.updateReplicaLogEndOffset("orders", 0, 9, 1L));
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
    void markBrokerOfflineElectsFirstSurvivingReplicaByConfiguredOrder() {
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
        assertEquals(1, partition.leaderBrokerId());
        assertEquals(List.of(1, 0), partition.isrBrokerIds());
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
    void markBrokerOfflineRejectsLastIsrLeaderWithoutMutatingState() {
        ClusterController controller = new ClusterController(new MetadataStore(tempDir));
        controller.registerBroker(new RegisterBrokerRequest(0, "localhost", 9092));
        controller.createTopic(new com.minimq.protocol.message.ProtocolMessages.CreateTopicRequest(
                "orders", 1, 1));

        assertEquals(ErrorCode.INVALID_REQUEST, controller.markBrokerOffline(0));
        assertTrue(controller.getBroker(0).online);
        assertEquals(0, controller.getLeaderBrokerId("orders", 0));
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

    private ClusterController controllerWithReplicatedTopic() {
        ClusterController controller = new ClusterController(new MetadataStore(tempDir));
        controller.registerBroker(new RegisterBrokerRequest(0, "localhost", 9092));
        controller.registerBroker(new RegisterBrokerRequest(1, "localhost", 9093));
        controller.createTopic(new CreateTopicRequest("orders", 1, 2));
        return controller;
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
