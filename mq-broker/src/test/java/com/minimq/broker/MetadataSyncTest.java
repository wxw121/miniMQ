package com.minimq.broker;

import com.minimq.broker.controller.ClusterController;
import com.minimq.broker.metadata.MetadataStore;
import com.minimq.protocol.message.ProtocolMessages.BrokerMetadata;
import com.minimq.protocol.message.ProtocolMessages.MetadataResponse;
import com.minimq.protocol.message.ProtocolMessages.PartitionMetadata;
import com.minimq.protocol.message.ProtocolMessages.TopicMetadata;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MetadataSyncTest {

    @TempDir
    Path tempDir;

    @Test
    void applyMetadataCopiesReplicaLogEndOffsetsAndFrozenState() {
        ClusterController controller = new ClusterController(new MetadataStore(tempDir));
        MetadataResponse response = new MetadataResponse(
                List.of(
                        new BrokerMetadata(0, "localhost", 9092),
                        new BrokerMetadata(1, "localhost", 9093)),
                List.of(new TopicMetadata(
                        "orders",
                        List.of(new PartitionMetadata(
                                0,
                                0,
                                List.of(0, 1),
                                List.of(),
                                Map.of(0, 12L, 1, 9L),
                                true)))));

        MetadataSync.applyMetadata(controller, response);

        assertEquals(
                Map.of(0, 12L, 1, 9L),
                controller.snapshot().topics.get("orders").partitions.get(0).replicaLogEndOffsets);
        assertTrue(controller.snapshot().topics.get("orders").partitions.get(0).frozen);
        assertTrue(controller.snapshot().topics.get("orders").partitions.get(0).isrBrokerIds.isEmpty());
    }
}
