package com.minimq.broker.status;

import com.minimq.broker.BrokerAvailabilityGuard;
import com.minimq.broker.BrokerConfig;
import com.minimq.broker.controller.ClusterController;
import com.minimq.broker.metadata.MetadataStore;
import com.minimq.broker.support.BrokerTestHarness;
import com.minimq.common.error.ErrorCode;
import com.minimq.protocol.message.ProtocolMessages.CreateTopicRequest;
import com.minimq.protocol.message.ProtocolMessages.OpsStatusResponse;
import com.minimq.protocol.message.ProtocolMessages.RegisterBrokerRequest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BrokerStatusServiceTest {

    @TempDir
    Path tempDir;

    @Test
    void statusSortsAndIndependentlyBoundsBrokersAndPartitionsFromOneSnapshot() {
        BrokerConfig config = config();
        ClusterController controller = new ClusterController(new MetadataStore(tempDir));
        controller.registerBroker(new RegisterBrokerRequest(2, "broker-2", 9094));
        controller.registerBroker(new RegisterBrokerRequest(0, "broker-0", 9092));
        controller.registerBroker(new RegisterBrokerRequest(1, "broker-1", 9093));
        assertEquals(ErrorCode.NONE, controller.createTopic(new CreateTopicRequest("payments", 1, 1)));
        assertEquals(ErrorCode.NONE, controller.createTopic(new CreateTopicRequest("orders", 2, 1)));

        BrokerAvailabilityGuard guard = new BrokerAvailabilityGuard();
        guard.fence();
        BrokerStatusService service = new BrokerStatusService(config, controller, guard, () -> 123_456L);

        OpsStatusResponse response = service.status(2, 2);

        assertEquals(0, response.targetBrokerId());
        assertEquals(123_456L, response.snapshotAtEpochMs());
        assertEquals(controller.snapshot().metadataVersion, response.metadataVersion());
        assertEquals(4, response.metadataFormatVersion());
        assertEquals("CONTROLLER", response.role());
        assertEquals("FENCED", response.maintenanceState());
        assertEquals(3, response.brokers().total());
        assertTrue(response.brokers().truncated());
        assertEquals(List.of(0, 1), response.brokers().items().stream().map(b -> b.brokerId()).toList());
        assertEquals(3, response.partitions().total());
        assertTrue(response.partitions().truncated());
        assertEquals(List.of("orders:0", "orders:1"),
                response.partitions().items().stream()
                        .map(p -> p.topicName() + ":" + p.partitionId())
                        .toList());
        assertFalse(response.partitions().items().get(0).replicaIds().truncated());
    }

    @Test
    void statusReportsFrozenAndUnknownWatermarkReadinessWithoutUsingNegativeLagMath() {
        BrokerConfig config = config();
        ClusterController controller = new ClusterController(new MetadataStore(tempDir));
        controller.registerBroker(new RegisterBrokerRequest(0, "broker-0", 9092));
        assertEquals(ErrorCode.NONE, controller.createTopic(new CreateTopicRequest("orders", 1, 1)));
        controller.updateMetadata(metadata -> {
            var partition = metadata.topics.get("orders").partitions.get(0);
            partition.frozen = true;
            partition.lastCompleteWatermark.physicalHighWatermark = -1L;
            partition.lastCompleteWatermark.visibleHighWatermark = -1L;
            return null;
        });

        BrokerAvailabilityGuard guard = new BrokerAvailabilityGuard();
        guard.activate();
        BrokerStatusService service = new BrokerStatusService(config, controller, guard, () -> 123_456L);

        OpsStatusResponse response = service.status(10, 10);

        assertNotNull(response.partitions().items().get(0));
        assertEquals("FROZEN", response.partitions().items().get(0).readiness());
        assertEquals(-1L, response.partitions().items().get(0).physicalHighWatermark());
        assertEquals(-1L, response.partitions().items().get(0).visibleHighWatermark());
        assertTrue(response.partitions().items().get(0).frozen());
    }

    private BrokerConfig config() {
        return new BrokerConfig(
                0, "127.0.0.1", 9092, tempDir, true, "127.0.0.1", 9092, false,
                BrokerTestHarness.ADMIN_TOKEN);
    }
}
