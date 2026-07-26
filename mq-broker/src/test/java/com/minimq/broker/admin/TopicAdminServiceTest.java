package com.minimq.broker.admin;

import com.minimq.broker.controller.ClusterController;
import com.minimq.broker.metadata.ClusterMetadata;
import com.minimq.broker.metadata.MetadataStore;
import com.minimq.common.error.ErrorCode;
import com.minimq.protocol.message.ProtocolMessages.CreateTopicAdminRequest;
import com.minimq.protocol.message.ProtocolMessages.ConfigureTopicReliabilityRequest;
import com.minimq.protocol.message.ProtocolMessages.DeleteTopicRequest;
import com.minimq.protocol.message.ProtocolMessages.DescribeTopicRequest;
import com.minimq.protocol.message.ProtocolMessages.IncreasePartitionsRequest;
import com.minimq.protocol.message.ProtocolMessages.ListTopicsRequest;
import com.minimq.protocol.message.ProtocolMessages.RegisterBrokerRequest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TopicAdminServiceTest {
    @TempDir
    Path tempDir;

    @Test
    void createsListsDescribesAndIncreasesPartitionsWithoutRewritingExisting() {
        ClusterController controller = controllerWithBrokers(3);
        TopicAdminService service = new TopicAdminService(controller);

        assertEquals(ErrorCode.NONE, service.create(new CreateTopicAdminRequest(
                "token", "orders", 2, 2)).errorCode());
        ClusterMetadata.PartitionEntry firstBefore =
                controller.snapshot().topics.get("orders").partitions.get(0);
        List<Integer> replicasBefore = List.copyOf(firstBefore.replicaBrokerIds);

        assertEquals(ErrorCode.NONE,
                service.increase(new IncreasePartitionsRequest("token", "orders", 5)).errorCode());
        assertEquals(List.of("orders"),
                service.list(new ListTopicsRequest("token", 10)).response().topicNames().items());
        var description = service.describe(new DescribeTopicRequest("token", "orders", 10, false));
        assertEquals(ErrorCode.NONE, description.errorCode());
        assertEquals(5, description.response().partitions().total());
        assertEquals(replicasBefore,
                controller.snapshot().topics.get("orders").partitions.get(0).replicaBrokerIds);
        assertEquals(4, description.response().partitions().items().get(4).partitionId());
    }

    @Test
    void deletionInstallsTombstoneAndAckCompletesItIdempotently() {
        ClusterController controller = controllerWithBrokers(1);
        TopicAdminService service = new TopicAdminService(controller);
        assertEquals(ErrorCode.NONE, service.create(new CreateTopicAdminRequest(
                "token", "orders", 1, 1)).errorCode());

        var deleting = service.delete(new DeleteTopicRequest("token", "orders"));
        assertEquals(ErrorCode.NONE, deleting.errorCode());
        assertEquals("DELETING", deleting.response().tombstoneState());
        assertEquals(ErrorCode.UNKNOWN_TOPIC,
                service.describe(new DescribeTopicRequest("token", "orders", 10, false)).errorCode());
        assertEquals("DELETING", service.describe(
                new DescribeTopicRequest("token", "orders", 10, true)).response().tombstone().state());

        ClusterMetadata.BrokerEntry broker = controller.getBroker(0);
        ClusterMetadata.TopicTombstone tombstone = controller.getTopicTombstone("orders");
        assertEquals(ErrorCode.NONE, controller.acknowledgeTopicDeletion(
                "orders", tombstone.tombstoneVersion, 0, broker.brokerEpoch,
                controller.snapshot().controllerEpoch));
        assertEquals("DELETED", service.describe(
                new DescribeTopicRequest("token", "orders", 10, true)).response().tombstone().state());
        assertTrue(controller.snapshot().topics.isEmpty());
    }

    @Test
    void configuresReliabilityLinksAndClearsThemAtomically() {
        ClusterController controller = controllerWithBrokers(1);
        TopicAdminService service = new TopicAdminService(controller);
        assertEquals(ErrorCode.NONE, service.create(new CreateTopicAdminRequest(
                "token", "orders", 1, 1)).errorCode());
        assertEquals(ErrorCode.NONE, service.create(new CreateTopicAdminRequest(
                "token", "orders.retry", 1, 1)).errorCode());
        assertEquals(ErrorCode.NONE, service.create(new CreateTopicAdminRequest(
                "token", "orders.dlq", 1, 1)).errorCode());

        var configured = service.configureReliability(new ConfigureTopicReliabilityRequest(
                "token", "orders", true, "orders.retry", false, true, "orders.dlq", false));

        assertEquals(ErrorCode.NONE, configured.errorCode());
        assertEquals("orders.retry", configured.response().retryTopicName());
        assertEquals("orders.dlq", configured.response().deadLetterTopicName());
        var described = service.describe(new DescribeTopicRequest("token", "orders", 10, false));
        assertEquals("orders.retry", described.response().retryTopic());
        assertEquals("orders.dlq", described.response().deadLetterTopic());

        var cleared = service.configureReliability(new ConfigureTopicReliabilityRequest(
                "token", "orders", false, null, true, false, null, true));

        assertEquals(ErrorCode.NONE, cleared.errorCode());
        assertEquals(null, cleared.response().retryTopicName());
        assertEquals(null, cleared.response().deadLetterTopicName());
        assertEquals(null, controller.snapshot().retryTopicBySource.get("orders"));
        assertEquals(null, controller.snapshot().deadLetterTopicBySource.get("orders"));
    }

    @Test
    void rejectsReliabilityLinksToInactiveSelfOrCyclicTargets() {
        ClusterController controller = controllerWithBrokers(1);
        TopicAdminService service = new TopicAdminService(controller);
        assertEquals(ErrorCode.NONE, service.create(new CreateTopicAdminRequest(
                "token", "orders", 1, 1)).errorCode());
        assertEquals(ErrorCode.NONE, service.create(new CreateTopicAdminRequest(
                "token", "orders.retry", 1, 1)).errorCode());

        assertEquals(ErrorCode.UNKNOWN_TOPIC, service.configureReliability(
                new ConfigureTopicReliabilityRequest(
                        "token", "orders", true, "missing", false, false, null, false)).errorCode());
        assertEquals(ErrorCode.INVALID_REQUEST, service.configureReliability(
                new ConfigureTopicReliabilityRequest(
                        "token", "orders", true, "orders", false, false, null, false)).errorCode());
        assertEquals(ErrorCode.NONE, service.configureReliability(
                new ConfigureTopicReliabilityRequest(
                        "token", "orders", true, "orders.retry", false, false, null, false)).errorCode());
        assertEquals(ErrorCode.INVALID_REQUEST, service.configureReliability(
                new ConfigureTopicReliabilityRequest(
                        "token", "orders.retry", true, "orders", false, false, null, false)).errorCode());
    }

    private ClusterController controllerWithBrokers(int count) {
        ClusterController controller = new ClusterController(new MetadataStore(tempDir));
        for (int brokerId = 0; brokerId < count; brokerId++) {
            assertEquals(ErrorCode.NONE, controller.registerBroker(
                    new RegisterBrokerRequest(brokerId, "127.0.0.1", 9000 + brokerId)).errorCode());
        }
        return controller;
    }
}
