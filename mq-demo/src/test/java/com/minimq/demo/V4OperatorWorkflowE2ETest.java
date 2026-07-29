package com.minimq.demo;

import com.minimq.broker.support.BrokerTestHarness;
import com.minimq.client.ControllerAwareNetworkClient;
import com.minimq.client.NetworkClient;
import com.minimq.protocol.ApiKeys;
import com.minimq.protocol.message.ProtocolMessages.*;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class V4OperatorWorkflowE2ETest {

    @Test
    void opsStatusPrintsCompleteRuntimeAndMetadataIdentity() throws Exception {
        Path dataDir = BrokerTestHarness.newDataDir("v4-operator-status");
        try (BrokerTestHarness broker = BrokerTestHarness.startController(dataDir)) {
            String output = captureStdout(() -> DemoCli.main(new String[] {
                    "ops-status",
                    "--broker", broker.bootstrapServers(),
                    "--admin-token", BrokerTestHarness.ADMIN_TOKEN
            }));

            for (String field : new String[] {
                    "startedAtEpochMs=",
                    "snapshotAtEpochMs=",
                    "metadataFormatVersion=",
                    "transportMode="
            }) {
                assertTrue(output.contains(field), "ops-status missing " + field + ": " + output);
            }
        }
    }

    @Test
    void topicDeleteOutlivesTheDefaultServerWaitAndReturnsDeletingState() throws Exception {
        Path dataDir = BrokerTestHarness.newDataDir("v4-operator-delete-timeout");
        try (BrokerTestHarness broker = BrokerTestHarness.startController(dataDir)) {
            captureStdout(() -> DemoCli.main(new String[] {
                    "create-topic",
                    "--brokers", broker.bootstrapServers(),
                    "--admin-token", BrokerTestHarness.ADMIN_TOKEN,
                    "--name", "delete-after-wait",
                    "--partitions", "1"
            }));

            try (NetworkClient phantomBroker = new NetworkClient("v4-operator-phantom-broker")) {
                phantomBroker.connect("127.0.0.1", broker.port());
                var registration = phantomBroker.send(
                        ApiKeys.REGISTER_BROKER,
                        new RegisterBrokerRequest(99, "127.0.0.1", 1));
                phantomBroker.ensureSuccess(registration, "register phantom broker");

                String output = captureStdout(() -> DemoCli.main(new String[] {
                        "delete-topic",
                        "--brokers", broker.bootstrapServers(),
                        "--admin-token", BrokerTestHarness.ADMIN_TOKEN,
                        "--topic", "delete-after-wait"
                }));

                assertTrue(output.contains("state=DELETING"), output);
                assertTrue(output.contains("items=[99]"), output);
            }
        }
    }

    @Test
    void topicManagementWorkflowRunsOverTheRealControllerProtocol() throws Exception {
        Path dataDir = BrokerTestHarness.newDataDir("v4-operator-topic-workflow");
        try (BrokerTestHarness broker = BrokerTestHarness.startController(
                     dataDir, "--topic-delete-timeout-ms", "500");
             ControllerAwareNetworkClient operator = new ControllerAwareNetworkClient(
                     broker.bootstrapServers(), "v4-operator-e2e")) {
            operator.connect();

            for (String topic : List.of("workflow", "workflow.retry", "workflow.dlq")) {
                var created = operator.send(
                        ApiKeys.CREATE_TOPIC_ADMIN,
                        new CreateTopicAdminRequest(
                                BrokerTestHarness.ADMIN_TOKEN, topic, 1, 1));
                operator.ensureSuccess(created, "create " + topic);
            }

            var configured = operator.send(
                    ApiKeys.CONFIGURE_TOPIC_RELIABILITY,
                    new ConfigureTopicReliabilityRequest(
                            BrokerTestHarness.ADMIN_TOKEN,
                            "workflow",
                            true,
                            "workflow.retry",
                            false,
                            true,
                            "workflow.dlq",
                            false));
            operator.ensureSuccess(configured, "configure reliability");
            ConfigureTopicReliabilityResponse reliability =
                    (ConfigureTopicReliabilityResponse) configured.body();
            assertEquals("workflow.retry", reliability.retryTopicName());
            assertEquals("workflow.dlq", reliability.deadLetterTopicName());

            var increased = operator.send(
                    ApiKeys.INCREASE_PARTITIONS,
                    new IncreasePartitionsRequest(BrokerTestHarness.ADMIN_TOKEN, "workflow", 2));
            operator.ensureSuccess(increased, "increase partitions");
            IncreasePartitionsResponse partitionChange =
                    (IncreasePartitionsResponse) increased.body();
            assertEquals(1, partitionChange.previousPartitionCount());
            assertEquals(2, partitionChange.newPartitionCount());

            var listed = operator.send(
                    ApiKeys.LIST_TOPICS,
                    new ListTopicsRequest(BrokerTestHarness.ADMIN_TOKEN, 10));
            operator.ensureSuccess(listed, "list topics");
            assertTrue(((ListTopicsResponse) listed.body()).topicNames().items()
                    .containsAll(List.of("workflow", "workflow.retry", "workflow.dlq")));

            var described = operator.send(
                    ApiKeys.DESCRIBE_TOPIC,
                    new DescribeTopicRequest(
                            BrokerTestHarness.ADMIN_TOKEN, "workflow", 10, false));
            operator.ensureSuccess(described, "describe workflow");
            DescribeTopicResponse active = (DescribeTopicResponse) described.body();
            assertTrue(active.active());
            assertEquals(2, active.partitions().total());

            var deleted = operator.send(
                    ApiKeys.DELETE_TOPIC,
                    new DeleteTopicRequest(BrokerTestHarness.ADMIN_TOKEN, "workflow.dlq"));
            operator.ensureSuccess(deleted, "delete workflow.dlq");
            DeleteTopicResponse tombstone = (DeleteTopicResponse) deleted.body();
            assertEquals("DELETED", tombstone.tombstoneState());

            var describedDeleted = operator.send(
                    ApiKeys.DESCRIBE_TOPIC,
                    new DescribeTopicRequest(
                            BrokerTestHarness.ADMIN_TOKEN, "workflow.dlq", 10, true));
            operator.ensureSuccess(describedDeleted, "describe deleted workflow.dlq");
            DescribeTopicResponse deletedTopic = (DescribeTopicResponse) describedDeleted.body();
            assertFalse(deletedTopic.active());
            assertEquals("DELETED", deletedTopic.tombstone().state());
        }
    }

    private static String captureStdout(ThrowingRunnable action) throws Exception {
        PrintStream original = System.out;
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        try {
            System.setOut(new PrintStream(output, true, StandardCharsets.UTF_8));
            action.run();
            return output.toString(StandardCharsets.UTF_8);
        } finally {
            System.setOut(original);
        }
    }

    @FunctionalInterface
    private interface ThrowingRunnable {
        void run() throws Exception;
    }
}
