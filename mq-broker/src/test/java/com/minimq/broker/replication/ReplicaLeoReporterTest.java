package com.minimq.broker.replication;

import com.minimq.broker.BrokerConfig;
import com.minimq.broker.controller.ClusterController;
import com.minimq.broker.metadata.MetadataStore;
import com.minimq.broker.support.BrokerTestHarness;
import com.minimq.broker.support.TestNetworkClient;
import com.minimq.common.error.ErrorCode;
import com.minimq.protocol.ApiKeys;
import com.minimq.protocol.message.ProtocolMessages.CreateTopicRequest;
import com.minimq.protocol.message.ProtocolMessages.MetadataRequest;
import com.minimq.protocol.message.ProtocolMessages.MetadataResponse;
import com.minimq.protocol.message.ProtocolMessages.RegisterBrokerRequest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.net.ServerSocket;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ReplicaLeoReporterTest {

    @TempDir
    Path tempDir;

    @Test
    void controllerReporterUpdatesLocalMetadata() throws Exception {
        ClusterController controller = new ClusterController(new MetadataStore(tempDir));
        controller.registerBroker(new RegisterBrokerRequest(0, "127.0.0.1", 9092));
        controller.createTopic(new CreateTopicRequest("orders", 1));
        BrokerConfig config = new BrokerConfig(
                0, "127.0.0.1", 9092, tempDir, true, "127.0.0.1", 9092, false,
                BrokerTestHarness.ADMIN_TOKEN);

        try (ReplicaLeoReporter reporter = new ReplicaLeoReporter(config, controller)) {
            assertEquals(ErrorCode.NONE, reporter.report("orders", 0, 0, 7L));
        }

        assertEquals(7L, controller.replicaLogEndOffset("orders", 0, 0));
    }

    @Test
    void followerReporterUpdatesRemoteControllerMetadata() throws Exception {
        Path controllerDir = BrokerTestHarness.newDataDir("leo-reporter-controller");
        try (BrokerTestHarness controller = BrokerTestHarness.startController(controllerDir);
             TestNetworkClient admin = new TestNetworkClient()) {
            admin.connect("127.0.0.1", controller.port());
            assertEquals(ErrorCode.NONE, admin.send(
                    ApiKeys.REGISTER_BROKER,
                    new RegisterBrokerRequest(1, "127.0.0.1", 9093)).header().errorCode());
            assertEquals(ErrorCode.NONE, admin.send(
                    ApiKeys.CREATE_TOPIC,
                    new CreateTopicRequest("orders", 1, 2)).header().errorCode());

            BrokerConfig followerConfig = new BrokerConfig(
                    1,
                    "127.0.0.1",
                    9093,
                    tempDir,
                    false,
                    "127.0.0.1",
                    controller.port(),
                    false,
                    BrokerTestHarness.ADMIN_TOKEN);
            ClusterController followerMetadata =
                    new ClusterController(new MetadataStore(tempDir.resolve("follower")));
            try (ReplicaLeoReporter reporter =
                         new ReplicaLeoReporter(followerConfig, followerMetadata)) {
                assertEquals(ErrorCode.NONE, reporter.report("orders", 0, 1, 9L));
            }

            MetadataResponse metadata = (MetadataResponse) admin.send(
                    ApiKeys.METADATA, new MetadataRequest("orders")).body();
            assertEquals(
                    9L,
                    metadata.topics().get(0).partitions().get(0)
                            .replicaLogEndOffsets().get(1));
        }
    }

    @Test
    void followerReporterReconnectsAfterControllerBecomesAvailable() throws Exception {
        int controllerPort;
        try (ServerSocket socket = new ServerSocket(0)) {
            controllerPort = socket.getLocalPort();
        }
        BrokerConfig followerConfig = new BrokerConfig(
                1,
                "127.0.0.1",
                9093,
                tempDir.resolve("recovering-follower"),
                false,
                "127.0.0.1",
                controllerPort,
                false,
                BrokerTestHarness.ADMIN_TOKEN);
        ClusterController followerMetadata = new ClusterController(
                new MetadataStore(tempDir.resolve("recovering-follower")));

        try (ReplicaLeoReporter reporter =
                     new ReplicaLeoReporter(followerConfig, followerMetadata)) {
            assertEquals(
                    ErrorCode.INVALID_REQUEST,
                    reporter.report("orders", 0, 1, 9L));

            BrokerConfig controllerConfig = new BrokerConfig(
                    0,
                    "127.0.0.1",
                    controllerPort,
                    tempDir.resolve("recovering-controller"),
                    true,
                    "127.0.0.1",
                    controllerPort,
                    false,
                    BrokerTestHarness.ADMIN_TOKEN);
            try (BrokerTestHarness controller = BrokerTestHarness.start(controllerConfig);
                 TestNetworkClient admin = new TestNetworkClient()) {
                admin.connect("127.0.0.1", controller.port());
                assertEquals(ErrorCode.NONE, admin.send(
                        ApiKeys.REGISTER_BROKER,
                        new RegisterBrokerRequest(1, "127.0.0.1", 9093))
                        .header().errorCode());
                assertEquals(ErrorCode.NONE, admin.send(
                        ApiKeys.CREATE_TOPIC,
                        new CreateTopicRequest("orders", 1, 2))
                        .header().errorCode());

                assertEquals(
                        ErrorCode.NONE,
                        reporter.report("orders", 0, 1, 9L));
            }
        }
    }

    @Test
    void followerReporterSkipsAlreadyConfirmedLeoWhenControllerIsUnavailable() throws Exception {
        Path controllerDir = BrokerTestHarness.newDataDir("leo-reporter-deduplicate");
        BrokerTestHarness controller = BrokerTestHarness.startController(controllerDir);
        try (TestNetworkClient admin = new TestNetworkClient()) {
            admin.connect("127.0.0.1", controller.port());
            assertEquals(ErrorCode.NONE, admin.send(
                    ApiKeys.REGISTER_BROKER,
                    new RegisterBrokerRequest(1, "127.0.0.1", 9093)).header().errorCode());
            assertEquals(ErrorCode.NONE, admin.send(
                    ApiKeys.CREATE_TOPIC,
                    new CreateTopicRequest("orders", 1, 2)).header().errorCode());
            BrokerConfig followerConfig = new BrokerConfig(
                    1,
                    "127.0.0.1",
                    9093,
                    tempDir.resolve("deduplicate-follower"),
                    false,
                    "127.0.0.1",
                    controller.port(),
                    false,
                    BrokerTestHarness.ADMIN_TOKEN);
            ClusterController followerMetadata = new ClusterController(
                    new MetadataStore(tempDir.resolve("deduplicate-follower")));
            try (ReplicaLeoReporter reporter =
                         new ReplicaLeoReporter(followerConfig, followerMetadata)) {
                assertEquals(ErrorCode.NONE, reporter.report("orders", 0, 1, 9L));
                controller.close();
                controller = null;

                assertEquals(ErrorCode.NONE, reporter.report("orders", 0, 1, 9L));
            }
        } finally {
            if (controller != null) {
                controller.close();
            }
        }
    }
}
