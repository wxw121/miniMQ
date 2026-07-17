package com.minimq.broker;

import com.minimq.broker.support.BrokerTestHarness;
import com.minimq.broker.support.TestNetworkClient;
import com.minimq.common.error.ErrorCode;
import com.minimq.protocol.ApiKeys;
import com.minimq.protocol.codec.MqDecoder;
import com.minimq.protocol.message.ProtocolMessages.CreateTopicRequest;
import com.minimq.protocol.message.ProtocolMessages.FetchRequest;
import com.minimq.protocol.message.ProtocolMessages.FetchResponse;
import com.minimq.protocol.message.ProtocolMessages.MarkBrokerOfflineRequest;
import com.minimq.protocol.message.ProtocolMessages.ProduceRecord;
import com.minimq.protocol.message.ProtocolMessages.ProduceRequest;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ReplicationIntegrationTest {

    @Test
    void clientFetchWaitsForReplicationHighWatermark() throws Exception {
        Path dir0 = BrokerTestHarness.newDataDir("fetch-before-replication");
        Path dir1 = BrokerTestHarness.newDataDir("fetch-follower");
        Path dir2 = BrokerTestHarness.newDataDir("fetch-follower-2");
        try (BrokerTestHarness controller = BrokerTestHarness.startController(dir0);
             BrokerTestHarness broker1 = BrokerTestHarness.startFollower(1, dir1, "127.0.0.1", controller.port());
             BrokerTestHarness broker2 = BrokerTestHarness.startFollower(2, dir2, "127.0.0.1", controller.port());
             TestNetworkClient admin = new TestNetworkClient();
             TestNetworkClient leaderClient = new TestNetworkClient()) {
            admin.connect("127.0.0.1", controller.port());
            registerBrokers(admin, controller, broker1, broker2);
            createTopic(admin, "orders", 1, 3);
            leaderClient.connect("127.0.0.1", controller.port());
            produce(leaderClient, "orders", 0, "unreplicated");

            MqDecoder.DecodedResponse fetchResponse = leaderClient.send(
                    ApiKeys.FETCH, new FetchRequest("orders", 0, 0, 1024 * 1024));
            assertEquals(ErrorCode.NONE, fetchResponse.header().errorCode());
            FetchResponse fetch = (FetchResponse) fetchResponse.body();
            assertEquals(0L, fetch.highWatermark());
            assertEquals(0, fetch.records().size());
        }
    }

    @Test
    void followerReplicatesLeaderLog() throws Exception {
        Path dir0 = BrokerTestHarness.newDataDir("replication-leader");
        Path dir1 = BrokerTestHarness.newDataDir("replication-follower");
        Path dir2 = BrokerTestHarness.newDataDir("replication-follower-2");
        try (BrokerTestHarness controller = BrokerTestHarness.startController(dir0);
             BrokerTestHarness broker1 = BrokerTestHarness.startFollower(1, dir1, "127.0.0.1", controller.port());
             BrokerTestHarness broker2 = BrokerTestHarness.startFollower(2, dir2, "127.0.0.1", controller.port());
             TestNetworkClient admin = new TestNetworkClient()) {
            admin.connect("127.0.0.1", controller.port());
            registerBrokers(admin, controller, broker1, broker2);
            createTopic(admin, "orders", 1, 3);

            TestNetworkClient leaderClient = new TestNetworkClient();
            leaderClient.connect("127.0.0.1", controller.port());
            produce(leaderClient, "orders", 0, "hello");

            waitForFollowerReplication(broker1, "orders", 0, 1);
            waitForFollowerReplication(broker2, "orders", 0, 1);
        }
    }

    @Test
    void leaderFailoverAllowsProduceOnPromotedFollower() throws Exception {
        Path dir0 = BrokerTestHarness.newDataDir("failover-leader");
        Path dir1 = BrokerTestHarness.newDataDir("failover-follower");
        Path dir2 = BrokerTestHarness.newDataDir("failover-follower-2");
        try (BrokerTestHarness controller = BrokerTestHarness.startController(dir0);
             BrokerTestHarness broker1 = BrokerTestHarness.startFollower(1, dir1, "127.0.0.1", controller.port());
             BrokerTestHarness broker2 = BrokerTestHarness.startFollower(2, dir2, "127.0.0.1", controller.port());
             TestNetworkClient admin = new TestNetworkClient()) {
            admin.connect("127.0.0.1", controller.port());
            registerBrokers(admin, controller, broker1, broker2);
            createTopic(admin, "orders", 1, 3);

            TestNetworkClient leaderClient = new TestNetworkClient();
            leaderClient.connect("127.0.0.1", controller.port());
            produce(leaderClient, "orders", 0, "before-failover");
            waitForFollowerReplication(broker1, "orders", 0, 1);
            waitForFollowerReplication(broker2, "orders", 0, 1);

            MqDecoder.DecodedResponse offline = admin.send(
                    ApiKeys.MARK_BROKER_OFFLINE, new MarkBrokerOfflineRequest(0));
            assertEquals(ErrorCode.NONE, offline.header().errorCode());

            TestNetworkClient newLeaderClient = new TestNetworkClient();
            newLeaderClient.connect("127.0.0.1", broker1.port());
            produce(newLeaderClient, "orders", 0, "after-failover");
            waitForFollowerReplication(broker2, "orders", 0, 2);

            MqDecoder.DecodedResponse fetchResponse = newLeaderClient.send(
                    ApiKeys.FETCH, new FetchRequest("orders", 0, 0, 1024 * 1024));
            assertEquals(ErrorCode.NONE, fetchResponse.header().errorCode());
            FetchResponse fetch = (FetchResponse) fetchResponse.body();
            assertTrue(fetch.records().size() >= 2);
            assertTrue(fetch.highWatermark() >= 2);
        }
    }

    @Test
    void produceReturnsNotLeaderOnFollowerBroker() throws Exception {
        Path dir0 = BrokerTestHarness.newDataDir("not-leader-controller");
        Path dir1 = BrokerTestHarness.newDataDir("not-leader-follower");
        try (BrokerTestHarness controller = BrokerTestHarness.startController(dir0);
             BrokerTestHarness broker1 = BrokerTestHarness.startFollower(1, dir1, "127.0.0.1", controller.port());
             TestNetworkClient admin = new TestNetworkClient();
             TestNetworkClient followerClient = new TestNetworkClient()) {
            admin.connect("127.0.0.1", controller.port());
            registerBrokers(admin, controller, broker1);
            createTopic(admin, "orders", 1, 2);

            followerClient.connect("127.0.0.1", broker1.port());
            MqDecoder.DecodedResponse response = followerClient.send(
                    ApiKeys.PRODUCE,
                    new ProduceRequest(
                            "orders",
                            0,
                            List.of(new ProduceRecord(null, "x".getBytes(StandardCharsets.UTF_8)))));
            assertEquals(ErrorCode.NOT_LEADER, response.header().errorCode());
        }
    }

    private static void registerBrokers(
            TestNetworkClient admin,
            BrokerTestHarness controller,
            BrokerTestHarness... followers) throws InterruptedException {
        assertEquals(
                ErrorCode.NONE,
                admin.send(ApiKeys.REGISTER_BROKER,
                        new com.minimq.protocol.message.ProtocolMessages.RegisterBrokerRequest(
                                controller.brokerId(), "127.0.0.1", controller.port()))
                        .header().errorCode());
        for (BrokerTestHarness follower : followers) {
            assertEquals(
                    ErrorCode.NONE,
                    admin.send(ApiKeys.REGISTER_BROKER,
                            new com.minimq.protocol.message.ProtocolMessages.RegisterBrokerRequest(
                                    follower.brokerId(), "127.0.0.1", follower.port()))
                            .header().errorCode());
        }
    }

    private static void createTopic(
            TestNetworkClient client, String topic, int partitions, int replicationFactor)
            throws InterruptedException {
        MqDecoder.DecodedResponse response = client.send(
                ApiKeys.CREATE_TOPIC, new CreateTopicRequest(topic, partitions, replicationFactor));
        assertEquals(ErrorCode.NONE, response.header().errorCode());
    }

    private static void produce(TestNetworkClient client, String topic, int partition, String value)
            throws InterruptedException {
        MqDecoder.DecodedResponse response = client.send(
                ApiKeys.PRODUCE,
                new ProduceRequest(
                        topic,
                        partition,
                        List.of(new ProduceRecord(
                                "key".getBytes(StandardCharsets.UTF_8),
                                value.getBytes(StandardCharsets.UTF_8)))));
        assertEquals(ErrorCode.NONE, response.header().errorCode());
    }

    private static void waitForFollowerReplication(
            BrokerTestHarness follower, String topic, int partition, long expectedRecords)
            throws InterruptedException {
        for (int attempt = 0; attempt < 100; attempt++) {
            if (follower.localHighWatermark(topic, partition) >= expectedRecords) {
                return;
            }
            Thread.sleep(50);
        }
        assertEquals(
                expectedRecords,
                follower.localHighWatermark(topic, partition),
                "follower did not replicate expected records");
    }
}
