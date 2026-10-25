package com.minimq.broker;

import com.minimq.broker.support.BrokerTestHarness;
import com.minimq.broker.support.TestNetworkClient;
import com.minimq.common.error.ErrorCode;
import com.minimq.protocol.ApiKeys;
import com.minimq.protocol.codec.MqDecoder;
import com.minimq.protocol.message.ProtocolMessages.CreateTopicRequest;
import com.minimq.protocol.message.ProtocolMessages.FetchRequest;
import com.minimq.protocol.message.ProtocolMessages.FetchResponse;
import com.minimq.protocol.message.ProtocolMessages.ProduceRecord;
import com.minimq.protocol.message.ProtocolMessages.ProduceRequest;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ProduceFetchIntegrationTest {

    @Test
    void produceAndFetchOnLeaderBroker() throws Exception {
        Path dataDir = BrokerTestHarness.newDataDir("produce-fetch");
        try (BrokerTestHarness broker = BrokerTestHarness.startController(dataDir);
             TestNetworkClient client = new TestNetworkClient()) {
            client.connect("127.0.0.1", broker.port());
            createTopic(client, "orders", 1);

            MqDecoder.DecodedResponse produceResponse = client.send(
                    ApiKeys.PRODUCE,
                    new ProduceRequest(
                            "orders",
                            0,
                            List.of(new ProduceRecord(
                                    "key".getBytes(StandardCharsets.UTF_8),
                                    "hello".getBytes(StandardCharsets.UTF_8)))));
            assertEquals(ErrorCode.NONE, produceResponse.header().errorCode());

            MqDecoder.DecodedResponse fetchResponse = client.send(
                    ApiKeys.FETCH,
                    new FetchRequest("orders", 0, 0, 1024 * 1024));
            assertEquals(ErrorCode.NONE, fetchResponse.header().errorCode());

            FetchResponse fetch = (FetchResponse) fetchResponse.body();
            assertEquals(1, fetch.records().size());
            assertEquals("hello", new String(fetch.records().get(0).value(), StandardCharsets.UTF_8));
        }
    }

    @Test
    void notLeaderWhenWrongBroker() throws Exception {
        Path dir0 = BrokerTestHarness.newDataDir("leader");
        Path dir1 = BrokerTestHarness.newDataDir("follower");
        try (BrokerTestHarness controller = BrokerTestHarness.startController(dir0);
             BrokerTestHarness follower = BrokerTestHarness.startFollower(
                     1, dir1, "127.0.0.1", controller.port());
             TestNetworkClient controllerClient = new TestNetworkClient();
             TestNetworkClient followerClient = new TestNetworkClient()) {
            controllerClient.connect("127.0.0.1", controller.port());
            createTopic(controllerClient, "orders", 2);

            followerClient.connect("127.0.0.1", follower.port());
            MqDecoder.DecodedResponse response = followerClient.send(
                    ApiKeys.PRODUCE,
                    new ProduceRequest(
                            "orders",
                            0,
                            List.of(new ProduceRecord(null, "x".getBytes(StandardCharsets.UTF_8)))));
            assertEquals(ErrorCode.NOT_LEADER, response.header().errorCode());
        }
    }

    private static void createTopic(TestNetworkClient client, String topic, int partitions) throws InterruptedException {
        MqDecoder.DecodedResponse response = client.send(
                ApiKeys.CREATE_TOPIC, new CreateTopicRequest(topic, partitions));
        assertEquals(ErrorCode.NONE, response.header().errorCode());
    }
}
