package com.minimq.client;

import com.minimq.broker.support.BrokerTestHarness;
import com.minimq.client.model.ProducerRecord;
import com.minimq.protocol.ApiKeys;
import com.minimq.protocol.codec.MqDecoder;
import com.minimq.protocol.message.ProtocolMessages.CreateTopicRequest;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ProducerIntegrationTest {

    @Test
    void sendThreeMessagesOffsetsIncrease() throws Exception {
        Path dataDir = BrokerTestHarness.newDataDir("producer");
        try (BrokerTestHarness broker = BrokerTestHarness.startController(dataDir);
             NetworkClient admin = new NetworkClient("admin");
             Producer producer = new Producer(broker.bootstrapServers(), "producer-it")) {
            admin.connect("localhost", broker.port());
            MqDecoder.DecodedResponse create = admin.send(
                    ApiKeys.CREATE_TOPIC, new CreateTopicRequest("orders", 1));
            admin.ensureSuccess(create);

            producer.start();

            assertEquals(0, producer.send(new ProducerRecord(
                    "orders", null, "k".getBytes(StandardCharsets.UTF_8), "m0".getBytes(StandardCharsets.UTF_8))).offset());
            assertEquals(1, producer.send(new ProducerRecord(
                    "orders", null, "k".getBytes(StandardCharsets.UTF_8), "m1".getBytes(StandardCharsets.UTF_8))).offset());
            assertEquals(2, producer.send(new ProducerRecord(
                    "orders", null, "k".getBytes(StandardCharsets.UTF_8), "m2".getBytes(StandardCharsets.UTF_8))).offset());
        }
    }
}
