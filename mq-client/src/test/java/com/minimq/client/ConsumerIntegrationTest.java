package com.minimq.client;

import com.minimq.broker.support.BrokerTestHarness;
import com.minimq.client.model.ConsumerRecord;
import com.minimq.client.model.ProducerRecord;
import com.minimq.protocol.ApiKeys;
import com.minimq.protocol.codec.MqDecoder;
import com.minimq.protocol.message.ProtocolMessages.CreateTopicRequest;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConsumerIntegrationTest {

    @Test
    void consumeFiveMessagesAndCommit() throws Exception {
        Path dataDir = BrokerTestHarness.newDataDir("consumer");
        try (BrokerTestHarness broker = BrokerTestHarness.startController(dataDir);
             NetworkClient admin = new NetworkClient("admin");
             Producer producer = new Producer(broker.bootstrapServers(), "producer-it");
             Consumer consumer = new Consumer(broker.bootstrapServers(), "it-group", "consumer-it")) {
            admin.connect("localhost", broker.port());
            admin.ensureSuccess(admin.send(
                    ApiKeys.CREATE_TOPIC, new CreateTopicRequest("orders", 1)));

            producer.start();
            for (int i = 0; i < 5; i++) {
                producer.send(new ProducerRecord(
                        "orders",
                        null,
                        ("k" + i).getBytes(StandardCharsets.UTF_8),
                        ("v" + i).getBytes(StandardCharsets.UTF_8)));
            }

            consumer.subscribe("orders");
            List<ConsumerRecord> records = consumer.poll(Duration.ofSeconds(2));
            assertEquals(5, records.size());

            for (ConsumerRecord record : records) {
                consumer.commitSync(record.topic(), record.partition(), record.offset() + 1);
            }

            List<ConsumerRecord> again = consumer.poll(Duration.ofMillis(300));
            assertTrue(again.isEmpty());
        }
    }
}
