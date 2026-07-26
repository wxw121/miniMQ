package com.minimq.demo;

import com.minimq.broker.support.BrokerTestHarness;
import com.minimq.client.Consumer;
import com.minimq.client.NetworkClient;
import com.minimq.client.Producer;
import com.minimq.client.model.ConsumerRecord;
import com.minimq.client.model.ProducerRecord;
import com.minimq.protocol.ApiKeys;
import com.minimq.protocol.codec.MqDecoder;
import com.minimq.protocol.message.ProtocolMessages.CreateTopicAdminRequest;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Task 11: 3 Broker 集群端到端验证（自动化）
 */
class MiniMqE2ETest {

    @Test
    void threeBrokerClusterProduceConsumeWithTwoConsumers() throws Exception {
        Path dir0 = BrokerTestHarness.newDataDir("e2e-b0");
        Path dir1 = BrokerTestHarness.newDataDir("e2e-b1");
        Path dir2 = BrokerTestHarness.newDataDir("e2e-b2");

        try (BrokerTestHarness broker0 = BrokerTestHarness.startController(dir0)) {
            try (BrokerTestHarness broker1 = BrokerTestHarness.startFollower(
                         1, dir1, "localhost", broker0.port());
                 BrokerTestHarness broker2 = BrokerTestHarness.startFollower(
                         2, dir2, "localhost", broker0.port());
                 NetworkClient admin = new NetworkClient("e2e-admin");
                 Producer producer = new Producer(broker0.bootstrapServers(), "e2e-producer");
                 Consumer consumer1 = new Consumer(broker0.bootstrapServers(), "e2e-group", "e2e-c1");
                 Consumer consumer2 = new Consumer(broker0.bootstrapServers(), "e2e-group", "e2e-c2")) {

                admin.connect("localhost", broker0.port());
                admin.ensureSuccess(admin.send(
                        ApiKeys.CREATE_TOPIC_ADMIN,
                        new CreateTopicAdminRequest(
                                BrokerTestHarness.ADMIN_TOKEN, "orders", 3, 1)));

                producer.start();
                consumer1.subscribe("orders");
                consumer2.subscribe("orders");

                Set<Integer> usedPartitions = new HashSet<>();
                for (int i = 0; i < 10; i++) {
                    var result = producer.send(new ProducerRecord(
                            "orders",
                            null,
                            ("user-" + i).getBytes(StandardCharsets.UTF_8),
                            ("msg-" + i).getBytes(StandardCharsets.UTF_8)));
                    usedPartitions.add(result.partitionId());
                }
                assertTrue(usedPartitions.size() >= 1, "messages should land in at least one partition");

                List<ConsumerRecord> all = new ArrayList<>();
                all.addAll(consumer1.poll(Duration.ofSeconds(2)));
                all.addAll(consumer2.poll(Duration.ofSeconds(2)));
                assertEquals(10, all.size(), "both consumers should receive all assigned messages");

                Set<String> values = new HashSet<>();
                for (ConsumerRecord record : all) {
                    values.add(new String(record.value(), StandardCharsets.UTF_8));
                }
                for (int i = 0; i < 10; i++) {
                    assertTrue(values.contains("msg-" + i));
                }
            }
        }
    }
}
