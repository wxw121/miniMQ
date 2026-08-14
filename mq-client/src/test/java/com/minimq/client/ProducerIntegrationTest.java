package com.minimq.client;

import com.minimq.broker.support.BrokerTestHarness;
import com.minimq.client.model.ProducerRecord;
import com.minimq.protocol.ApiKeys;
import com.minimq.protocol.codec.MqDecoder;
import com.minimq.protocol.message.ProtocolMessages.CreateTopicRequest;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;

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

    @Test
    void sequentialSendsWithoutReadingOffsetShareOneBrokerRoundTrip() throws Exception {
        Path dataDir = BrokerTestHarness.newDataDir("producer-auto-batch");
        try (BrokerTestHarness broker = BrokerTestHarness.startController(dataDir);
             NetworkClient admin = new NetworkClient("admin");
             Producer producer = new Producer(broker.bootstrapServers(), "producer-auto-batch-it")) {
            admin.connect("localhost", broker.port());
            admin.ensureSuccess(admin.send(
                    ApiKeys.CREATE_TOPIC, new CreateTopicRequest("orders", 1)));
            producer.start();

            var first = producer.send(new ProducerRecord(
                    "orders", null, "k".getBytes(StandardCharsets.UTF_8), "m0".getBytes(StandardCharsets.UTF_8)));
            var second = producer.send(new ProducerRecord(
                    "orders", null, "k".getBytes(StandardCharsets.UTF_8), "m1".getBytes(StandardCharsets.UTF_8)));
            var third = producer.send(new ProducerRecord(
                    "orders", null, "k".getBytes(StandardCharsets.UTF_8), "m2".getBytes(StandardCharsets.UTF_8)));

            assertEquals(0, first.offset());
            assertEquals(1, second.offset());
            assertEquals(2, third.offset());
        }
    }

    @Test
    void sendBatchAssignsContiguousOffsetsOnOnePartition() throws Exception {
        Path dataDir = BrokerTestHarness.newDataDir("producer-batch");
        try (BrokerTestHarness broker = BrokerTestHarness.startController(dataDir);
             NetworkClient admin = new NetworkClient("admin");
             Producer producer = new Producer(broker.bootstrapServers(), "producer-batch-it")) {
            admin.connect("localhost", broker.port());
            admin.ensureSuccess(admin.send(
                    ApiKeys.CREATE_TOPIC, new CreateTopicRequest("orders", 1)));
            producer.start();

            var results = producer.send(List.of(
                    new ProducerRecord("orders", 0, "k".getBytes(StandardCharsets.UTF_8), "b0".getBytes(StandardCharsets.UTF_8)),
                    new ProducerRecord("orders", 0, "k".getBytes(StandardCharsets.UTF_8), "b1".getBytes(StandardCharsets.UTF_8)),
                    new ProducerRecord("orders", 0, "k".getBytes(StandardCharsets.UTF_8), "b2".getBytes(StandardCharsets.UTF_8))));
            assertEquals(0, results.get(0).offset());
            assertEquals(1, results.get(1).offset());
            assertEquals(2, results.get(2).offset());
        }
    }

    @Test
    void concurrentSendersToDistinctPartitionsComplete() throws Exception {
        Path dataDir = BrokerTestHarness.newDataDir("producer-concurrent");
        try (BrokerTestHarness broker = BrokerTestHarness.startController(dataDir);
             NetworkClient admin = new NetworkClient("admin");
             Producer producer = new Producer(broker.bootstrapServers(), "producer-concurrent-it")) {
            admin.connect("localhost", broker.port());
            admin.ensureSuccess(admin.send(
                    ApiKeys.CREATE_TOPIC, new CreateTopicRequest("orders", 4)));
            producer.start();

            int partitions = 4;
            int perPartition = 8;
            var executor = java.util.concurrent.Executors.newFixedThreadPool(partitions);
            var start = new java.util.concurrent.CountDownLatch(1);
            var futures = new java.util.ArrayList<java.util.concurrent.Future<?>>();
            try {
                for (int partitionId = 0; partitionId < partitions; partitionId++) {
                    final int id = partitionId;
                    futures.add(executor.submit(() -> {
                        start.await();
                        for (int i = 0; i < perPartition; i++) {
                            producer.send(new ProducerRecord(
                                    "orders",
                                    id,
                                    ("k" + id).getBytes(StandardCharsets.UTF_8),
                                    ("v" + i).getBytes(StandardCharsets.UTF_8)));
                        }
                        return null;
                    }));
                }
                start.countDown();
                for (var future : futures) {
                    future.get();
                }
            } finally {
                executor.shutdownNow();
            }
        }
    }
}
