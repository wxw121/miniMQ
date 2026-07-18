package com.minimq.broker.replication;

import com.minimq.broker.BrokerConfig;
import com.minimq.broker.PartitionManager;
import com.minimq.broker.controller.ClusterController;
import com.minimq.broker.metadata.MetadataStore;
import com.minimq.common.error.ErrorCode;
import com.minimq.protocol.message.ProtocolMessages.CreateTopicRequest;
import com.minimq.protocol.message.ProtocolMessages.FetchedRecord;
import com.minimq.protocol.message.ProtocolMessages.RegisterBrokerRequest;
import com.minimq.protocol.message.ProtocolMessages.ReplicaFetchRequest;
import com.minimq.protocol.message.ProtocolMessages.ReplicaFetchResponse;
import com.minimq.storage.LogManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ReplicaFetcherTest {

    @TempDir
    Path tempDir;

    @Test
    void followerFetchesLeaderRecordsIntoLocalPartitionLog() throws Exception {
        AtomicReference<ReplicaFetchRequest> captured = new AtomicReference<>();
        try (LeaderReplicaClientTest.TestLeader leader = LeaderReplicaClientTest.TestLeader.start(
                ErrorCode.NONE,
                new ReplicaFetchResponse("orders", 0, 1L,
                        List.of(new FetchedRecord(0L, "k".getBytes(), "v".getBytes()))),
                captured)) {
            ClusterController controller = new ClusterController(new MetadataStore(tempDir.resolve("controller")));
            controller.registerBroker(new RegisterBrokerRequest(0, "127.0.0.1", leader.port()));
            controller.registerBroker(new RegisterBrokerRequest(1, "127.0.0.1", 19093));
            assertEquals(ErrorCode.NONE, controller.createTopic(new CreateTopicRequest("orders", 1, 2)));

            PartitionManager partitionManager = new PartitionManager(new LogManager(tempDir.resolve("follower")));
            ReplicaFetcher fetcher = new ReplicaFetcher(
                    new BrokerConfig(1, "127.0.0.1", 19093, tempDir.resolve("broker-1"),
                            true, "127.0.0.1", 9092, false),
                    controller,
                    partitionManager);
            try {
                fetcher.start();
                waitForHighWatermark(partitionManager, "orders", 0, 1L);

                assertEquals(new ReplicaFetchRequest("orders", 0, 1, 0L, 1024 * 1024), captured.get());
                assertEquals(1L, partitionManager.highWatermark("orders", 0));
            } finally {
                fetcher.close();
                partitionManager.close();
            }
        }
    }

    private static void waitForHighWatermark(
            PartitionManager partitionManager,
            String topicName,
            int partitionId,
            long expectedHighWatermark) throws InterruptedException {
        long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(5);
        while (System.nanoTime() < deadline) {
            if (partitionManager.highWatermark(topicName, partitionId) >= expectedHighWatermark) {
                return;
            }
            Thread.sleep(25);
        }
        assertEquals(expectedHighWatermark, partitionManager.highWatermark(topicName, partitionId));
    }
}