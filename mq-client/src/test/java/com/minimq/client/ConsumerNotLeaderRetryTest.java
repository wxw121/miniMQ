package com.minimq.client;

import com.minimq.broker.support.BrokerTestHarness;
import com.minimq.client.exception.MiniMqServerException;
import com.minimq.client.model.ConsumerRecord;
import com.minimq.common.error.ErrorCode;
import com.minimq.protocol.ApiKeys;
import com.minimq.protocol.codec.MqDecoder;
import com.minimq.protocol.message.MqResponseHeader;
import com.minimq.protocol.message.ProtocolMessages.BrokerMetadata;
import com.minimq.protocol.message.ProtocolMessages.CreateTopicRequest;
import com.minimq.protocol.message.ProtocolMessages.FetchRequest;
import com.minimq.protocol.message.ProtocolMessages.FetchResponse;
import com.minimq.protocol.message.ProtocolMessages.FetchedRecord;
import com.minimq.protocol.message.ProtocolMessages.MetadataRequest;
import com.minimq.protocol.message.ProtocolMessages.MetadataResponse;
import com.minimq.protocol.message.ProtocolMessages.PartitionMetadata;
import com.minimq.protocol.message.ProtocolMessages.TopicMetadata;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ConsumerNotLeaderRetryTest {

    @Test
    void pollRefreshesMetadataAndRetriesOnceAfterNotLeader() throws Exception {
        Path dataDir = BrokerTestHarness.newDataDir("consumer-not-leader");
        try (BrokerTestHarness controller = BrokerTestHarness.startController(dataDir);
             NetworkClient admin = new NetworkClient("admin");
             TestableConsumer consumer = new TestableConsumer(
                     controller.bootstrapServers(), "retry-group", "consumer-v13")) {
            admin.connect("127.0.0.1", controller.port());
            admin.ensureSuccess(admin.send(
                    ApiKeys.CREATE_TOPIC, new CreateTopicRequest("orders", 1)));
            MetadataResponse freshMetadata = (MetadataResponse) admin.send(
                    ApiKeys.METADATA, new MetadataRequest("orders"))
                    .body();

            consumer.subscribe("orders");
            consumer.enqueueMetadata(freshMetadata);
            consumer.countMetadataFetchesDuringPoll();
            consumer.enqueueFetchResponse(new MqDecoder.DecodedResponse(
                    new MqResponseHeader(1, ErrorCode.NOT_LEADER),
                    null));
            consumer.enqueueFetchResponse(new MqDecoder.DecodedResponse(
                    new MqResponseHeader(2, ErrorCode.NONE),
                    new FetchResponse("orders", 0, 1, List.of(new FetchedRecord(
                            0,
                            "k".getBytes(StandardCharsets.UTF_8),
                            "v".getBytes(StandardCharsets.UTF_8))))));

            List<ConsumerRecord> records = consumer.poll(Duration.ofMillis(10));

            assertEquals(1, records.size());
            assertEquals("v", new String(records.get(0).value(), StandardCharsets.UTF_8));
            assertEquals(2, consumer.fetchAttempts());
            assertEquals(1, consumer.metadataFetchesDuringPoll());
            assertEquals(1, consumer.invalidatedBrokerClients());
        }
    }

    @Test
    void pollReusesMetadataCacheUntilNotLeaderInvalidatesIt() throws Exception {
        Path dataDir = BrokerTestHarness.newDataDir("consumer-metadata-cache");
        try (BrokerTestHarness controller = BrokerTestHarness.startController(dataDir);
             NetworkClient admin = new NetworkClient("admin");
             TestableConsumer consumer = new TestableConsumer(
                     controller.bootstrapServers(), "cache-group", "consumer-cache-v13")) {
            admin.connect("127.0.0.1", controller.port());
            admin.ensureSuccess(admin.send(
                    ApiKeys.CREATE_TOPIC, new CreateTopicRequest("orders", 1)));
            MetadataResponse freshMetadata = (MetadataResponse) admin.send(
                    ApiKeys.METADATA, new MetadataRequest("orders"))
                    .body();

            consumer.subscribe("orders");
            consumer.countMetadataFetchesDuringPoll();
            consumer.enqueueMetadata(freshMetadata);
            consumer.enqueueFetchResponse(successResponse(1));
            consumer.enqueueFetchResponse(successResponse(2));
            consumer.enqueueFetchResponse(new MqDecoder.DecodedResponse(
                    new MqResponseHeader(3, ErrorCode.NOT_LEADER),
                    null));
            consumer.enqueueFetchResponse(successResponse(4));

            assertEquals(1, consumer.poll(Duration.ofMillis(10)).size());
            assertEquals(1, consumer.poll(Duration.ofMillis(10)).size());
            assertEquals(0, consumer.metadataFetchesDuringPoll());
            assertEquals(2, consumer.fetchAttempts());

            assertEquals(1, consumer.poll(Duration.ofMillis(10)).size());
            assertEquals(1, consumer.metadataFetchesDuringPoll());
            assertEquals(4, consumer.fetchAttempts());
            assertEquals(1, consumer.invalidatedBrokerClients());
        }
    }

    @Test
    void pollRetriesNotLeaderOnlyOnceAndReportsContext() throws Exception {
        Path dataDir = BrokerTestHarness.newDataDir("consumer-not-leader-budget");
        try (BrokerTestHarness controller = BrokerTestHarness.startController(dataDir);
             NetworkClient admin = new NetworkClient("admin");
             TestableConsumer consumer = new TestableConsumer(
                     controller.bootstrapServers(), "budget-group", "consumer-budget-v13")) {
            admin.connect("127.0.0.1", controller.port());
            admin.ensureSuccess(admin.send(
                    ApiKeys.CREATE_TOPIC, new CreateTopicRequest("orders", 1)));
            MetadataResponse freshMetadata = (MetadataResponse) admin.send(
                    ApiKeys.METADATA, new MetadataRequest("orders"))
                    .body();

            consumer.subscribe("orders");
            consumer.countMetadataFetchesDuringPoll();
            consumer.enqueueMetadata(freshMetadata);
            consumer.enqueueFetchResponse(new MqDecoder.DecodedResponse(
                    new MqResponseHeader(1, ErrorCode.NOT_LEADER),
                    null));
            consumer.enqueueFetchResponse(new MqDecoder.DecodedResponse(
                    new MqResponseHeader(2, ErrorCode.NOT_LEADER),
                    null));

            MiniMqServerException ex = assertThrows(
                    MiniMqServerException.class,
                    () -> consumer.poll(Duration.ofMillis(10)));

            assertEquals(ErrorCode.NOT_LEADER, ex.errorCode());
            assertEquals("FETCH topic=orders partition=0 attempt=2/2 failed: NOT_LEADER", ex.getMessage());
            assertEquals(1, consumer.metadataFetchesDuringPoll());
            assertEquals(2, consumer.fetchAttempts());
            assertEquals(1, consumer.invalidatedBrokerClients());
        }
    }

    @Test
    void pollUsesRefreshedMetadataForLaterPartitionsInSameCall() throws Exception {
        Path dataDir = BrokerTestHarness.newDataDir("consumer-refresh-propagates");
        try (BrokerTestHarness controller = BrokerTestHarness.startController(dataDir);
             NetworkClient admin = new NetworkClient("admin");
             TestableConsumer consumer = new TestableConsumer(
                     controller.bootstrapServers(), "multi-group", "consumer-multi-v13")) {
            admin.connect("127.0.0.1", controller.port());
            admin.ensureSuccess(admin.send(
                    ApiKeys.CREATE_TOPIC, new CreateTopicRequest("orders", 2)));

            MetadataResponse staleMetadata = metadataWithLeaders(0, 0);
            MetadataResponse freshMetadata = metadataWithLeaders(1, 2);
            consumer.enqueueMetadata(staleMetadata);
            consumer.subscribe("orders");
            consumer.countMetadataFetchesDuringPoll();
            consumer.enqueueMetadata(freshMetadata);
            consumer.enqueueFetchResponse(new MqDecoder.DecodedResponse(
                    new MqResponseHeader(1, ErrorCode.NOT_LEADER),
                    null));
            consumer.enqueueFetchResponse(successResponse(0));
            consumer.enqueueFetchResponse(successResponse(1));

            List<ConsumerRecord> records = consumer.poll(Duration.ofMillis(10));

            assertEquals(2, records.size());
            assertEquals(List.of(0, 1, 2), consumer.fetchLeaderBrokerIds());
            assertEquals(1, consumer.metadataFetchesDuringPoll());
            assertEquals(3, consumer.fetchAttempts());
            assertEquals(1, consumer.invalidatedBrokerClients());
        }
    }

    private static MetadataResponse metadataWithLeaders(int partition0Leader, int partition1Leader) {
        return new MetadataResponse(
                List.of(
                        new BrokerMetadata(0, "127.0.0.1", 19090),
                        new BrokerMetadata(1, "127.0.0.1", 19091),
                        new BrokerMetadata(2, "127.0.0.1", 19092)),
                List.of(new TopicMetadata(
                        "orders",
                        List.of(
                                new PartitionMetadata(0, partition0Leader),
                                new PartitionMetadata(1, partition1Leader)))));
    }

    private static MqDecoder.DecodedResponse successResponse(long offset) {
        return new MqDecoder.DecodedResponse(
                new MqResponseHeader((int) offset, ErrorCode.NONE),
                new FetchResponse("orders", 0, offset + 1, List.of(new FetchedRecord(
                        offset,
                        ("k" + offset).getBytes(StandardCharsets.UTF_8),
                        ("v" + offset).getBytes(StandardCharsets.UTF_8)))));
    }

    private static final class TestableConsumer extends Consumer {
        private final Queue<MetadataResponse> metadataResponses = new ArrayDeque<>();
        private final Queue<MqDecoder.DecodedResponse> fetchResponses = new ArrayDeque<>();
        private final List<Integer> fetchLeaderBrokerIds = new ArrayList<>();
        private int metadataFetchesDuringPoll;
        private int fetchAttempts;
        private int invalidatedBrokerClients;
        private boolean countMetadataFetchesDuringPoll;

        private TestableConsumer(String bootstrapServers, String groupId, String clientId) {
            super(bootstrapServers, groupId, clientId);
        }

        private void enqueueMetadata(MetadataResponse metadata) {
            metadataResponses.add(metadata);
        }

        private void enqueueFetchResponse(MqDecoder.DecodedResponse response) {
            fetchResponses.add(response);
        }

        private void countMetadataFetchesDuringPoll() {
            countMetadataFetchesDuringPoll = true;
        }

        private int metadataFetchesDuringPoll() {
            return metadataFetchesDuringPoll;
        }

        private int fetchAttempts() {
            return fetchAttempts;
        }

        private int invalidatedBrokerClients() {
            return invalidatedBrokerClients;
        }

        private List<Integer> fetchLeaderBrokerIds() {
            return fetchLeaderBrokerIds;
        }

        @Override
        protected MetadataResponse fetchMetadata(String topicName) throws InterruptedException {
            if (countMetadataFetchesDuringPoll) {
                metadataFetchesDuringPoll++;
            }
            if (!metadataResponses.isEmpty()) {
                return metadataResponses.remove();
            }
            return super.fetchMetadata(topicName);
        }

        @Override
        protected MqDecoder.DecodedResponse sendFetch(
                BrokerMetadata leader,
                FetchRequest request) {
            fetchAttempts++;
            fetchLeaderBrokerIds.add(leader.brokerId());
            return fetchResponses.remove();
        }

        @Override
        protected void invalidateBrokerClient(int brokerId) {
            invalidatedBrokerClients++;
        }
    }
}
