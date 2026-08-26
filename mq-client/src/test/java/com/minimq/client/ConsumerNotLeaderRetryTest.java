package com.minimq.client;

import com.minimq.broker.support.BrokerTestHarness;
import com.minimq.client.exception.MiniMqConnectionException;
import com.minimq.client.exception.MiniMqMetadataException;
import com.minimq.client.exception.MiniMqRequestTimeoutException;
import com.minimq.client.exception.MiniMqServerException;
import com.minimq.client.model.ConsumerRecord;
import com.minimq.common.error.ErrorCode;
import com.minimq.protocol.ApiKeys;
import com.minimq.protocol.codec.MqDecoder;
import com.minimq.protocol.message.MqResponseHeader;
import com.minimq.protocol.message.ProtocolMessages.BrokerMetadata;
import com.minimq.protocol.message.ProtocolMessages.CreateTopicAdminRequest;
import com.minimq.protocol.message.ProtocolMessages.FetchV2Request;
import com.minimq.protocol.message.ProtocolMessages.FetchV2Response;
import com.minimq.protocol.message.ProtocolMessages.FetchedRecord;
import com.minimq.protocol.message.ProtocolMessages.MetadataRequest;
import com.minimq.protocol.message.ProtocolMessages.MetadataResponse;
import com.minimq.protocol.message.ProtocolMessages.PartitionMetadata;
import com.minimq.protocol.message.ProtocolMessages.TopicMetadata;
import com.minimq.protocol.message.ProtocolMessages.V4BoundedList;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConsumerNotLeaderRetryTest {

    @Test
    void pollRejectsNegativeTimeout() throws Exception {
        Path dataDir = BrokerTestHarness.newDataDir("consumer-negative-timeout");
        try (BrokerTestHarness controller = BrokerTestHarness.startController(dataDir);
             TestableConsumer consumer = new TestableConsumer(
                     controller.bootstrapServers(), "negative-timeout-group", "negative-timeout-consumer")) {
            assertThrows(
                    IllegalArgumentException.class,
                    () -> consumer.poll(Duration.ofMillis(-1)));
            assertThrows(
                    NullPointerException.class,
                    () -> consumer.poll(null));
        }
    }

    @Test
    void pollReportsMissingTopicPartitionAndLeaderFromRefreshedMetadata() throws Exception {
        Path dataDir = BrokerTestHarness.newDataDir("consumer-route-validation");
        try (BrokerTestHarness controller = BrokerTestHarness.startController(dataDir);
             NetworkClient admin = new NetworkClient("consumer-route-admin");
             TestableConsumer consumer = new TestableConsumer(
                     controller.bootstrapServers(), "route-validation-group", "route-validation-consumer")) {
            admin.connect("127.0.0.1", controller.port());
            admin.ensureSuccess(admin.send(
                    ApiKeys.CREATE_TOPIC_ADMIN,
                    new CreateTopicAdminRequest(BrokerTestHarness.ADMIN_TOKEN, "orders", 1, 1)));
            consumer.subscribe("orders");

            List<MetadataResponse> invalidMetadata = List.of(
                    new MetadataResponse(
                            List.of(new BrokerMetadata(0, "127.0.0.1", 19090)), List.of()),
                    new MetadataResponse(
                            List.of(new BrokerMetadata(0, "127.0.0.1", 19090)),
                            List.of(new TopicMetadata(
                                    "orders", List.of(new PartitionMetadata(1, 0))))),
                    new MetadataResponse(
                            List.of(new BrokerMetadata(0, "127.0.0.1", 19090)),
                            List.of(new TopicMetadata(
                                    "orders", List.of(new PartitionMetadata(0, 7))))));
            List<String> expectedMessages = List.of(
                    "Topic not found: orders",
                    "Partition 0 not found for topic orders",
                    "Leader broker 7 not found");

            for (int i = 0; i < invalidMetadata.size(); i++) {
                consumer.advanceTime(Consumer.DEFAULT_METADATA_CACHE_MAX_AGE_MS + 1L);
                consumer.enqueueMetadata(invalidMetadata.get(i));
                consumer.enqueueMetadata(invalidMetadata.get(i));

                MiniMqMetadataException failure = assertThrows(
                        MiniMqMetadataException.class,
                        () -> consumer.poll(Duration.ofMillis(10)));
                assertEquals(expectedMessages.get(i), failure.getMessage());
            }
        }
    }

    @Test
    void invalidatingBrokerConnectionsClosesAndRemovesBothConnectionVariants() throws Exception {
        Path dataDir = BrokerTestHarness.newDataDir("consumer-broker-cleanup");
        try (BrokerTestHarness controller = BrokerTestHarness.startController(dataDir);
             Consumer consumer = new Consumer(
                     controller.bootstrapServers(), "cleanup-group", "cleanup-consumer")) {
            java.lang.reflect.Field clientsField = Consumer.class.getDeclaredField("brokerClients");
            clientsField.setAccessible(true);
            @SuppressWarnings("unchecked")
            Map<Integer, NetworkClient> clients =
                    (Map<Integer, NetworkClient>) clientsField.get(consumer);

            TrackingBrokerClient graceful = new TrackingBrokerClient("consumer-graceful-cleanup");
            clients.put(1, graceful);
            consumer.invalidateBrokerClient(1);
            assertTrue(graceful.closed);
            assertTrue(clients.isEmpty());

            TrackingBrokerClient bounded = new TrackingBrokerClient("consumer-bounded-cleanup");
            clients.put(2, bounded);
            consumer.invalidateBrokerClient(2, consumer.currentTimeNanos() + 1_000_000L);
            assertTrue(bounded.aborted);
            assertTrue(clients.isEmpty());
        }
    }

    @Test
    void pollRefreshesMetadataAndRetriesOnceAfterNotLeader() throws Exception {
        Path dataDir = BrokerTestHarness.newDataDir("consumer-not-leader");
        try (BrokerTestHarness controller = BrokerTestHarness.startController(dataDir);
             NetworkClient admin = new NetworkClient("admin");
             TestableConsumer consumer = new TestableConsumer(
                     controller.bootstrapServers(), "retry-group", "consumer-v13")) {
            admin.connect("127.0.0.1", controller.port());
            admin.ensureSuccess(admin.send(
                    ApiKeys.CREATE_TOPIC_ADMIN,
                    new CreateTopicAdminRequest(BrokerTestHarness.ADMIN_TOKEN, "orders", 1, 1)));
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
                    fetchV2Response(0)));

            List<ConsumerRecord> records = consumer.poll(Duration.ofMillis(10));

            assertEquals(1, records.size());
            assertEquals("v0", new String(records.get(0).value(), StandardCharsets.UTF_8));
            assertEquals(2, consumer.fetchAttempts());
            assertEquals(1, consumer.metadataFetchesDuringPoll());
            assertEquals(1, consumer.invalidatedBrokerClients());
        }
    }

    @Test
    void pollRefreshesMetadataAndRetriesOnceAfterBrokerFenced() throws Exception {
        Path dataDir = BrokerTestHarness.newDataDir("consumer-broker-fenced");
        try (BrokerTestHarness controller = BrokerTestHarness.startController(dataDir);
             NetworkClient admin = new NetworkClient("admin");
             TestableConsumer consumer = new TestableConsumer(
                     controller.bootstrapServers(), "fenced-group", "consumer-fenced")) {
            admin.connect("127.0.0.1", controller.port());
            admin.ensureSuccess(admin.send(
                    ApiKeys.CREATE_TOPIC_ADMIN,
                    new CreateTopicAdminRequest(BrokerTestHarness.ADMIN_TOKEN, "orders", 1, 1)));
            MetadataResponse freshMetadata = (MetadataResponse) admin.send(
                    ApiKeys.METADATA, new MetadataRequest("orders"))
                    .body();

            consumer.subscribe("orders");
            consumer.enqueueMetadata(freshMetadata);
            consumer.countMetadataFetchesDuringPoll();
            consumer.enqueueFetchResponse(new MqDecoder.DecodedResponse(
                    new MqResponseHeader(1, ErrorCode.BROKER_FENCED),
                    null));
            consumer.enqueueFetchResponse(successResponse(0));
            consumer.enqueueFetchResponse(successResponse(1));

            assertEquals(1, consumer.poll(Duration.ofMillis(10)).size());
            assertEquals(1, consumer.poll(Duration.ofMillis(10)).size());
            assertEquals(List.of(0L, 0L, 1L), consumer.fetchOffsets());
            assertEquals(3, consumer.fetchAttempts());
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
                    ApiKeys.CREATE_TOPIC_ADMIN,
                    new CreateTopicAdminRequest(BrokerTestHarness.ADMIN_TOKEN, "orders", 1, 1)));
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
                    ApiKeys.CREATE_TOPIC_ADMIN,
                    new CreateTopicAdminRequest(BrokerTestHarness.ADMIN_TOKEN, "orders", 1, 1)));
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
            assertEquals(
                    "FETCH topic=orders partition=0 broker=0 attempt=2/2 failed: NOT_LEADER",
                    ex.getMessage());
            assertEquals(1, consumer.metadataFetchesDuringPoll());
            assertEquals(2, consumer.fetchAttempts());
            assertEquals(1, consumer.invalidatedBrokerClients());
        }
    }

    @Test
    void pollRetriesBrokerFencedOnlyOnceWithoutAdvancingOffset() throws Exception {
        Path dataDir = BrokerTestHarness.newDataDir("consumer-broker-fenced-budget");
        try (BrokerTestHarness controller = BrokerTestHarness.startController(dataDir);
             NetworkClient admin = new NetworkClient("admin");
             TestableConsumer consumer = new TestableConsumer(
                     controller.bootstrapServers(), "fenced-budget-group", "consumer-fenced-budget")) {
            admin.connect("127.0.0.1", controller.port());
            admin.ensureSuccess(admin.send(
                    ApiKeys.CREATE_TOPIC_ADMIN,
                    new CreateTopicAdminRequest(BrokerTestHarness.ADMIN_TOKEN, "orders", 1, 1)));
            MetadataResponse freshMetadata = (MetadataResponse) admin.send(
                    ApiKeys.METADATA, new MetadataRequest("orders"))
                    .body();

            consumer.subscribe("orders");
            consumer.countMetadataFetchesDuringPoll();
            consumer.enqueueMetadata(freshMetadata);
            consumer.enqueueFetchResponse(new MqDecoder.DecodedResponse(
                    new MqResponseHeader(1, ErrorCode.BROKER_FENCED),
                    null));
            consumer.enqueueFetchResponse(new MqDecoder.DecodedResponse(
                    new MqResponseHeader(2, ErrorCode.BROKER_FENCED),
                    null));

            MiniMqServerException ex = assertThrows(
                    MiniMqServerException.class,
                    () -> consumer.poll(Duration.ofMillis(10)));

            assertEquals(ErrorCode.BROKER_FENCED, ex.errorCode());
            assertEquals(
                    "FETCH topic=orders partition=0 broker=0 attempt=2/2 failed: BROKER_FENCED",
                    ex.getMessage());
            assertEquals(List.of(0L, 0L), consumer.fetchOffsets());
            assertEquals(1, consumer.metadataFetchesDuringPoll());
            assertEquals(2, consumer.fetchAttempts());
            assertEquals(1, consumer.invalidatedBrokerClients());
        }
    }

    @Test
    void pollRefreshesMetadataAndRetriesOnceAfterPreWriteConnectionFailure() throws Exception {
        Path dataDir = BrokerTestHarness.newDataDir("consumer-pre-write-connection");
        try (BrokerTestHarness controller = BrokerTestHarness.startController(dataDir);
             NetworkClient admin = new NetworkClient("admin");
             TestableConsumer consumer = new TestableConsumer(
                     controller.bootstrapServers(), "connection-group", "consumer-connection")) {
            admin.connect("127.0.0.1", controller.port());
            admin.ensureSuccess(admin.send(
                    ApiKeys.CREATE_TOPIC_ADMIN,
                    new CreateTopicAdminRequest(
                            BrokerTestHarness.ADMIN_TOKEN, "orders", 1, 1)));
            MetadataResponse freshMetadata = (MetadataResponse) admin.send(
                    ApiKeys.METADATA, new MetadataRequest("orders")).body();

            consumer.subscribe("orders");
            consumer.countMetadataFetchesDuringPoll();
            consumer.enqueueMetadata(freshMetadata);
            consumer.enqueueFetchFailure(new MiniMqConnectionException(
                    "client is not connected", null));
            consumer.enqueueFetchResponse(successResponse(0L));

            List<ConsumerRecord> records = consumer.poll(Duration.ofMillis(50));

            assertEquals(1, records.size());
            assertEquals(2, consumer.fetchAttempts());
            assertEquals(1, consumer.metadataFetchesDuringPoll());
            assertEquals(1, consumer.invalidatedBrokerClients());
            assertTrue(consumer.invalidationTimeout() != null,
                    "Broker invalidation cleanup must use the remaining poll budget");
        }
    }

    @Test
    void pollInvalidatesBrokerWithoutReplayingTimedOutFetch() throws Exception {
        Path dataDir = BrokerTestHarness.newDataDir("consumer-fetch-timeout-no-replay");
        try (BrokerTestHarness controller = BrokerTestHarness.startController(dataDir);
             NetworkClient admin = new NetworkClient("admin");
             TestableConsumer consumer = new TestableConsumer(
                     controller.bootstrapServers(), "timeout-group", "consumer-fetch-timeout")) {
            admin.connect("127.0.0.1", controller.port());
            admin.ensureSuccess(admin.send(
                    ApiKeys.CREATE_TOPIC_ADMIN,
                    new CreateTopicAdminRequest(
                            BrokerTestHarness.ADMIN_TOKEN, "orders", 1, 1)));
            consumer.subscribe("orders");
            consumer.countMetadataFetchesDuringPoll();
            consumer.enqueueFetchFailure(new MiniMqRequestTimeoutException(
                    "FETCH_V2", Duration.ofMillis(25)));
            consumer.enqueueFetchResponse(successResponse(0L));

            assertThrows(
                    MiniMqRequestTimeoutException.class,
                    () -> consumer.poll(Duration.ofMillis(50)));

            assertEquals(1, consumer.fetchAttempts());
            assertEquals(0, consumer.metadataFetchesDuringPoll());
            assertEquals(1, consumer.invalidatedBrokerClients());
            assertTrue(consumer.invalidationTimeout() != null);
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
                    ApiKeys.CREATE_TOPIC_ADMIN,
                    new CreateTopicAdminRequest(BrokerTestHarness.ADMIN_TOKEN, "orders", 2, 1)));

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

    @Test
    void pollRefreshesExpiredCachedPartitionMetadataBeforeFetching() throws Exception {
        Path dataDir = BrokerTestHarness.newDataDir("consumer-expired-partition-metadata");
        try (BrokerTestHarness controller = BrokerTestHarness.startController(dataDir);
             NetworkClient admin = new NetworkClient("admin");
             TestableConsumer consumer = new TestableConsumer(
                     controller.bootstrapServers(), "expired-metadata-group", "consumer-expired-metadata")) {
            admin.connect("127.0.0.1", controller.port());
            admin.ensureSuccess(admin.send(
                    ApiKeys.CREATE_TOPIC_ADMIN,
                    new CreateTopicAdminRequest(BrokerTestHarness.ADMIN_TOKEN, "orders", 1, 1)));
            MetadataResponse freshMetadata = (MetadataResponse) admin.send(
                    ApiKeys.METADATA, new MetadataRequest("orders"))
                    .body();
            List<PartitionMetadata> cachedPartitions = new ArrayList<>(
                    freshMetadata.topics().get(0).partitions());
            MetadataResponse cachedMetadata = new MetadataResponse(
                    freshMetadata.brokers(),
                    List.of(new TopicMetadata("orders", cachedPartitions)));

            consumer.enqueueMetadata(cachedMetadata);
            consumer.subscribe("orders");
            cachedPartitions.clear();

            consumer.countMetadataFetchesDuringPoll();
            consumer.enqueueMetadata(freshMetadata);
            consumer.enqueueFetchResponse(successResponse(0));

            List<ConsumerRecord> records = consumer.poll(Duration.ofMillis(10));

            assertEquals(1, records.size());
            assertEquals(1, consumer.metadataFetchesDuringPoll());
            assertEquals(1, consumer.fetchAttempts());
        }
    }

    @Test
    void pollUsesFetchV2NextVisibleTimestampBeforeRetryingSameOffset() throws Exception {
        Path dataDir = BrokerTestHarness.newDataDir("consumer-delayed-fetch-v2");
        try (BrokerTestHarness controller = BrokerTestHarness.startController(dataDir);
             NetworkClient admin = new NetworkClient("admin");
             TestableConsumer consumer = new TestableConsumer(
                     controller.bootstrapServers(), "delayed-group", "consumer-delayed-v2")) {
            admin.connect("127.0.0.1", controller.port());
            admin.ensureSuccess(admin.send(
                    ApiKeys.CREATE_TOPIC_ADMIN,
                    new CreateTopicAdminRequest(BrokerTestHarness.ADMIN_TOKEN, "orders", 1, 1)));
            MetadataResponse freshMetadata = (MetadataResponse) admin.send(
                    ApiKeys.METADATA, new MetadataRequest("orders"))
                    .body();

            consumer.subscribe("orders");
            consumer.enqueueMetadata(freshMetadata);
            consumer.countMetadataFetchesDuringPoll();
            long nextVisibleAtEpochMs = consumer.currentTimeMillis() + 25;
            consumer.enqueueFetchResponse(new MqDecoder.DecodedResponse(
                    new MqResponseHeader(1, ErrorCode.NONE),
                    new FetchV2Response(
                            "orders",
                            0,
                            1,
                            0,
                            nextVisibleAtEpochMs,
                            new V4BoundedList<>(0, false, List.of()))));
            consumer.enqueueFetchResponse(successResponse(0));

            List<ConsumerRecord> records = consumer.poll(Duration.ofMillis(50));

            assertEquals(1, records.size());
            assertEquals(List.of(0L, 0L), consumer.fetchOffsets());
            assertEquals(List.of(25L), consumer.pollSleeps());
            assertEquals(2, consumer.fetchAttempts());
        }
    }

    @Test
    void pollDoesNotFetchAgainAfterSleepingToDeadlineWithoutVisibilityHint() throws Exception {
        Path dataDir = BrokerTestHarness.newDataDir("consumer-poll-deadline-no-hint");
        try (BrokerTestHarness controller = BrokerTestHarness.startController(dataDir);
             NetworkClient admin = new NetworkClient("admin");
             TestableConsumer consumer = new TestableConsumer(
                     controller.bootstrapServers(), "deadline-group", "consumer-deadline")) {
            admin.connect("127.0.0.1", controller.port());
            admin.ensureSuccess(admin.send(
                    ApiKeys.CREATE_TOPIC_ADMIN,
                    new CreateTopicAdminRequest(BrokerTestHarness.ADMIN_TOKEN, "orders", 1, 1)));
            MetadataResponse metadata = (MetadataResponse) admin.send(
                    ApiKeys.METADATA, new MetadataRequest("orders")).body();
            consumer.subscribe("orders");
            consumer.enqueueMetadata(metadata);
            consumer.enqueueFetchResponse(emptyFetchResponse(-1L));
            consumer.enqueueFetchResponse(emptyFetchResponse(-1L));

            assertEquals(List.of(), consumer.poll(Duration.ofMillis(25)));
            assertEquals(List.of(25L), consumer.pollSleeps());
            assertEquals(1, consumer.fetchAttempts());
        }
    }

    @Test
    void pollReturnsEmptyWhenDeadlineExpiresBeforeAnotherFetch() throws Exception {
        Path dataDir = BrokerTestHarness.newDataDir("consumer-poll-sub-ms-deadline");
        try (BrokerTestHarness controller = BrokerTestHarness.startController(dataDir);
             NetworkClient admin = new NetworkClient("admin");
             TestableConsumer consumer = new TestableConsumer(
                     controller.bootstrapServers(), "sub-ms-group", "consumer-sub-ms")) {
            admin.connect("127.0.0.1", controller.port());
            admin.ensureSuccess(admin.send(
                    ApiKeys.CREATE_TOPIC_ADMIN,
                    new CreateTopicAdminRequest(BrokerTestHarness.ADMIN_TOKEN, "orders", 1, 1)));
            MetadataResponse metadata = (MetadataResponse) admin.send(
                    ApiKeys.METADATA, new MetadataRequest("orders")).body();
            consumer.subscribe("orders");
            consumer.enqueueMetadata(metadata);
            consumer.enqueueFetchResponse(emptyFetchResponse(-1L));
            consumer.advanceTime(Consumer.DEFAULT_METADATA_CACHE_MAX_AGE_MS + 1);
            consumer.delayNextMetadataFetch(50);

            assertEquals(List.of(), consumer.poll(Duration.ofMillis(50)));
            assertEquals(0, consumer.fetchAttempts());
        }
    }

    @Test
    void pollReturnsEmptyWhenFetchTimeoutExhaustsDeadline() throws Exception {
        Path dataDir = BrokerTestHarness.newDataDir("consumer-poll-fetch-deadline");
        try (BrokerTestHarness controller = BrokerTestHarness.startController(dataDir);
             NetworkClient admin = new NetworkClient("admin");
             TestableConsumer consumer = new TestableConsumer(
                     controller.bootstrapServers(), "fetch-deadline-group", "consumer-fetch-deadline")) {
            admin.connect("127.0.0.1", controller.port());
            admin.ensureSuccess(admin.send(
                    ApiKeys.CREATE_TOPIC_ADMIN,
                    new CreateTopicAdminRequest(BrokerTestHarness.ADMIN_TOKEN, "orders", 1, 1)));
            consumer.subscribe("orders");
            consumer.delayNextFetch(50);
            consumer.enqueueFetchFailure(new MiniMqRequestTimeoutException(
                    "FETCH_V2", Duration.ZERO));

            assertEquals(List.of(), consumer.poll(Duration.ofMillis(50)));
            assertEquals(1, consumer.fetchAttempts());
            assertEquals(1, consumer.invalidatedBrokerClients());
        }
    }

    @Test
    void pollDoesNotFetchAgainWhenNextVisibilityIsBeyondDeadline() throws Exception {
        Path dataDir = BrokerTestHarness.newDataDir("consumer-poll-deadline-future-hint");
        try (BrokerTestHarness controller = BrokerTestHarness.startController(dataDir);
             NetworkClient admin = new NetworkClient("admin");
             TestableConsumer consumer = new TestableConsumer(
                     controller.bootstrapServers(), "future-group", "consumer-future")) {
            admin.connect("127.0.0.1", controller.port());
            admin.ensureSuccess(admin.send(
                    ApiKeys.CREATE_TOPIC_ADMIN,
                    new CreateTopicAdminRequest(BrokerTestHarness.ADMIN_TOKEN, "orders", 1, 1)));
            MetadataResponse metadata = (MetadataResponse) admin.send(
                    ApiKeys.METADATA, new MetadataRequest("orders")).body();
            consumer.subscribe("orders");
            consumer.enqueueMetadata(metadata);
            consumer.enqueueFetchResponse(emptyFetchResponse(consumer.currentTimeMillis() + 100L));
            consumer.enqueueFetchResponse(emptyFetchResponse(-1L));

            assertEquals(List.of(), consumer.poll(Duration.ofMillis(25)));
            assertEquals(List.of(25L), consumer.pollSleeps());
            assertEquals(1, consumer.fetchAttempts());
        }
    }

    @Test
    void pollTimeoutIncludesMetadataRefreshTime() throws Exception {
        Path dataDir = BrokerTestHarness.newDataDir("consumer-poll-metadata-deadline");
        try (BrokerTestHarness controller = BrokerTestHarness.startController(dataDir);
             NetworkClient admin = new NetworkClient("admin");
             TestableConsumer consumer = new TestableConsumer(
                     controller.bootstrapServers(), "metadata-deadline-group", "consumer-metadata-deadline")) {
            admin.connect("127.0.0.1", controller.port());
            admin.ensureSuccess(admin.send(
                    ApiKeys.CREATE_TOPIC_ADMIN,
                    new CreateTopicAdminRequest(BrokerTestHarness.ADMIN_TOKEN, "orders", 1, 1)));
            MetadataResponse metadata = (MetadataResponse) admin.send(
                    ApiKeys.METADATA, new MetadataRequest("orders")).body();

            consumer.enqueueMetadata(metadata);
            consumer.subscribe("orders");
            consumer.advanceTime(Consumer.DEFAULT_METADATA_CACHE_MAX_AGE_MS + 1L);
            consumer.enqueueMetadata(metadata);
            consumer.countMetadataFetchesDuringPoll();
            consumer.delayNextMetadataFetch(20L);
            consumer.enqueueFetchResponse(emptyFetchResponse(-1L));
            long startedAtMillis = consumer.currentTimeMillis();

            assertEquals(List.of(), consumer.poll(Duration.ofMillis(25)));

            assertEquals(25L, consumer.currentTimeMillis() - startedAtMillis);
            assertEquals(List.of(5L), consumer.pollSleeps());
            assertEquals(1, consumer.metadataFetchesDuringPoll());
            assertEquals(1, consumer.fetchAttempts());
        }
    }

    @Test
    void pollPassesOneMonotonicDeadlineThroughMetadataAndFetch() throws Exception {
        Path dataDir = BrokerTestHarness.newDataDir("consumer-poll-shared-deadline");
        try (BrokerTestHarness controller = BrokerTestHarness.startController(dataDir);
             NetworkClient admin = new NetworkClient("admin");
             TestableConsumer consumer = new TestableConsumer(
                     controller.bootstrapServers(), "shared-deadline-group", "consumer-shared-deadline")) {
            admin.connect("127.0.0.1", controller.port());
            admin.ensureSuccess(admin.send(
                    ApiKeys.CREATE_TOPIC_ADMIN,
                    new CreateTopicAdminRequest(BrokerTestHarness.ADMIN_TOKEN, "orders", 1, 1)));
            MetadataResponse metadata = (MetadataResponse) admin.send(
                    ApiKeys.METADATA, new MetadataRequest("orders")).body();

            consumer.enqueueMetadata(metadata);
            consumer.subscribe("orders");
            consumer.advanceTime(Consumer.DEFAULT_METADATA_CACHE_MAX_AGE_MS + 1L);
            consumer.enqueueMetadata(metadata);
            consumer.countMetadataFetchesDuringPoll();
            consumer.delayNextMetadataFetch(10L);
            consumer.delayNextFetch(5L);
            consumer.enqueueFetchResponse(emptyFetchResponse(-1L));
            long startedAtMillis = consumer.currentTimeMillis();

            assertEquals(List.of(), consumer.poll(Duration.ofMillis(25)));

            assertEquals(Duration.ofMillis(25), consumer.metadataTimeout());
            assertEquals(Duration.ofMillis(15), consumer.fetchTimeout());
            assertEquals(List.of(10L), consumer.pollSleeps());
            assertEquals(25L, consumer.currentTimeMillis() - startedAtMillis);
        }
    }

    private static MqDecoder.DecodedResponse emptyFetchResponse(long nextVisibleAtEpochMs) {
        return new MqDecoder.DecodedResponse(
                new MqResponseHeader(1, ErrorCode.NONE),
                new FetchV2Response(
                        "orders", 0, 0L, 0L, nextVisibleAtEpochMs,
                        new V4BoundedList<>(0, false, List.of())));
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
                fetchV2Response(offset));
    }

    private static FetchV2Response fetchV2Response(long offset) {
        return new FetchV2Response(
                "orders",
                0,
                offset + 1,
                offset + 1,
                -1L,
                new V4BoundedList<>(1, false, List.of(new FetchedRecord(
                        offset,
                        ("k" + offset).getBytes(StandardCharsets.UTF_8),
                        ("v" + offset).getBytes(StandardCharsets.UTF_8)))));
    }

    private static final class TestableConsumer extends Consumer {
        private final Queue<MetadataResponse> metadataResponses = new ArrayDeque<>();
        private final Queue<Object> fetchResponses = new ArrayDeque<>();
        private final List<Integer> fetchLeaderBrokerIds = new ArrayList<>();
        private final List<Long> fetchOffsets = new ArrayList<>();
        private final List<Long> pollSleeps = new ArrayList<>();
        private long currentTimeMillis = 1_000_000L;
        private long nextMetadataFetchDelayMs;
        private long nextFetchDelayMs;
        private Duration metadataTimeout;
        private Duration fetchTimeout;
        private Duration invalidationTimeout;
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

        private void enqueueFetchFailure(RuntimeException failure) {
            fetchResponses.add(failure);
        }

        private void countMetadataFetchesDuringPoll() {
            countMetadataFetchesDuringPoll = true;
        }

        private void advanceTime(long elapsedMs) {
            currentTimeMillis += elapsedMs;
        }

        private void delayNextMetadataFetch(long delayMs) {
            nextMetadataFetchDelayMs = delayMs;
        }

        private void delayNextFetch(long delayMs) {
            nextFetchDelayMs = delayMs;
        }

        private Duration metadataTimeout() {
            return metadataTimeout;
        }

        private Duration fetchTimeout() {
            return fetchTimeout;
        }

        private Duration invalidationTimeout() {
            return invalidationTimeout;
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

        private List<Long> fetchOffsets() {
            return fetchOffsets;
        }

        private List<Long> pollSleeps() {
            return pollSleeps;
        }

        @Override
        protected MetadataResponse fetchMetadata(String topicName) throws InterruptedException {
            if (countMetadataFetchesDuringPoll) {
                metadataFetchesDuringPoll++;
            }
            if (nextMetadataFetchDelayMs > 0L) {
                currentTimeMillis += nextMetadataFetchDelayMs;
                nextMetadataFetchDelayMs = 0L;
            }
            if (!metadataResponses.isEmpty()) {
                return metadataResponses.remove();
            }
            return super.fetchMetadata(topicName);
        }

        protected MetadataResponse fetchMetadata(String topicName, long deadlineNanos)
                throws InterruptedException {
            metadataTimeout = remainingTimeout(deadlineNanos);
            MetadataResponse metadata = fetchMetadata(topicName);
            if (deadlineNanos - currentTimeNanos() < TimeUnit.MILLISECONDS.toNanos(1L)) {
                throw new MiniMqRequestTimeoutException("METADATA", Duration.ZERO);
            }
            return metadata;
        }

        @Override
        protected MqDecoder.DecodedResponse sendFetchV2(
                BrokerMetadata leader,
                FetchV2Request request) {
            fetchAttempts++;
            fetchLeaderBrokerIds.add(leader.brokerId());
            fetchOffsets.add(request.fetchOffset());
            if (nextFetchDelayMs > 0L) {
                currentTimeMillis += nextFetchDelayMs;
                nextFetchDelayMs = 0L;
            }
            Object outcome = fetchResponses.remove();
            if (outcome instanceof RuntimeException failure) {
                throw failure;
            }
            return (MqDecoder.DecodedResponse) outcome;
        }

        protected MqDecoder.DecodedResponse sendFetchV2(
                BrokerMetadata leader,
                FetchV2Request request,
                long deadlineNanos) {
            fetchTimeout = remainingTimeout(deadlineNanos);
            return sendFetchV2(leader, request);
        }

        private Duration remainingTimeout(long deadlineNanos) {
            return Duration.ofNanos(Math.max(0L, deadlineNanos - currentTimeNanos()));
        }

        @Override
        protected long currentTimeMillis() {
            return currentTimeMillis;
        }

        protected long currentTimeNanos() {
            return currentTimeMillis * 1_000_000L;
        }

        @Override
        protected void sleepForPoll(long sleepMs) {
            pollSleeps.add(sleepMs);
            currentTimeMillis += sleepMs;
        }

        @Override
        protected void invalidateBrokerClient(int brokerId) {
            invalidatedBrokerClients++;
        }

        protected void invalidateBrokerClient(int brokerId, long deadlineNanos) {
            invalidationTimeout = remainingTimeout(deadlineNanos);
            invalidateBrokerClient(brokerId);
        }
    }

    private static final class TrackingBrokerClient extends NetworkClient {
        private boolean closed;
        private boolean aborted;

        private TrackingBrokerClient(String clientId) {
            super(clientId);
        }

        @Override
        public void close() throws InterruptedException {
            closed = true;
            super.close();
        }

        @Override
        void abortCandidate(Duration timeout) throws InterruptedException {
            aborted = true;
            super.abortCandidate(timeout);
        }
    }
}
