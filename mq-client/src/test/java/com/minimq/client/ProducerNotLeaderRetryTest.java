package com.minimq.client;

import com.minimq.broker.support.BrokerTestHarness;
import com.minimq.client.exception.MiniMqConnectionException;
import com.minimq.client.exception.MiniMqRequestTimeoutException;
import com.minimq.client.exception.MiniMqServerException;
import com.minimq.client.model.ProduceResult;
import com.minimq.client.model.ProducerRecord;
import com.minimq.common.error.ErrorCode;
import com.minimq.protocol.ApiKeys;
import com.minimq.protocol.codec.MqDecoder;
import com.minimq.protocol.message.MqResponseHeader;
import com.minimq.protocol.message.ProtocolMessages.CreateTopicAdminRequest;
import com.minimq.protocol.message.ProtocolMessages.BrokerMetadata;
import com.minimq.protocol.message.ProtocolMessages.MetadataRequest;
import com.minimq.protocol.message.ProtocolMessages.MetadataResponse;
import com.minimq.protocol.message.ProtocolMessages.MetadataV2Response;
import com.minimq.protocol.message.ProtocolMessages.PartitionMetadata;
import com.minimq.protocol.message.ProtocolMessages.ProduceRequest;
import com.minimq.protocol.message.ProtocolMessages.ProduceResponse;
import com.minimq.protocol.message.ProtocolMessages.ProduceV2Request;
import com.minimq.protocol.message.ProtocolMessages.ProduceV2Response;
import com.minimq.protocol.message.ProtocolMessages.TopicMetadata;
import com.minimq.protocol.message.ProtocolMessages.TopicSummary;
import com.minimq.protocol.message.ProtocolMessages.V4BoundedList;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProducerNotLeaderRetryTest {

    @Test
    void sendRefreshesMetadataAndRetriesOnceAfterNotLeader() throws Exception {
        Path dataDir = BrokerTestHarness.newDataDir("producer-not-leader");
        try (BrokerTestHarness controller = BrokerTestHarness.startController(dataDir);
             NetworkClient admin = new NetworkClient("admin");
             TestableProducer producer = new TestableProducer(controller.bootstrapServers(), "producer-v13")) {
            admin.connect("127.0.0.1", controller.port());
            admin.ensureSuccess(admin.send(
                    ApiKeys.CREATE_TOPIC_ADMIN,
                    new CreateTopicAdminRequest(BrokerTestHarness.ADMIN_TOKEN, "orders", 1, 1)));

            producer.start();
            MetadataResponse freshMetadata = (MetadataResponse) admin.send(
                    ApiKeys.METADATA, new MetadataRequest("orders"))
                    .body();
            producer.enqueueProduceResponse(new MqDecoder.DecodedResponse(
                    new MqResponseHeader(1, ErrorCode.NOT_LEADER),
                    null));
            producer.enqueueProduceResponse(new MqDecoder.DecodedResponse(
                    new MqResponseHeader(2, ErrorCode.NONE),
                    new ProduceResponse("orders", 0, 7)));
            producer.enqueueMetadata(freshMetadata);

            assertEquals(7, producer.send(new ProducerRecord(
                    "orders",
                    null,
                    "k".getBytes(StandardCharsets.UTF_8),
                    "v".getBytes(StandardCharsets.UTF_8))).offset());
            assertEquals(2, producer.produceAttempts());
            assertEquals(2, producer.metadataFetches());
            assertEquals(1, producer.invalidatedBrokerClients());
        }
    }

    @Test
    void sendRefreshesMetadataAndRetriesOnceAfterBrokerFenced() throws Exception {
        Path dataDir = BrokerTestHarness.newDataDir("producer-broker-fenced");
        try (BrokerTestHarness controller = BrokerTestHarness.startController(dataDir);
             NetworkClient admin = new NetworkClient("admin");
             TestableProducer producer = new TestableProducer(controller.bootstrapServers(), "producer-fenced")) {
            admin.connect("127.0.0.1", controller.port());
            admin.ensureSuccess(admin.send(
                    ApiKeys.CREATE_TOPIC_ADMIN,
                    new CreateTopicAdminRequest(BrokerTestHarness.ADMIN_TOKEN, "orders", 1, 1)));

            producer.start();
            MetadataResponse freshMetadata = (MetadataResponse) admin.send(
                    ApiKeys.METADATA, new MetadataRequest("orders"))
                    .body();
            producer.enqueueProduceResponse(new MqDecoder.DecodedResponse(
                    new MqResponseHeader(1, ErrorCode.BROKER_FENCED),
                    null));
            producer.enqueueProduceResponse(successResponse(7));
            producer.enqueueMetadata(freshMetadata);

            assertEquals(7, send(producer).offset());
            assertEquals(2, producer.produceAttempts());
            assertEquals(2, producer.metadataFetches());
            assertEquals(1, producer.invalidatedBrokerClients());
        }
    }

    @Test
    void sendReusesMetadataCacheUntilNotLeaderInvalidatesIt() throws Exception {
        Path dataDir = BrokerTestHarness.newDataDir("producer-metadata-cache");
        try (BrokerTestHarness controller = BrokerTestHarness.startController(dataDir);
             NetworkClient admin = new NetworkClient("admin");
             TestableProducer producer = new TestableProducer(controller.bootstrapServers(), "producer-cache-v13")) {
            admin.connect("127.0.0.1", controller.port());
            admin.ensureSuccess(admin.send(
                    ApiKeys.CREATE_TOPIC_ADMIN,
                    new CreateTopicAdminRequest(BrokerTestHarness.ADMIN_TOKEN, "orders", 1, 1)));

            producer.start();
            MetadataResponse freshMetadata = (MetadataResponse) admin.send(
                    ApiKeys.METADATA, new MetadataRequest("orders"))
                    .body();
            producer.enqueueMetadata(freshMetadata);
            producer.enqueueMetadata(freshMetadata);
            producer.enqueueProduceResponse(successResponse(1));
            producer.enqueueProduceResponse(successResponse(2));
            producer.enqueueProduceResponse(new MqDecoder.DecodedResponse(
                    new MqResponseHeader(3, ErrorCode.NOT_LEADER),
                    null));
            producer.enqueueProduceResponse(successResponse(4));

            assertEquals(1, send(producer).offset());
            assertEquals(2, send(producer).offset());
            assertEquals(1, producer.metadataFetches());
            assertEquals(2, producer.produceAttempts());

            assertEquals(4, send(producer).offset());
            assertEquals(2, producer.metadataFetches());
            assertEquals(4, producer.produceAttempts());
            assertEquals(1, producer.invalidatedBrokerClients());
        }
    }

    @Test
    void sendRetriesNotLeaderOnlyOnceAndReportsContext() throws Exception {
        Path dataDir = BrokerTestHarness.newDataDir("producer-not-leader-budget");
        try (BrokerTestHarness controller = BrokerTestHarness.startController(dataDir);
             NetworkClient admin = new NetworkClient("admin");
             TestableProducer producer = new TestableProducer(controller.bootstrapServers(), "producer-budget-v13")) {
            admin.connect("127.0.0.1", controller.port());
            admin.ensureSuccess(admin.send(
                    ApiKeys.CREATE_TOPIC_ADMIN,
                    new CreateTopicAdminRequest(BrokerTestHarness.ADMIN_TOKEN, "orders", 1, 1)));

            producer.start();
            MetadataResponse freshMetadata = (MetadataResponse) admin.send(
                    ApiKeys.METADATA, new MetadataRequest("orders"))
                    .body();
            producer.enqueueMetadata(freshMetadata);
            producer.enqueueMetadata(freshMetadata);
            producer.enqueueProduceResponse(new MqDecoder.DecodedResponse(
                    new MqResponseHeader(1, ErrorCode.NOT_LEADER),
                    null));
            producer.enqueueProduceResponse(new MqDecoder.DecodedResponse(
                    new MqResponseHeader(2, ErrorCode.NOT_LEADER),
                    null));

            MiniMqServerException ex = assertThrows(MiniMqServerException.class, () -> send(producer));

            assertEquals(ErrorCode.NOT_LEADER, ex.errorCode());
            assertEquals(
                    "PRODUCE topic=orders partition=0 broker=0 attempt=2/2 failed: NOT_LEADER",
                    ex.getMessage());
            assertEquals(2, producer.metadataFetches());
            assertEquals(2, producer.produceAttempts());
            assertEquals(1, producer.invalidatedBrokerClients());
        }
    }

    @Test
    void sendRetriesBrokerFencedOnlyOnceAndReportsContext() throws Exception {
        Path dataDir = BrokerTestHarness.newDataDir("producer-broker-fenced-budget");
        try (BrokerTestHarness controller = BrokerTestHarness.startController(dataDir);
             NetworkClient admin = new NetworkClient("admin");
             TestableProducer producer = new TestableProducer(
                     controller.bootstrapServers(), "producer-fenced-budget")) {
            admin.connect("127.0.0.1", controller.port());
            admin.ensureSuccess(admin.send(
                    ApiKeys.CREATE_TOPIC_ADMIN,
                    new CreateTopicAdminRequest(BrokerTestHarness.ADMIN_TOKEN, "orders", 1, 1)));

            producer.start();
            MetadataResponse freshMetadata = (MetadataResponse) admin.send(
                    ApiKeys.METADATA, new MetadataRequest("orders"))
                    .body();
            producer.enqueueMetadata(freshMetadata);
            producer.enqueueMetadata(freshMetadata);
            producer.enqueueProduceResponse(new MqDecoder.DecodedResponse(
                    new MqResponseHeader(1, ErrorCode.BROKER_FENCED),
                    null));
            producer.enqueueProduceResponse(new MqDecoder.DecodedResponse(
                    new MqResponseHeader(2, ErrorCode.BROKER_FENCED),
                    null));

            MiniMqServerException ex = assertThrows(MiniMqServerException.class, () -> send(producer));

            assertEquals(ErrorCode.BROKER_FENCED, ex.errorCode());
            assertEquals(
                    "PRODUCE topic=orders partition=0 broker=0 attempt=2/2 failed: BROKER_FENCED",
                    ex.getMessage());
            assertEquals(2, producer.metadataFetches());
            assertEquals(2, producer.produceAttempts());
            assertEquals(1, producer.invalidatedBrokerClients());
        }
    }

    @Test
    void sendRefreshesMetadataAndRetriesOnceAfterPreWriteConnectionFailure() throws Exception {
        try (TestableProducer producer = new TestableProducer(
                "127.0.0.1:1", "producer-pre-write-connection")) {
            producer.enqueueMetadata(metadataFor("orders"));
            producer.enqueueMetadata(metadataFor("orders"));
            producer.enqueueProduceFailure(new MiniMqConnectionException(
                    "client is not connected", null));
            producer.enqueueProduceResponse(successResponse(9L));

            ProduceResult result = send(producer);

            assertEquals(9L, result.offset());
            assertEquals(2, producer.metadataFetches());
            assertEquals(2, producer.produceAttempts());
            assertEquals(1, producer.invalidatedBrokerClients());
        }
    }

    @Test
    void sendToDeadLetterResolvesMetadataV2LinkAndUsesOrdinaryProduce() throws Exception {
        try (TestableProducer producer = new TestableProducer("127.0.0.1:1", "producer-dlq")) {
            producer.enqueueMetadataV2(metadataV2WithLinks("orders", "orders-retry", "orders-dlq"));
            producer.enqueueMetadata(metadataFor("orders-dlq"));
            producer.enqueueProduceResponse(new MqDecoder.DecodedResponse(
                    new MqResponseHeader(1, ErrorCode.NONE),
                    new ProduceResponse("orders-dlq", 0, 41L)));

            ProducerRecord original = new ProducerRecord(
                    "orders",
                    null,
                    "retry-key".getBytes(StandardCharsets.UTF_8),
                    "retry-value".getBytes(StandardCharsets.UTF_8));
            ProduceResult result = producer.sendToDeadLetter("orders", original);

            assertEquals("orders-dlq", result.topic());
            assertEquals(41L, result.offset());
            assertEquals(1, producer.metadataV2Fetches());
            assertEquals(1, producer.produceAttempts());
            assertEquals("orders-dlq", producer.lastProduceRequest().topicName());
            assertArrayEquals(original.key(), producer.lastProduceRequest().records().get(0).key());
            assertArrayEquals(original.value(), producer.lastProduceRequest().records().get(0).value());
        }
    }

    @Test
    void sendToRetryResolvesMetadataV2LinkAndUsesProduceV2Delay() throws Exception {
        try (TestableProducer producer = new TestableProducer("127.0.0.1:1", "producer-retry")) {
            producer.enqueueMetadataV2(metadataV2WithLinks("orders", "orders-retry", "orders-dlq"));
            producer.enqueueMetadata(metadataFor("orders-retry"));
            producer.enqueueProduceV2Response(new MqDecoder.DecodedResponse(
                    new MqResponseHeader(1, ErrorCode.NONE),
                    new ProduceV2Response("orders-retry", 0, 42L)));

            ProducerRecord original = new ProducerRecord(
                    "orders",
                    null,
                    "retry-key".getBytes(StandardCharsets.UTF_8),
                    "retry-value".getBytes(StandardCharsets.UTF_8));
            ProduceResult result = producer.sendToRetry("orders", original, 123_456L);

            assertEquals("orders-retry", result.topic());
            assertEquals(42L, result.offset());
            assertEquals(1, producer.metadataV2Fetches());
            assertEquals(1, producer.produceV2Attempts());
            assertEquals("orders-retry", producer.lastProduceV2Request().topicName());
            assertEquals(123_456L,
                    producer.lastProduceV2Request().records().items().get(0).deliverAtEpochMs());
            assertArrayEquals(original.key(), producer.lastProduceV2Request().records().items().get(0).key());
            assertArrayEquals(original.value(), producer.lastProduceV2Request().records().items().get(0).value());
        }
    }

    @Test
    void sendDelayedUsesProduceV2WithTheRequestedVisibilityTimestamp() throws Exception {
        try (TestableProducer producer = new TestableProducer("127.0.0.1:1", "producer-delayed")) {
            producer.enqueueMetadata(metadataFor("orders"));
            producer.enqueueProduceV2Response(new MqDecoder.DecodedResponse(
                    new MqResponseHeader(1, ErrorCode.NONE),
                    new ProduceV2Response("orders", 0, 43L)));

            ProduceResult result = producer.sendDelayed(record(), 654_321L);

            assertEquals(43L, result.offset());
            assertEquals(1, producer.produceV2Attempts());
            assertEquals(654_321L,
                    producer.lastProduceV2Request().records().items().get(0).deliverAtEpochMs());
        }
    }

    @Test
    void reliabilityHelpersRejectMissingMetadataV2LinkBeforeProducing() throws Exception {
        try (TestableProducer producer = new TestableProducer("127.0.0.1:1", "producer-no-link")) {
            producer.enqueueMetadataV2(metadataV2WithLinks("orders", null, null));

            assertThrows(IllegalArgumentException.class,
                    () -> producer.sendToRetry("orders", record(), 123L));

            assertEquals(1, producer.metadataV2Fetches());
            assertEquals(0, producer.produceAttempts());
            assertEquals(0, producer.produceV2Attempts());
        }
    }

    @Test
    void reliabilityHelpersClearSourcePartitionBeforeRoutingTarget() throws Exception {
        try (TestableProducer producer = new TestableProducer("127.0.0.1:1", "producer-reroute")) {
            producer.enqueueMetadataV2(metadataV2WithLinks("orders", "orders-retry", "orders-dlq"));
            producer.enqueueMetadata(metadataFor("orders-dlq"));
            producer.enqueueProduceResponse(new MqDecoder.DecodedResponse(
                    new MqResponseHeader(1, ErrorCode.NONE),
                    new ProduceResponse("orders-dlq", 0, 1L)));
            ProducerRecord sourcePartitionRecord = new ProducerRecord(
                    "orders", 7, null, "value".getBytes(StandardCharsets.UTF_8));

            producer.sendToDeadLetter("orders", sourcePartitionRecord);

            assertEquals(0, producer.lastProduceRequest().partitionId());
        }
    }

    @Test
    void reliabilityLinksAreCachedForThirtySecondsAndRefreshedAfterExpiry() throws Exception {
        try (TestableProducer producer = new TestableProducer("127.0.0.1:1", "producer-link-cache")) {
            producer.enqueueMetadataV2(metadataV2WithLinks("orders", "orders-retry", "orders-dlq"));
            producer.enqueueMetadataV2(metadataV2WithLinks("orders", "orders-retry", "orders-dlq"));
            producer.enqueueMetadata(metadataFor("orders-dlq"));
            producer.enqueueMetadata(metadataFor("orders-dlq"));
            for (int i = 0; i < 3; i++) {
                producer.enqueueProduceResponse(new MqDecoder.DecodedResponse(
                        new MqResponseHeader(i + 1, ErrorCode.NONE),
                        new ProduceResponse("orders-dlq", 0, i + 1L)));
            }

            producer.sendToDeadLetter("orders", record());
            producer.sendToDeadLetter("orders", record());
            assertEquals(1, producer.metadataV2Fetches());

            producer.advanceTime(Producer.DEFAULT_RELIABILITY_LINK_CACHE_MAX_AGE_MS + 1L);
            producer.sendToDeadLetter("orders", record());

            assertEquals(2, producer.metadataV2Fetches());
        }
    }

    @Test
    void reliabilityLinkExpiryUsesMonotonicTimeAcrossWallClockRollback() throws Exception {
        try (TestableProducer producer = new TestableProducer(
                "127.0.0.1:1", "producer-link-monotonic-cache")) {
            producer.enqueueMetadataV2(metadataV2WithLinks(
                    "orders", "orders-retry", "orders-dlq"));
            producer.enqueueMetadataV2(metadataV2WithLinks(
                    "orders", "orders-retry", "orders-dlq"));
            producer.enqueueMetadata(metadataFor("orders-dlq"));
            producer.enqueueProduceResponse(successResponse(1L));
            producer.enqueueProduceResponse(successResponse(2L));

            producer.sendToDeadLetter("orders", record());
            producer.advanceWallClock(-60_000L);
            producer.advanceMonotonicTime(
                    Producer.DEFAULT_RELIABILITY_LINK_CACHE_MAX_AGE_MS + 1L);
            producer.sendToDeadLetter("orders", record());

            assertEquals(2, producer.metadataV2Fetches());
        }
    }

    @Test
    void reliabilityLinkCacheEvictsLeastRecentlyUsedEntryAtItsBound() throws Exception {
        try (TestableProducer producer = new TestableProducer(
                "127.0.0.1:1", "producer-link-bounded-cache")) {
            int cacheCapacity = Producer.DEFAULT_RELIABILITY_LINK_CACHE_MAX_ENTRIES;
            String target = "reliability-target";
            producer.enqueueMetadata(metadataFor(target));
            for (int index = 0; index <= cacheCapacity; index++) {
                String source = "source-" + index;
                producer.enqueueMetadataV2(metadataV2WithLinks(source, null, target));
                producer.enqueueProduceResponse(new MqDecoder.DecodedResponse(
                        new MqResponseHeader(index + 1, ErrorCode.NONE),
                        new ProduceResponse(target, 0, index + 1L)));
                producer.sendToDeadLetter(source, record());
            }
            assertEquals(cacheCapacity + 1, producer.metadataV2Fetches());

            producer.enqueueMetadataV2(metadataV2WithLinks(
                    "source-0", null, target));
            producer.enqueueProduceResponse(new MqDecoder.DecodedResponse(
                    new MqResponseHeader(cacheCapacity + 2, ErrorCode.NONE),
                    new ProduceResponse(target, 0, cacheCapacity + 2L)));
            producer.sendToDeadLetter("source-0", record());

            assertEquals(cacheCapacity + 2, producer.metadataV2Fetches());
        }
    }

    @Test
    void staleReliabilityTargetInvalidatesLinkAndReresolvesOnce() throws Exception {
        try (TestableProducer producer = new TestableProducer("127.0.0.1:1", "producer-link-refresh")) {
            producer.enqueueMetadataV2(metadataV2WithLinks("orders", "orders-retry", "orders-dlq-old"));
            producer.enqueueMetadataV2(metadataV2WithLinks("orders", "orders-retry", "orders-dlq-new"));
            producer.enqueueMetadata(metadataFor("orders-dlq-old"));
            producer.enqueueMetadata(metadataFor("orders-dlq-old"));
            producer.enqueueMetadata(metadataFor("orders-dlq-new"));
            producer.enqueueProduceResponse(new MqDecoder.DecodedResponse(
                    new MqResponseHeader(1, ErrorCode.UNKNOWN_TOPIC), null));
            producer.enqueueProduceResponse(new MqDecoder.DecodedResponse(
                    new MqResponseHeader(2, ErrorCode.UNKNOWN_TOPIC), null));
            producer.enqueueProduceResponse(new MqDecoder.DecodedResponse(
                    new MqResponseHeader(3, ErrorCode.NONE),
                    new ProduceResponse("orders-dlq-new", 0, 3L)));

            ProduceResult result = producer.sendToDeadLetter("orders", record());

            assertEquals("orders-dlq-new", result.topic());
            assertEquals(2, producer.metadataV2Fetches());
            assertEquals(3, producer.produceAttempts());
            assertEquals("orders-dlq-new", producer.lastProduceRequest().topicName());
        }
    }

    @Test
    void reliabilityConnectionFailureRefreshesRoutingBeforeOneSafeRetry() throws Exception {
        try (TestableProducer producer = new TestableProducer(
                "127.0.0.1:1", "producer-link-connection-failure")) {
            producer.enqueueMetadataV2(metadataV2WithLinks(
                    "orders", "orders-retry", "orders-dlq"));
            producer.enqueueMetadataV2(metadataV2WithLinks(
                    "orders", "orders-retry", "orders-dlq"));
            producer.enqueueMetadata(metadataFor("orders-dlq"));
            producer.enqueueMetadata(metadataFor("orders-dlq"));
            producer.enqueueProduceFailure(new MiniMqConnectionException(
                    "client is not connected", null));
            producer.enqueueProduceResponse(new MqDecoder.DecodedResponse(
                    new MqResponseHeader(2, ErrorCode.NONE),
                    new ProduceResponse("orders-dlq", 0, 2L)));

            ProduceResult result = producer.sendToDeadLetter("orders", record());

            assertEquals(2L, result.offset());
            assertEquals(2, producer.metadataV2Fetches());
            assertEquals(2, producer.metadataFetches());
            assertEquals(2, producer.produceAttempts());
        }
    }

    @Test
    void reliabilityConnectionFailureDiscardsCachedBrokerClientBeforeRetry() throws Exception {
        StubBrokerClient staleClient = new StubBrokerClient(
                "stale-broker-client",
                new MiniMqConnectionException("client is not connected", null));
        StubBrokerClient replacementClient = new StubBrokerClient(
                "replacement-broker-client",
                new MqDecoder.DecodedResponse(
                        new MqResponseHeader(2, ErrorCode.NONE),
                        new ProduceResponse("orders-dlq", 0, 2L)));
        Queue<NetworkClient> brokerClients = new ArrayDeque<>(
                List.of(staleClient, replacementClient));

        try (BrokerClientBackedProducer producer = new BrokerClientBackedProducer(brokerClients)) {
            producer.enqueueMetadataV2(metadataV2WithLinks(
                    "orders", "orders-retry", "orders-dlq"));
            producer.enqueueMetadataV2(metadataV2WithLinks(
                    "orders", "orders-retry", "orders-dlq"));
            producer.enqueueMetadata(metadataFor("orders-dlq"));
            producer.enqueueMetadata(metadataFor("orders-dlq"));

            ProduceResult result = producer.sendToDeadLetter("orders", record());

            assertEquals(2L, result.offset());
            assertEquals(0, brokerClients.size());
            assertTrue(staleClient.closed());
        }
    }

    @Test
    void reliabilityScheduledConnectionFailureDiscardsCachedBrokerClientBeforeRetry()
            throws Exception {
        StubBrokerClient staleClient = new StubBrokerClient(
                "stale-scheduled-broker-client",
                new MiniMqConnectionException("client is not connected", null));
        StubBrokerClient replacementClient = new StubBrokerClient(
                "replacement-scheduled-broker-client",
                new MqDecoder.DecodedResponse(
                        new MqResponseHeader(2, ErrorCode.NONE),
                        new ProduceV2Response("orders-retry", 0, 2L)));
        Queue<NetworkClient> brokerClients = new ArrayDeque<>(
                List.of(staleClient, replacementClient));

        try (BrokerClientBackedProducer producer = new BrokerClientBackedProducer(brokerClients)) {
            producer.enqueueMetadataV2(metadataV2WithLinks(
                    "orders", "orders-retry", "orders-dlq"));
            producer.enqueueMetadataV2(metadataV2WithLinks(
                    "orders", "orders-retry", "orders-dlq"));
            producer.enqueueMetadata(metadataFor("orders-retry"));
            producer.enqueueMetadata(metadataFor("orders-retry"));

            ProduceResult result = producer.sendToRetry("orders", record(), 123_456L);

            assertEquals(2L, result.offset());
            assertEquals(0, brokerClients.size());
            assertTrue(staleClient.closed());
        }
    }

    @Test
    void reliabilityTimeoutInvalidatesRoutingWithoutReplayingTheRequest() throws Exception {
        try (TestableProducer producer = new TestableProducer(
                "127.0.0.1:1", "producer-link-timeout")) {
            producer.enqueueMetadataV2(metadataV2WithLinks(
                    "orders", "orders-retry", "orders-dlq"));
            producer.enqueueMetadataV2(metadataV2WithLinks(
                    "orders", "orders-retry", "orders-dlq"));
            producer.enqueueMetadata(metadataFor("orders-dlq"));
            producer.enqueueMetadata(metadataFor("orders-dlq"));
            producer.enqueueProduceFailure(new MiniMqRequestTimeoutException(
                    "PRODUCE", Duration.ofMillis(75)));
            producer.enqueueProduceResponse(new MqDecoder.DecodedResponse(
                    new MqResponseHeader(2, ErrorCode.NONE),
                    new ProduceResponse("orders-dlq", 0, 2L)));

            assertThrows(MiniMqRequestTimeoutException.class,
                    () -> producer.sendToDeadLetter("orders", record()));
            assertEquals(1, producer.produceAttempts());

            ProduceResult result = producer.sendToDeadLetter("orders", record());

            assertEquals(2L, result.offset());
            assertEquals(2, producer.metadataV2Fetches());
            assertEquals(2, producer.metadataFetches());
            assertEquals(2, producer.produceAttempts());
        }
    }

    private static ProducerRecord record() {
        return new ProducerRecord(
                "orders",
                null,
                "k".getBytes(StandardCharsets.UTF_8),
                "v".getBytes(StandardCharsets.UTF_8));
    }

    private static ProduceResult send(TestableProducer producer) throws InterruptedException {
        return producer.send(record());
    }

    private static MetadataResponse metadataFor(String topicName) {
        return new MetadataResponse(
                List.of(new BrokerMetadata(0, "127.0.0.1", 1)),
                List.of(new TopicMetadata(topicName, List.of(new PartitionMetadata(0, 0)))),
                null,
                1L);
    }

    private static MetadataV2Response metadataV2WithLinks(
            String sourceTopic, String retryTopic, String deadLetterTopic) {
        return new MetadataV2Response(
                new V4BoundedList<>(0, false, List.of()),
                new V4BoundedList<>(
                        1,
                        false,
                        List.of(new TopicSummary(sourceTopic, 1, 1, retryTopic, deadLetterTopic))),
                new V4BoundedList<>(0, false, List.of()),
                null,
                1L,
                4);
    }

    private static MqDecoder.DecodedResponse successResponse(long baseOffset) {
        return new MqDecoder.DecodedResponse(
                new MqResponseHeader((int) baseOffset, ErrorCode.NONE),
                new ProduceResponse("orders", 0, baseOffset));
    }

    private static final class BrokerClientBackedProducer extends Producer {
        private final Queue<MetadataResponse> metadataResponses = new ArrayDeque<>();
        private final Queue<MetadataV2Response> metadataV2Responses = new ArrayDeque<>();

        private BrokerClientBackedProducer(Queue<NetworkClient> brokerClients) {
            super("127.0.0.1:1", "producer-real-broker-cache", ignored -> brokerClients.remove(),
                    new ProducerConfig(50, 0L));
        }

        private void enqueueMetadata(MetadataResponse metadata) {
            metadataResponses.add(metadata);
        }

        private void enqueueMetadataV2(MetadataV2Response metadata) {
            metadataV2Responses.add(metadata);
        }

        @Override
        protected MetadataResponse fetchMetadata(String topicName) {
            return metadataResponses.remove();
        }

        @Override
        protected MetadataV2Response fetchMetadataV2(String topicName) {
            return metadataV2Responses.remove();
        }
    }

    private static final class StubBrokerClient extends NetworkClient {
        private final Object sendOutcome;
        private boolean closed;

        private StubBrokerClient(String clientId, Object sendOutcome) {
            super(clientId);
            this.sendOutcome = sendOutcome;
        }

        @Override
        public void connect(String host, int port) {
        }

        @Override
        public MqDecoder.DecodedResponse send(short apiKey, Object body) {
            if (sendOutcome instanceof RuntimeException failure) {
                throw failure;
            }
            return (MqDecoder.DecodedResponse) sendOutcome;
        }

        @Override
        public void close() {
            closed = true;
        }

        private boolean closed() {
            return closed;
        }
    }

    private static final class TestableProducer extends Producer {
        private final Queue<MetadataResponse> metadataResponses = new ArrayDeque<>();
        private final Queue<MetadataV2Response> metadataV2Responses = new ArrayDeque<>();
        private final Queue<Object> produceResponses = new ArrayDeque<>();
        private final Queue<MqDecoder.DecodedResponse> produceV2Responses = new ArrayDeque<>();
        private int metadataFetches;
        private int metadataV2Fetches;
        private int produceAttempts;
        private int produceV2Attempts;
        private int invalidatedBrokerClients;
        private ProduceRequest lastProduceRequest;
        private ProduceV2Request lastProduceV2Request;
        private long currentTimeMillis = 1_000_000L;
        private long currentTimeNanos = TimeUnit.SECONDS.toNanos(1L);

        private TestableProducer(String bootstrapServers, String clientId) {
            super(bootstrapServers, clientId, NetworkClient::new, new ProducerConfig(50, 0L));
        }

        private void enqueueMetadata(MetadataResponse metadata) {
            metadataResponses.add(metadata);
        }

        private void enqueueMetadataV2(MetadataV2Response metadata) {
            metadataV2Responses.add(metadata);
        }

        private void enqueueProduceResponse(MqDecoder.DecodedResponse response) {
            produceResponses.add(response);
        }

        private void enqueueProduceFailure(RuntimeException failure) {
            produceResponses.add(failure);
        }

        private void enqueueProduceV2Response(MqDecoder.DecodedResponse response) {
            produceV2Responses.add(response);
        }

        private int metadataFetches() {
            return metadataFetches;
        }

        private int metadataV2Fetches() {
            return metadataV2Fetches;
        }

        private int produceAttempts() {
            return produceAttempts;
        }

        private int produceV2Attempts() {
            return produceV2Attempts;
        }

        private int invalidatedBrokerClients() {
            return invalidatedBrokerClients;
        }

        private ProduceRequest lastProduceRequest() {
            return lastProduceRequest;
        }

        private ProduceV2Request lastProduceV2Request() {
            return lastProduceV2Request;
        }

        private void advanceTime(long deltaMs) {
            currentTimeMillis += deltaMs;
            currentTimeNanos += TimeUnit.MILLISECONDS.toNanos(deltaMs);
        }

        private void advanceWallClock(long deltaMs) {
            currentTimeMillis += deltaMs;
        }

        private void advanceMonotonicTime(long deltaMs) {
            currentTimeNanos += TimeUnit.MILLISECONDS.toNanos(deltaMs);
        }

        @Override
        protected MetadataResponse fetchMetadata(String topicName) throws InterruptedException {
            metadataFetches++;
            if (!metadataResponses.isEmpty()) {
                return metadataResponses.remove();
            }
            return super.fetchMetadata(topicName);
        }

        @Override
        protected MetadataV2Response fetchMetadataV2(String topicName) throws InterruptedException {
            metadataV2Fetches++;
            if (!metadataV2Responses.isEmpty()) {
                return metadataV2Responses.remove();
            }
            return super.fetchMetadataV2(topicName);
        }

        @Override
        protected MqDecoder.DecodedResponse sendProduce(
                BrokerMetadata leader,
                ProduceRequest request) {
            produceAttempts++;
            lastProduceRequest = request;
            Object outcome = produceResponses.remove();
            if (outcome instanceof RuntimeException failure) {
                throw failure;
            }
            return (MqDecoder.DecodedResponse) outcome;
        }

        @Override
        protected MqDecoder.DecodedResponse sendProduceV2(
                BrokerMetadata leader,
                ProduceV2Request request) {
            produceV2Attempts++;
            lastProduceV2Request = request;
            return produceV2Responses.remove();
        }

        @Override
        protected void invalidateBrokerClient(int brokerId) {
            invalidatedBrokerClients++;
        }

        @Override
        protected long currentTimeMillis() {
            return currentTimeMillis;
        }

        @Override
        protected long currentTimeNanos() {
            return currentTimeNanos;
        }
    }
}
