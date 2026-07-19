package com.minimq.client;

import com.minimq.broker.support.BrokerTestHarness;
import com.minimq.client.exception.MiniMqServerException;
import com.minimq.client.model.ProduceResult;
import com.minimq.client.model.ProducerRecord;
import com.minimq.common.error.ErrorCode;
import com.minimq.protocol.ApiKeys;
import com.minimq.protocol.codec.MqDecoder;
import com.minimq.protocol.message.MqResponseHeader;
import com.minimq.protocol.message.ProtocolMessages.CreateTopicRequest;
import com.minimq.protocol.message.ProtocolMessages.BrokerMetadata;
import com.minimq.protocol.message.ProtocolMessages.MetadataRequest;
import com.minimq.protocol.message.ProtocolMessages.MetadataResponse;
import com.minimq.protocol.message.ProtocolMessages.ProduceRequest;
import com.minimq.protocol.message.ProtocolMessages.ProduceResponse;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.Queue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ProducerNotLeaderRetryTest {

    @Test
    void sendRefreshesMetadataAndRetriesOnceAfterNotLeader() throws Exception {
        Path dataDir = BrokerTestHarness.newDataDir("producer-not-leader");
        try (BrokerTestHarness controller = BrokerTestHarness.startController(dataDir);
             NetworkClient admin = new NetworkClient("admin");
             TestableProducer producer = new TestableProducer(controller.bootstrapServers(), "producer-v13")) {
            admin.connect("127.0.0.1", controller.port());
            admin.ensureSuccess(admin.send(
                    ApiKeys.CREATE_TOPIC, new CreateTopicRequest("orders", 1)));

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
                    ApiKeys.CREATE_TOPIC, new CreateTopicRequest("orders", 1)));

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
                    ApiKeys.CREATE_TOPIC, new CreateTopicRequest("orders", 1)));

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
                    ApiKeys.CREATE_TOPIC, new CreateTopicRequest("orders", 1)));

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
                    ApiKeys.CREATE_TOPIC, new CreateTopicRequest("orders", 1)));

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

    private static MqDecoder.DecodedResponse successResponse(long baseOffset) {
        return new MqDecoder.DecodedResponse(
                new MqResponseHeader((int) baseOffset, ErrorCode.NONE),
                new ProduceResponse("orders", 0, baseOffset));
    }

    private static final class TestableProducer extends Producer {
        private final Queue<MetadataResponse> metadataResponses = new ArrayDeque<>();
        private final Queue<MqDecoder.DecodedResponse> produceResponses = new ArrayDeque<>();
        private int metadataFetches;
        private int produceAttempts;
        private int invalidatedBrokerClients;

        private TestableProducer(String bootstrapServers, String clientId) {
            super(bootstrapServers, clientId);
        }

        private void enqueueMetadata(MetadataResponse metadata) {
            metadataResponses.add(metadata);
        }

        private void enqueueProduceResponse(MqDecoder.DecodedResponse response) {
            produceResponses.add(response);
        }

        private int metadataFetches() {
            return metadataFetches;
        }

        private int produceAttempts() {
            return produceAttempts;
        }

        private int invalidatedBrokerClients() {
            return invalidatedBrokerClients;
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
        protected MqDecoder.DecodedResponse sendProduce(
                BrokerMetadata leader,
                ProduceRequest request) {
            produceAttempts++;
            return produceResponses.remove();
        }

        @Override
        protected void invalidateBrokerClient(int brokerId) {
            invalidatedBrokerClients++;
        }
    }
}
