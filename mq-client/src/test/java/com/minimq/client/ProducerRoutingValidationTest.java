package com.minimq.client;

import com.minimq.client.exception.MiniMqConnectionException;
import com.minimq.client.exception.MiniMqMetadataException;
import com.minimq.client.model.ProduceResult;
import com.minimq.client.model.ProducerRecord;
import com.minimq.common.acks.ProduceAcks;
import com.minimq.common.error.ErrorCode;
import com.minimq.protocol.codec.MqDecoder;
import com.minimq.protocol.message.MqResponseHeader;
import com.minimq.protocol.message.ProtocolMessages.BrokerMetadata;
import com.minimq.protocol.message.ProtocolMessages.MetadataResponse;
import com.minimq.protocol.message.ProtocolMessages.PartitionMetadata;
import com.minimq.protocol.message.ProtocolMessages.ProduceRequest;
import com.minimq.protocol.message.ProtocolMessages.ProduceResponse;
import com.minimq.protocol.message.ProtocolMessages.TopicMetadata;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ProducerRoutingValidationTest {

    @Test
    void sendRefreshesMetadataThenReportsMissingTopic() throws Exception {
        assertRouteFailure(new MetadataResponse(List.of(), List.of()), record(0), "Topic not found: orders");
    }

    @Test
    void sendRefreshesMetadataThenReportsTopicWithoutPartitions() throws Exception {
        assertRouteFailure(metadata(List.of(), List.of()), record(0), "Topic has no partitions: orders");
    }

    @Test
    void sendRefreshesMetadataThenReportsMissingPartition() throws Exception {
        assertRouteFailure(metadata(
                        List.of(new BrokerMetadata(0, "127.0.0.1", 19092)),
                        List.of(new PartitionMetadata(0, 0))),
                record(1),
                "Partition 1 not found for topic orders");
    }

    @Test
    void sendRefreshesMetadataThenReportsMissingLeader() throws Exception {
        assertRouteFailure(metadata(
                        List.of(),
                        List.of(new PartitionMetadata(0, 7))),
                record(0),
                "Leader broker 7 not found");
    }

    @Test
    void sendRoutesRecordWithoutKeyToPartitionZero() throws Exception {
        try (MetadataOnlyProducer producer = new MetadataOnlyProducer()) {
            producer.enqueueMetadata(metadata(
                    List.of(new BrokerMetadata(0, "127.0.0.1", 19092)),
                    List.of(new PartitionMetadata(0, 0))));
            producer.enqueueProduceResponse(successResponse(9));

            ProduceResult result = producer.send(new ProducerRecord(
                    "orders", null, null, "value".getBytes(StandardCharsets.UTF_8)));

            assertEquals(9, result.offset());
            assertEquals(0, producer.lastRequest().partitionId());
        }
    }

    @Test
    void sendRejectsNegativeExplicitPartition() throws Exception {
        try (MetadataOnlyProducer producer = new MetadataOnlyProducer()) {
            producer.enqueueMetadata(metadata(
                    List.of(new BrokerMetadata(0, "127.0.0.1", 19092)),
                    List.of(new PartitionMetadata(0, 0))));

            IllegalArgumentException ex = assertThrows(
                    IllegalArgumentException.class,
                    () -> producer.send(record(-1)));

            assertEquals("Partition id must be non-negative", ex.getMessage());
            assertEquals(1, producer.metadataFetches());
        }
    }

    @Test
    void reliabilityAndDelayedHelpersRejectNegativeDeliveryTimes() throws Exception {
        try (MetadataOnlyProducer producer = new MetadataOnlyProducer()) {
            assertThrows(IllegalArgumentException.class,
                    () -> producer.sendToRetry("orders", record(0), -1L));
            assertThrows(IllegalArgumentException.class,
                    () -> producer.sendDelayed(record(0), -1L));
        }
    }

    @Test
    void reliabilityHelpersRejectBlankSourceTopicsBeforeMetadataLookup() throws Exception {
        try (MetadataOnlyProducer producer = new MetadataOnlyProducer()) {
            assertThrows(IllegalArgumentException.class,
                    () -> producer.sendToDeadLetter(" ", record(0)));
        }
    }

    @Test
    void sendBatchUsesOneRequestAndAssignsContiguousOffsets() throws Exception {
        try (MetadataOnlyProducer producer = new MetadataOnlyProducer()) {
            producer.enqueueMetadata(metadata(
                    List.of(new BrokerMetadata(0, "127.0.0.1", 19092)),
                    List.of(new PartitionMetadata(0, 0))));
            producer.enqueueProduceResponse(successResponse(4));

            List<ProduceResult> results = producer.send(List.of(record(0), record(0), record(0)));

            assertEquals(List.of(4L, 5L, 6L), results.stream().map(ProduceResult::offset).toList());
            assertEquals(1, producer.produceRequests());
            assertEquals(3, producer.lastRequest().records().size());
        }
    }

    @Test
    void sequentialSendsShareOneProduceRequestUntilOffsetIsRead() throws Exception {
        try (MetadataOnlyProducer producer = new MetadataOnlyProducer(new ProducerConfig(50, 60_000))) {
            producer.enqueueMetadata(metadata(
                    List.of(new BrokerMetadata(0, "127.0.0.1", 19092)),
                    List.of(new PartitionMetadata(0, 0))));
            producer.enqueueProduceResponse(successResponse(4));

            ProduceResult first = producer.send(record(0));
            ProduceResult second = producer.send(record(0));
            ProduceResult third = producer.send(record(0));

            assertEquals(0, producer.produceRequests());
            assertEquals(List.of(4L, 5L, 6L), List.of(first.offset(), second.offset(), third.offset()));
            assertEquals(1, producer.produceRequests());
            assertEquals(3, producer.lastRequest().records().size());
        }
    }

    @Test
    void batchSizeDispatchesWithoutReadingOffset() throws Exception {
        try (MetadataOnlyProducer producer = new MetadataOnlyProducer(new ProducerConfig(2, 60_000))) {
            producer.enqueueMetadata(metadata(
                    List.of(new BrokerMetadata(0, "127.0.0.1", 19092)),
                    List.of(new PartitionMetadata(0, 0))));
            producer.enqueueProduceResponse(successResponse(10));

            ProduceResult first = producer.send(record(0));
            ProduceResult second = producer.send(record(0));

            assertEquals(1, producer.produceRequests());
            assertEquals(2, producer.lastRequest().records().size());
            assertEquals(List.of(10L, 11L), List.of(first.offset(), second.offset()));
        }
    }

    @Test
    void lingerFlushesPendingBatch() throws Exception {
        try (MetadataOnlyProducer producer = new MetadataOnlyProducer(new ProducerConfig(50, 15))) {
            producer.enqueueMetadata(metadata(
                    List.of(new BrokerMetadata(0, "127.0.0.1", 19092)),
                    List.of(new PartitionMetadata(0, 0))));
            producer.enqueueProduceResponse(successResponse(9));

            ProduceResult result = producer.send(record(0));
            long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(2);
            while (producer.produceRequests() == 0 && System.nanoTime() < deadline) {
                Thread.sleep(10);
            }

            assertEquals(1, producer.produceRequests());
            assertEquals(9, result.offset());
        }
    }

    @Test
    void sendListFlushesPendingRecordsFirst() throws Exception {
        try (MetadataOnlyProducer producer = new MetadataOnlyProducer(new ProducerConfig(50, 60_000))) {
            producer.enqueueMetadata(metadata(
                    List.of(new BrokerMetadata(0, "127.0.0.1", 19092)),
                    List.of(new PartitionMetadata(0, 0))));
            producer.enqueueProduceResponse(successResponse(1));
            producer.enqueueProduceResponse(successResponse(10));

            ProduceResult pending = producer.send(record(0));
            List<ProduceResult> batched = producer.send(List.of(record(0), record(0)));

            assertEquals(1L, pending.offset());
            assertEquals(List.of(10L, 11L), batched.stream().map(ProduceResult::offset).toList());
            assertEquals(2, producer.produceRequests());
        }
    }

    @Test
    void differentAcksFlushThePreviousBatch() throws Exception {
        try (MetadataOnlyProducer producer = new MetadataOnlyProducer(new ProducerConfig(50, 60_000))) {
            producer.enqueueMetadata(metadata(
                    List.of(new BrokerMetadata(0, "127.0.0.1", 19092)),
                    List.of(new PartitionMetadata(0, 0))));
            producer.enqueueProduceResponse(successResponse(1));
            producer.enqueueProduceResponse(successResponse(8));

            ProduceResult leader = producer.send(record(0), ProduceAcks.LEADER);
            ProduceResult all = producer.send(record(0), ProduceAcks.ALL);

            assertEquals(1, producer.produceRequests());
            assertEquals(1L, leader.offset());
            assertEquals(8L, all.offset());
            assertEquals(2, producer.produceRequests());
        }
    }

    @Test
    void closedProducerRejectsSend() throws Exception {
        MetadataOnlyProducer producer = new MetadataOnlyProducer();
        producer.close();
        IllegalStateException ex = assertThrows(IllegalStateException.class, () -> producer.send(record(0)));
        assertEquals("Producer is closed", ex.getMessage());
    }

    @Test
    void pendingOffsetSurfacesProduceFailure() throws Exception {
        try (MetadataOnlyProducer producer = new MetadataOnlyProducer(new ProducerConfig(50, 60_000))) {
            producer.enqueueMetadata(metadata(
                    List.of(new BrokerMetadata(0, "127.0.0.1", 19092)),
                    List.of(new PartitionMetadata(0, 0))));
            producer.enqueueMetadata(metadata(
                    List.of(new BrokerMetadata(0, "127.0.0.1", 19092)),
                    List.of(new PartitionMetadata(0, 0))));
            producer.enqueueProduceFailure(new MiniMqConnectionException("disconnected", null));
            producer.enqueueProduceFailure(new MiniMqConnectionException("disconnected", null));

            ProduceResult result = producer.send(record(0));
            MiniMqConnectionException ex = assertThrows(
                    MiniMqConnectionException.class,
                    result::offset);
            assertEquals("disconnected", ex.getMessage());
        }
    }

    @Test
    void sendBatchRejectsEmptyAndNullRecords() throws Exception {
        try (MetadataOnlyProducer producer = new MetadataOnlyProducer()) {
            assertThrows(IllegalArgumentException.class, () -> producer.send(List.of()));
            assertThrows(IllegalArgumentException.class,
                    () -> producer.send(java.util.Arrays.asList(record(0), null)));
        }
    }

    @Test
    void sendAcksOverloadsDelegateToTimedSend() throws Exception {
        try (MetadataOnlyProducer producer = new MetadataOnlyProducer()) {
            producer.enqueueMetadata(metadata(
                    List.of(new BrokerMetadata(0, "127.0.0.1", 19092)),
                    List.of(new PartitionMetadata(0, 0), new PartitionMetadata(1, 0))));
            producer.enqueueProduceResponse(successResponse(1));
            producer.enqueueProduceResponse(successResponse(10));
            producer.enqueueProduceResponse(successResponse(20));

            assertEquals(1, producer.send(record(0), ProduceAcks.LEADER).offset());
            List<ProduceResult> batched = producer.send(
                    List.of(record(0), record(1)), ProduceAcks.LEADER);
            assertEquals(List.of(10L, 20L), batched.stream().map(ProduceResult::offset).toList());
            assertEquals(3, producer.produceRequests());
        }
    }

    @Test
    void sendBatchRetriesOnceAfterConnectionFailure() throws Exception {
        try (MetadataOnlyProducer producer = new MetadataOnlyProducer()) {
            MetadataResponse topicMetadata = metadata(
                    List.of(new BrokerMetadata(0, "127.0.0.1", 19092)),
                    List.of(new PartitionMetadata(0, 0)));
            producer.enqueueMetadata(topicMetadata);
            producer.enqueueProduceFailure(new MiniMqConnectionException("disconnected", null));
            producer.enqueueMetadata(topicMetadata);
            producer.enqueueProduceResponse(successResponse(8));

            List<ProduceResult> results = producer.send(List.of(record(0), record(0)));

            assertEquals(List.of(8L, 9L), results.stream().map(ProduceResult::offset).toList());
            assertEquals(2, producer.produceRequests());
        }
    }

    @Test
    void sendBatchRetriesOnceAfterStaleLeader() throws Exception {
        try (MetadataOnlyProducer producer = new MetadataOnlyProducer()) {
            MetadataResponse topicMetadata = metadata(
                    List.of(new BrokerMetadata(0, "127.0.0.1", 19092)),
                    List.of(new PartitionMetadata(0, 0)));
            producer.enqueueMetadata(topicMetadata);
            producer.enqueueProduceResponse(new MqDecoder.DecodedResponse(
                    new MqResponseHeader(2, ErrorCode.NOT_LEADER), null));
            producer.enqueueMetadata(topicMetadata);
            producer.enqueueProduceResponse(successResponse(8));

            List<ProduceResult> results = producer.send(List.of(record(0), record(0)));

            assertEquals(List.of(8L, 9L), results.stream().map(ProduceResult::offset).toList());
            assertEquals(2, producer.produceRequests());
        }
    }

    private static void assertRouteFailure(
            MetadataResponse metadata,
            ProducerRecord record,
            String expectedMessage) throws Exception {
        try (MetadataOnlyProducer producer = new MetadataOnlyProducer()) {
            producer.enqueueMetadata(metadata);
            producer.enqueueMetadata(metadata);

            MiniMqMetadataException ex = assertThrows(
                    MiniMqMetadataException.class,
                    () -> producer.send(record));

            assertEquals(expectedMessage, ex.getMessage());
            assertEquals(2, producer.metadataFetches());
        }
    }

    private static MetadataResponse metadata(
            List<BrokerMetadata> brokers,
            List<PartitionMetadata> partitions) {
        return new MetadataResponse(
                brokers,
                List.of(new TopicMetadata("orders", partitions)));
    }

    private static ProducerRecord record(int partitionId) {
        return new ProducerRecord(
                "orders",
                partitionId,
                "key".getBytes(StandardCharsets.UTF_8),
                "value".getBytes(StandardCharsets.UTF_8));
    }

    private static MqDecoder.DecodedResponse successResponse(long offset) {
        return new MqDecoder.DecodedResponse(
                new MqResponseHeader((int) offset, ErrorCode.NONE),
                new ProduceResponse("orders", 0, offset));
    }

    private static final class MetadataOnlyProducer extends Producer {
        private final Queue<MetadataResponse> metadataResponses = new ArrayDeque<>();
        private final Queue<MqDecoder.DecodedResponse> produceResponses = new ArrayDeque<>();
        private final Queue<RuntimeException> produceFailures = new ArrayDeque<>();
        private int metadataFetches;
        private int produceRequests;
        private ProduceRequest lastRequest;

        private MetadataOnlyProducer() {
            this(new ProducerConfig(ProducerConfig.DEFAULT_BATCH_SIZE, 0L));
        }

        private MetadataOnlyProducer(ProducerConfig producerConfig) {
            super("127.0.0.1:19092", "producer-routing-test", NetworkClient::new, producerConfig);
        }

        private void enqueueMetadata(MetadataResponse metadata) {
            metadataResponses.add(metadata);
        }

        private void enqueueProduceResponse(MqDecoder.DecodedResponse response) {
            produceResponses.add(response);
        }

        private void enqueueProduceFailure(RuntimeException failure) {
            produceFailures.add(failure);
        }

        private int metadataFetches() {
            return metadataFetches;
        }

        @Override
        protected synchronized MetadataResponse fetchMetadata(String topicName) {
            metadataFetches++;
            return metadataResponses.remove();
        }

        @Override
        protected synchronized MqDecoder.DecodedResponse sendProduce(
                BrokerMetadata leader,
                ProduceRequest request) {
            produceRequests++;
            lastRequest = request;
            if (!produceFailures.isEmpty()) {
                throw produceFailures.remove();
            }
            return produceResponses.remove();
        }

        private synchronized int produceRequests() {
            return produceRequests;
        }

        private synchronized ProduceRequest lastRequest() {
            return lastRequest;
        }
    }
}
