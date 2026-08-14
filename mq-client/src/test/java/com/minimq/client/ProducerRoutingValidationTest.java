package com.minimq.client;

import com.minimq.client.exception.MiniMqMetadataException;
import com.minimq.client.model.ProduceResult;
import com.minimq.client.model.ProducerRecord;
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
    void sendBatchRejectsEmptyAndNullRecords() throws Exception {
        try (MetadataOnlyProducer producer = new MetadataOnlyProducer()) {
            assertThrows(IllegalArgumentException.class, () -> producer.send(List.of()));
            assertThrows(IllegalArgumentException.class,
                    () -> producer.send(java.util.Arrays.asList(record(0), null)));
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
        private int metadataFetches;
        private int produceRequests;
        private ProduceRequest lastRequest;

        private MetadataOnlyProducer() {
            super("127.0.0.1:19092", "producer-routing-test");
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

        private int produceRequests() {
            return produceRequests;
        }

        private ProduceRequest lastRequest() {
            return lastRequest;
        }

        @Override
        protected MetadataResponse fetchMetadata(String topicName) {
            metadataFetches++;
            return metadataResponses.remove();
        }

        @Override
        protected MqDecoder.DecodedResponse sendProduce(
                BrokerMetadata leader,
                ProduceRequest request) {
            produceRequests++;
            lastRequest = request;
            return produceResponses.remove();
        }
    }
}
