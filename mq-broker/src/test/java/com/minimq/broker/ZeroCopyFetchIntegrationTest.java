package com.minimq.broker;

import com.minimq.broker.support.BrokerTestHarness;
import com.minimq.broker.support.TestNetworkClient;
import com.minimq.common.error.ErrorCode;
import com.minimq.protocol.ApiKeys;
import com.minimq.protocol.codec.MqDecoder;
import com.minimq.protocol.message.ProtocolMessages.CreateTopicRequest;
import com.minimq.protocol.message.ProtocolMessages.FetchRequest;
import com.minimq.protocol.message.ProtocolMessages.FetchResponse;
import com.minimq.protocol.message.ProtocolMessages.FetchedRecord;
import com.minimq.protocol.message.ProtocolMessages.ProduceRecord;
import com.minimq.protocol.message.ProtocolMessages.ProduceRequest;
import com.minimq.protocol.message.ProtocolMessages.ProduceV2Request;
import com.minimq.protocol.message.ProtocolMessages.ScheduledProduceRecord;
import com.minimq.protocol.message.ProtocolMessages.V4BoundedList;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ZeroCopyFetchIntegrationTest {

    @Test
    void autoSendfileFetchOverRealTcp(@TempDir Path dataDir) throws Exception {
        BrokerConfig config = BrokerTestHarness.controllerConfig(0, dataDir,
                "--segment-max-bytes", "64",
                "--sparse-index-interval-records", "1",
                "--fetch-transfer-mode", "auto",
                "--fetch-transfer-timeout-ms", "5000");
        List<ProducedRecord> produced = produceRecords(12);
        try (BrokerTestHarness broker = BrokerTestHarness.start(config);
             TestNetworkClient client = new TestNetworkClient()) {
            client.connect("127.0.0.1", broker.port());
            createTopic(client, "orders", 1);
            for (ProducedRecord record : produced) {
                produce(client, record);
            }

            MqDecoder.DecodedResponse response = client.send(
                    ApiKeys.FETCH, new FetchRequest("orders", 0, 1, 4_096));
            assertEquals(ErrorCode.NONE, response.header().errorCode());
            FetchResponse fetch = (FetchResponse) response.body();

            List<ProducedRecord> expected = produced.subList(1, produced.size());
            assertEquals(expected.size(), fetch.records().size());
            for (int i = 0; i < expected.size(); i++) {
                FetchedRecord actual = fetch.records().get(i);
                assertEquals(expected.get(i).offset(), actual.offset());
                assertArrayEquals(expected.get(i).key(), actual.key());
                assertArrayEquals(expected.get(i).value(), actual.value());
            }

            FetchTransferMetricsSnapshot metrics = broker.fetchTransferMetrics();
            assertEquals(1, metrics.sendfileResponses(), metrics.toString());
            assertTrue(metrics.sendfileBytes() > 0, metrics.toString());
            assertEquals(0, metrics.heapResponses(), metrics.toString());
        }
    }

    @Test
    void heapFetchMatchesAutoPayload(@TempDir Path autoDir, @TempDir Path heapDir) throws Exception {
        List<ProducedRecord> produced = produceRecords(12);
        FetchResponse autoFetch = runFetch(autoDir, FetchTransferMode.AUTO, produced);
        FetchResponse heapFetch = runFetch(heapDir, FetchTransferMode.HEAP, produced);

        assertEquals(autoFetch.records().size(), heapFetch.records().size());
        for (int i = 0; i < autoFetch.records().size(); i++) {
            FetchedRecord auto = autoFetch.records().get(i);
            FetchedRecord heap = heapFetch.records().get(i);
            assertEquals(auto.offset(), heap.offset());
            assertArrayEquals(auto.key(), heap.key());
            assertArrayEquals(auto.value(), heap.value());
        }
    }

    @Test
    void autoSendfileFetchPublishesVisibleHighWatermarkForDelayedRecords(@TempDir Path dataDir)
            throws Exception {
        BrokerConfig config = BrokerTestHarness.controllerConfig(0, dataDir,
                "--segment-max-bytes", "64",
                "--sparse-index-interval-records", "1",
                "--fetch-transfer-mode", "auto",
                "--fetch-transfer-timeout-ms", "5000");
        try (BrokerTestHarness broker = BrokerTestHarness.start(config);
             TestNetworkClient client = new TestNetworkClient()) {
            client.connect("127.0.0.1", broker.port());
            createTopic(client, "orders", 1);

            long deliverAtEpochMs = System.currentTimeMillis() + 60_000L;
            MqDecoder.DecodedResponse produced = client.send(
                    ApiKeys.PRODUCE_V2,
                    new ProduceV2Request(
                            "orders",
                            0,
                            0L,
                            new V4BoundedList<>(
                                    2,
                                    false,
                                    List.of(
                                            new ScheduledProduceRecord(
                                                    "ready".getBytes(StandardCharsets.UTF_8),
                                                    "now".getBytes(StandardCharsets.UTF_8),
                                                    0L),
                                            new ScheduledProduceRecord(
                                                    "later".getBytes(StandardCharsets.UTF_8),
                                                    "wait".getBytes(StandardCharsets.UTF_8),
                                                    deliverAtEpochMs)))));
            assertEquals(ErrorCode.NONE, produced.header().errorCode());

            MqDecoder.DecodedResponse response = client.send(
                    ApiKeys.FETCH, new FetchRequest("orders", 0, 0, 4_096));
            assertEquals(ErrorCode.NONE, response.header().errorCode());
            FetchResponse fetch = (FetchResponse) response.body();
            assertEquals(1L, fetch.highWatermark());
            assertEquals(1, fetch.records().size());
            assertEquals(0L, fetch.records().get(0).offset());

            FetchTransferMetricsSnapshot metrics = broker.fetchTransferMetrics();
            assertEquals(1, metrics.sendfileResponses(), metrics.toString());
            assertTrue(metrics.sendfileBytes() > 0, metrics.toString());
        }
    }

    private static FetchResponse runFetch(
            Path dataDir, FetchTransferMode mode, List<ProducedRecord> produced) throws Exception {
        BrokerConfig config = BrokerTestHarness.controllerConfig(0, dataDir,
                "--segment-max-bytes", "64",
                "--sparse-index-interval-records", "1",
                "--fetch-transfer-mode", mode == FetchTransferMode.AUTO ? "auto" : "heap",
                "--fetch-transfer-timeout-ms", "5000");
        try (BrokerTestHarness broker = BrokerTestHarness.start(config);
             TestNetworkClient client = new TestNetworkClient()) {
            client.connect("127.0.0.1", broker.port());
            createTopic(client, "orders", 1);
            for (ProducedRecord record : produced) {
                produce(client, record);
            }
            MqDecoder.DecodedResponse response = client.send(
                    ApiKeys.FETCH, new FetchRequest("orders", 0, 1, 4_096));
            assertEquals(ErrorCode.NONE, response.header().errorCode());
            FetchTransferMetricsSnapshot metrics = broker.fetchTransferMetrics();
            if (mode == FetchTransferMode.AUTO) {
                assertEquals(1, metrics.sendfileResponses(), metrics.toString());
                assertEquals(0, metrics.heapResponses(), metrics.toString());
            } else {
                assertEquals(0, metrics.sendfileResponses(), metrics.toString());
                assertEquals(1, metrics.heapModeFallbacks(), metrics.toString());
            }
            return (FetchResponse) response.body();
        }
    }

    private static List<ProducedRecord> produceRecords(int count) {
        List<ProducedRecord> records = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            byte[] key = ("k-" + i).getBytes(StandardCharsets.UTF_8);
            byte[] value = ("value-" + i + "-" + "x".repeat(24)).getBytes(StandardCharsets.UTF_8);
            records.add(new ProducedRecord(i, key, value));
        }
        return records;
    }

    private static void createTopic(TestNetworkClient client, String topic, int partitions)
            throws InterruptedException {
        MqDecoder.DecodedResponse response = client.send(
                ApiKeys.CREATE_TOPIC, new CreateTopicRequest(topic, partitions));
        assertEquals(ErrorCode.NONE, response.header().errorCode());
    }

    private static void produce(TestNetworkClient client, ProducedRecord record)
            throws InterruptedException {
        MqDecoder.DecodedResponse response = client.send(
                ApiKeys.PRODUCE,
                new ProduceRequest("orders", 0, List.of(new ProduceRecord(record.key(), record.value()))));
        assertEquals(ErrorCode.NONE, response.header().errorCode());
    }

    private record ProducedRecord(long offset, byte[] key, byte[] value) {
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof ProducedRecord that)) {
                return false;
            }
            return offset == that.offset
                    && Arrays.equals(key, that.key)
                    && Arrays.equals(value, that.value);
        }

        @Override
        public int hashCode() {
            return Long.hashCode(offset) * 31 + Arrays.hashCode(key) * 17 + Arrays.hashCode(value);
        }
    }
}
