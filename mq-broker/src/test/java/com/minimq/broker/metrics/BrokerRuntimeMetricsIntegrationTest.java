package com.minimq.broker.metrics;

import com.minimq.broker.support.BrokerTestHarness;
import com.minimq.broker.support.TestNetworkClient;
import com.minimq.common.error.ErrorCode;
import com.minimq.protocol.ApiKeys;
import com.minimq.protocol.message.ProtocolMessages.CreateTopicRequest;
import com.minimq.protocol.message.ProtocolMessages.FetchRequest;
import com.minimq.protocol.message.ProtocolMessages.FetchResponse;
import com.minimq.protocol.message.ProtocolMessages.FetchV2Request;
import com.minimq.protocol.message.ProtocolMessages.FetchV2Response;
import com.minimq.protocol.message.ProtocolMessages.MetadataRequest;
import com.minimq.protocol.message.ProtocolMessages.ProduceRecord;
import com.minimq.protocol.message.ProtocolMessages.ProduceRequest;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BrokerRuntimeMetricsIntegrationTest {

    @Test
    void brokerServerExposesRequestAndResponseMetricsFromRequestHandler() throws Exception {
        Path dir = BrokerTestHarness.newDataDir("runtime-metrics");
        try (BrokerTestHarness broker = BrokerTestHarness.startController(dir);
             TestNetworkClient client = new TestNetworkClient()) {
            client.connect("127.0.0.1", broker.port());
            BrokerRuntimeMetricsSnapshot before = broker.runtimeMetrics();

            assertEquals(ErrorCode.NONE, client.send(
                    ApiKeys.METADATA, new MetadataRequest("")).header().errorCode());

            BrokerRuntimeMetricsSnapshot after = broker.runtimeMetrics();
            assertEquals(
                    before.requestsByApiKey().getOrDefault(ApiKeys.METADATA, 0L) + 1,
                    after.requestsByApiKey().get(ApiKeys.METADATA));
            assertTrue(after.responsesByErrorCode().getOrDefault(ErrorCode.NONE, 0L)
                    >= before.responsesByErrorCode().getOrDefault(ErrorCode.NONE, 0L) + 1);
            assertTrue(after.latencyCountsByApiKey().getOrDefault(ApiKeys.METADATA, 0L)
                    >= before.latencyCountsByApiKey().getOrDefault(ApiKeys.METADATA, 0L) + 1);
        }
    }

    @Test
    void fetchBytesTracksLegacySendfilePayloadWithoutProtocolFraming() throws Exception {
        Path dir = BrokerTestHarness.newDataDir("runtime-fetch-sendfile");
        try (BrokerTestHarness broker = BrokerTestHarness.startController(dir);
             TestNetworkClient client = new TestNetworkClient()) {
            client.connect("127.0.0.1", broker.port());
            byte[] key = "key".getBytes();
            byte[] value = "value".getBytes();
            createSingleRecordTopic(client, key, value);
            long before = broker.runtimeMetrics().fetchBytes();

            FetchResponse response = (FetchResponse) client.send(
                    ApiKeys.FETCH, new FetchRequest("orders", 0, 0L, 0L, 1_024)).body();

            assertEquals(1, response.records().size());
            assertEquals(
                    before + key.length + value.length,
                    awaitFetchBytes(broker, before + key.length + value.length));
            assertTrue(broker.fetchTransferMetrics().sendfileResponses() >= 1);
        }
    }

    @Test
    void fetchBytesTracksLegacyHeapPayloadWithoutProtocolFraming() throws Exception {
        Path dir = BrokerTestHarness.newDataDir("runtime-fetch-heap");
        try (BrokerTestHarness broker = BrokerTestHarness.startController(
                     dir, "--fetch-transfer-mode", "heap");
             TestNetworkClient client = new TestNetworkClient()) {
            client.connect("127.0.0.1", broker.port());
            byte[] key = "heap-key".getBytes();
            byte[] value = "heap-value".getBytes();
            createSingleRecordTopic(client, key, value);
            long before = broker.runtimeMetrics().fetchBytes();

            FetchResponse response = (FetchResponse) client.send(
                    ApiKeys.FETCH, new FetchRequest("orders", 0, 0L, 0L, 1_024)).body();

            assertEquals(1, response.records().size());
            assertEquals(
                    before + key.length + value.length,
                    awaitFetchBytes(broker, before + key.length + value.length));
            assertTrue(broker.fetchTransferMetrics().heapResponses() >= 1);
        }
    }

    @Test
    void fetchBytesTracksFetchV2ReturnedPayloadWithoutProtocolFraming() throws Exception {
        Path dir = BrokerTestHarness.newDataDir("runtime-fetch-v2");
        try (BrokerTestHarness broker = BrokerTestHarness.startController(dir);
             TestNetworkClient client = new TestNetworkClient()) {
            client.connect("127.0.0.1", broker.port());
            byte[] key = "v2-key".getBytes();
            byte[] value = "v2-value".getBytes();
            createSingleRecordTopic(client, key, value);
            long before = broker.runtimeMetrics().fetchBytes();

            FetchV2Response response = (FetchV2Response) client.send(
                    ApiKeys.FETCH_V2, new FetchV2Request("orders", 0, 0L, 0L, 1_024)).body();

            assertEquals(1, response.records().items().size());
            assertEquals(
                    before + key.length + value.length,
                    awaitFetchBytes(broker, before + key.length + value.length));
        }
    }

    private static void createSingleRecordTopic(
            TestNetworkClient client, byte[] key, byte[] value) throws InterruptedException {
        assertEquals(ErrorCode.NONE, client.send(
                ApiKeys.CREATE_TOPIC, new CreateTopicRequest("orders", 1)).header().errorCode());
        assertEquals(ErrorCode.NONE, client.send(
                ApiKeys.PRODUCE,
                new ProduceRequest("orders", 0, List.of(new ProduceRecord(key, value))))
                .header().errorCode());
    }

    private static long awaitFetchBytes(BrokerTestHarness broker, long expected)
            throws InterruptedException {
        long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(2);
        long actual;
        while ((actual = broker.runtimeMetrics().fetchBytes()) != expected
                && System.nanoTime() < deadline) {
            Thread.sleep(10L);
        }
        return actual;
    }
}
