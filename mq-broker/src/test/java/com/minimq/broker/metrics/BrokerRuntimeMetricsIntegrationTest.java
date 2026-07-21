package com.minimq.broker.metrics;

import com.minimq.broker.support.BrokerTestHarness;
import com.minimq.broker.support.TestNetworkClient;
import com.minimq.common.error.ErrorCode;
import com.minimq.protocol.ApiKeys;
import com.minimq.protocol.message.ProtocolMessages.MetadataRequest;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;

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
}
