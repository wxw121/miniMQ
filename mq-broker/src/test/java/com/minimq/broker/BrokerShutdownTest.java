package com.minimq.broker;

import com.minimq.broker.support.BrokerTestHarness;
import com.minimq.broker.support.TestNetworkClient;
import com.minimq.common.error.ErrorCode;
import com.minimq.protocol.ApiKeys;
import com.minimq.protocol.message.ProtocolMessages.CreateTopicRequest;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;

class BrokerShutdownTest {

    @Test
    void brokerCanRestartOnSamePortAfterStop() throws Exception {
        Path dataDir = BrokerTestHarness.newDataDir("restart");
        BrokerConfig firstConfig = BrokerTestHarness.controllerConfig(0, dataDir);
        BrokerServer first = new BrokerServer(firstConfig);
        first.start();
        first.stop();

        BrokerServer second = new BrokerServer(firstConfig);
        try (TestNetworkClient client = new TestNetworkClient()) {
            second.start();
            client.connect("127.0.0.1", firstConfig.port());
            var response = client.send(ApiKeys.CREATE_TOPIC, new CreateTopicRequest("orders", 1));
            assertEquals(ErrorCode.NONE, response.header().errorCode());
        } finally {
            second.stop();
        }
    }
}
