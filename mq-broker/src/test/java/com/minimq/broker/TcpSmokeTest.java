package com.minimq.broker;

import com.minimq.broker.support.BrokerTestHarness;
import com.minimq.broker.support.TestNetworkClient;
import com.minimq.protocol.ApiKeys;
import com.minimq.protocol.message.ProtocolMessages.CreateTopicRequest;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;

class TcpSmokeTest {

    @Test
    void brokerRespondsOverTcp() throws Exception {
        Path dataDir = BrokerTestHarness.newDataDir("tcp-smoke");
        try (BrokerTestHarness broker = BrokerTestHarness.startController(dataDir);
             TestNetworkClient client = new TestNetworkClient()) {
            System.out.println("Connecting to port " + broker.port());
            client.connect("127.0.0.1", broker.port());
            var response = client.send(ApiKeys.CREATE_TOPIC, new CreateTopicRequest("orders", 1));
            System.out.println("Response error=" + response.header().errorCode());
        }
    }
}
