package com.minimq.broker;

import com.minimq.broker.support.BrokerTestHarness;
import com.minimq.broker.support.TestNetworkClient;
import com.minimq.common.error.ErrorCode;
import com.minimq.protocol.ApiKeys;
import com.minimq.protocol.message.ProtocolMessages.CreateTopicRequest;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TcpSmokeTest {

    @Test
    void brokerRespondsOverTcp() throws Exception {
        Path dataDir = BrokerTestHarness.newDataDir("tcp-smoke");
        try (BrokerTestHarness broker = BrokerTestHarness.startController(dataDir);
             TestNetworkClient client = new TestNetworkClient()) {
            client.connect("127.0.0.1", broker.port());
            var response = client.send(ApiKeys.CREATE_TOPIC, new CreateTopicRequest("orders", 1));
            assertEquals(ErrorCode.NONE, response.header().errorCode());
        }
    }
}
