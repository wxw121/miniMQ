package com.minimq.client;

import com.minimq.broker.support.BrokerTestHarness;
import com.minimq.common.error.ErrorCode;
import com.minimq.protocol.ApiKeys;
import com.minimq.protocol.message.ProtocolMessages.MetadataRequest;
import org.junit.jupiter.api.Test;

import java.net.ServerSocket;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ControllerAwareNetworkClientTest {
    @Test
    void parsesEveryBootstrapEndpoint() {
        BootstrapServers servers = BootstrapServers.parse("host-a:19090, host-b:19091");
        assertEquals(2, servers.endpoints().size());
        assertEquals("host-b", servers.endpoints().get(1).host());
        assertThrows(IllegalArgumentException.class, () -> BootstrapServers.parse("host-only"));
    }

    @Test
    void skipsUnavailableBootstrapAndDiscoversControllerThroughFollower() throws Exception {
        Path controllerDir = BrokerTestHarness.newDataDir("client-discovery-controller");
        Path followerDir = BrokerTestHarness.newDataDir("client-discovery-follower");
        try (BrokerTestHarness controller = BrokerTestHarness.startController(controllerDir);
             BrokerTestHarness follower = BrokerTestHarness.startFollower(
                     1, followerDir, "127.0.0.1", controller.port());
             ControllerAwareNetworkClient client = new ControllerAwareNetworkClient(
                     "127.0.0.1:" + unusedPort() + "," + follower.bootstrapServers(),
                     "controller-aware-test")) {
            client.connect();
            var response = client.send(ApiKeys.METADATA, new MetadataRequest(""));
            assertEquals(ErrorCode.NONE, response.header().errorCode());
        }
    }

    private static int unusedPort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}
