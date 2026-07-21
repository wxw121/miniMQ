package com.minimq.client;

import com.minimq.broker.support.BrokerTestHarness;
import com.minimq.common.error.ErrorCode;
import com.minimq.protocol.ApiKeys;
import com.minimq.protocol.codec.MqDecoder;
import com.minimq.protocol.message.MqResponseHeader;
import com.minimq.protocol.message.ProtocolMessages.MetadataRequest;
import org.junit.jupiter.api.Test;

import java.net.ServerSocket;
import java.nio.file.Path;
import java.util.List;

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
    void rejectsInvalidBootstrapFormsAndExposesInitialEpoch() throws Exception {
        assertThrows(IllegalArgumentException.class, () -> new BootstrapServers(List.of()));
        assertThrows(IllegalArgumentException.class, () -> BootstrapServers.parse(null));
        assertThrows(IllegalArgumentException.class, () -> BootstrapServers.parse(" "));
        assertThrows(IllegalArgumentException.class, () -> BootstrapServers.parse(":9092"));
        assertThrows(IllegalArgumentException.class, () -> BootstrapServers.parse("host:0"));
        assertThrows(IllegalArgumentException.class, () -> BootstrapServers.parse("host:65536"));
        assertThrows(IllegalArgumentException.class, () -> BootstrapServers.parse("host:nope"));
        try (ControllerAwareNetworkClient client =
                     new ControllerAwareNetworkClient("localhost:9092", "validation")) {
            assertEquals(-1, client.controllerEpoch());
            assertThrows(com.minimq.client.exception.MiniMqServerException.class,
                    () -> client.ensureSuccess(new MqDecoder.DecodedResponse(
                            new MqResponseHeader(1, ErrorCode.NOT_CONTROLLER), null), "test"));
        }
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

    @Test
    void reportsFailureAfterEveryBootstrapIsUnavailable() throws Exception {
        try (ControllerAwareNetworkClient client = new ControllerAwareNetworkClient(
                "127.0.0.1:" + unusedPort(), "unavailable-bootstrap")) {
            assertThrows(com.minimq.client.exception.MiniMqConnectionException.class,
                    client::connect);
        }
    }

    private static int unusedPort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}
