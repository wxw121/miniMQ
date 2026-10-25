package com.minimq.broker;

import com.minimq.broker.support.BrokerTestHarness;
import com.minimq.broker.support.TestNetworkClient;
import com.minimq.common.error.ErrorCode;
import com.minimq.protocol.ApiKeys;
import com.minimq.protocol.message.ProtocolMessages.CreateTopicRequest;
import com.minimq.protocol.message.ProtocolMessages.MaintenanceReportRequest;
import io.netty.channel.ChannelFuture;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.net.ServerSocket;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

class BrokerShutdownTest {

    @Test
    void failedLeaseInitializationClosesBoundChannel() throws Exception {
        Path dataDir = BrokerTestHarness.newDataDir("failed-start");
        int brokerPort;
        int unavailableControllerPort;
        try (ServerSocket brokerSocket = new ServerSocket(0);
                ServerSocket controllerSocket = new ServerSocket(0)) {
            brokerPort = brokerSocket.getLocalPort();
            unavailableControllerPort = controllerSocket.getLocalPort();
        }
        BrokerConfig config = new BrokerConfig(
                1,
                "127.0.0.1",
                brokerPort,
                dataDir,
                false,
                "127.0.0.1",
                unavailableControllerPort,
                false,
                BrokerTestHarness.ADMIN_TOKEN);
        BrokerServer server = new BrokerServer(config);

        try {
            assertThrows(Exception.class, server::start);
            Field channelFutureField = BrokerServer.class.getDeclaredField("channelFuture");
            channelFutureField.setAccessible(true);
            ChannelFuture boundChannel = (ChannelFuture) channelFutureField.get(server);
            assertFalse(boundChannel.channel().isOpen());
        } finally {
            server.stop();
        }
    }

    @Test
    void brokerCanRestartOnSamePortAfterStop() throws Exception {
        Path dataDir = BrokerTestHarness.newDataDir("restart");
        BrokerConfig firstConfig = BrokerTestHarness.controllerConfig(0, dataDir);
        BrokerServer first = new BrokerServer(firstConfig);
        first.start();
        assertEquals(BrokerAvailabilityGuard.State.ACTIVE, first.availabilityState());
        assertEquals(1L, first.brokerEpoch());
        try (TestNetworkClient client = new TestNetworkClient()) {
            client.connect("127.0.0.1", firstConfig.port());
            var response = client.send(
                    ApiKeys.MAINTENANCE_REPORT,
                    new MaintenanceReportRequest(BrokerTestHarness.ADMIN_TOKEN));
            assertEquals(ErrorCode.NONE, response.header().errorCode());
        }
        first.stop();

        BrokerServer second = new BrokerServer(firstConfig);
        try (TestNetworkClient client = new TestNetworkClient()) {
            second.start();
            assertEquals(BrokerAvailabilityGuard.State.ACTIVE, second.availabilityState());
            assertEquals(1L, second.brokerEpoch());
            client.connect("127.0.0.1", firstConfig.port());
            var response = client.send(ApiKeys.CREATE_TOPIC, new CreateTopicRequest("orders", 1));
            assertEquals(ErrorCode.NONE, response.header().errorCode());
        } finally {
            second.stop();
        }
    }
}
