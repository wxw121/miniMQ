package com.minimq.broker.audit;

import com.minimq.broker.BrokerAvailabilityGuard;
import com.minimq.broker.BrokerConfig;
import com.minimq.broker.FetchTransferMetrics;
import com.minimq.broker.MaintenanceAdminService;
import com.minimq.broker.PartitionManager;
import com.minimq.broker.ControllerRole;
import com.minimq.broker.auth.OperatorAuthorizer;
import com.minimq.broker.controller.ClusterController;
import com.minimq.broker.controller.GroupCoordinator;
import com.minimq.broker.handler.MqRequestHandler;
import com.minimq.broker.metadata.MetadataStore;
import com.minimq.broker.metrics.BrokerRuntimeMetrics;
import com.minimq.broker.replication.ReplicaStateManager;
import com.minimq.broker.replication.ReplicaStateReporter;
import com.minimq.broker.resilience.RequestThrottler;
import com.minimq.broker.support.BrokerTestHarness;
import com.minimq.common.error.ErrorCode;
import com.minimq.protocol.ApiKeys;
import com.minimq.protocol.ProtocolSerdes;
import com.minimq.protocol.codec.MqDecoder;
import com.minimq.protocol.codec.MqEncoder;
import com.minimq.protocol.message.MqRequestHeader;
import com.minimq.protocol.message.ProtocolMessages.FailoverAuditRequest;
import com.minimq.protocol.message.ProtocolMessages.FailoverAuditResponse;
import com.minimq.storage.LogManager;
import io.netty.buffer.ByteBuf;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class FailoverAuditHandlerTest {
    @TempDir
    Path tempDir;

    @Test
    void key26ReturnsNewestFirstAndRequiresStatusCredential() throws Exception {
        BrokerConfig config = BrokerTestHarness.controllerConfig(0, tempDir);
        BrokerAvailabilityGuard guard = new BrokerAvailabilityGuard();
        guard.activate();
        ClusterController controller = new ClusterController(new MetadataStore(tempDir.resolve("metadata")));
        GroupCoordinator groups = new GroupCoordinator(controller);
        PartitionManager partitions = new PartitionManager(new LogManager(tempDir.resolve("logs")));
        MaintenanceAdminService maintenance = new MaintenanceAdminService(
                OperatorAuthorizer.fromConfig(config), partitions);
        ReplicaStateReporter reporter = new ReplicaStateReporter(config, controller, guard);
        FailoverAuditLog audit = new FailoverAuditLog(0, () -> 10L);
        audit.append(FailoverAuditEventType.LEADER_CHANGED, "old");
        audit.append(FailoverAuditEventType.LEADER_READY, "new");

        EmbeddedChannel channel = new EmbeddedChannel(
                new MqDecoder(),
                new MqRequestHandler(
                        config,
                        controller,
                        groups,
                        partitions,
                        new ReplicaStateManager(),
                        new FetchTransferMetrics(),
                        reporter,
                        guard,
                        maintenance,
                        ControllerRole.legacy(config),
                        OperatorAuthorizer.fromConfig(config),
                        new BrokerRuntimeMetrics(),
                        new RequestThrottler(0),
                        null,
                        audit),
                new MqEncoder());
        try {
            MqDecoder.DecodedResponse response = send(channel,
                    new FailoverAuditRequest(BrokerTestHarness.ADMIN_TOKEN, 1));
            assertEquals(ErrorCode.NONE, response.header().errorCode());
            FailoverAuditResponse body = (FailoverAuditResponse) response.body();
            assertEquals(2, body.events().total());
            assertEquals(1, body.events().items().size());
            assertEquals("LEADER_READY", body.events().items().get(0).eventType());
            assertEquals(2L, body.events().items().get(0).sequence());

            MqDecoder.DecodedResponse denied = send(channel,
                    new FailoverAuditRequest("wrong-token", 1));
            assertEquals(ErrorCode.UNAUTHORIZED, denied.header().errorCode());
            assertNull(denied.body());
        } finally {
            channel.finishAndReleaseAll();
            reporter.close();
            maintenance.close();
            partitions.close();
        }
    }

    private static MqDecoder.DecodedResponse send(
            EmbeddedChannel channel, FailoverAuditRequest request) {
        MqRequestHeader header = new MqRequestHeader(ApiKeys.FAILOVER_AUDIT, 1, "test");
        channel.writeInbound(ProtocolSerdes.encodeRequest(header, request));
        ByteBuf response = channel.readOutbound();
        if (response == null) {
            throw new AssertionError("missing response");
        }
        try {
            return MqDecoder.decodeResponse(unwrapPayload(response), ApiKeys.FAILOVER_AUDIT);
        } finally {
            response.release();
        }
    }

    private static ByteBuf unwrapPayload(ByteBuf frame) {
        frame.readShort();
        frame.readByte();
        int length = frame.readInt();
        return frame.readSlice(length);
    }
}
