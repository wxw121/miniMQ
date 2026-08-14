package com.minimq.broker;

import com.minimq.broker.admin.TopicAdminService;
import com.minimq.broker.auth.OperatorAuthorizer;
import com.minimq.broker.controller.ClusterController;
import com.minimq.broker.controller.GroupCoordinator;
import com.minimq.broker.coordination.ControllerIdentity;
import com.minimq.broker.handler.MqRequestHandler;
import com.minimq.broker.metadata.ClusterMetadata;
import com.minimq.broker.metadata.MetadataStore;
import com.minimq.broker.metrics.BrokerRuntimeMetrics;
import com.minimq.broker.metrics.BrokerRuntimeMetricsSnapshot;
import com.minimq.broker.replication.ReplicaStateReporter;
import com.minimq.broker.replication.ReplicaStateManager;
import com.minimq.broker.resilience.RequestThrottler;
import com.minimq.broker.resilience.ThrottleResult;
import com.minimq.broker.support.BrokerTestHarness;
import com.minimq.common.acks.ProduceAcks;
import com.minimq.common.error.ErrorCode;
import com.minimq.protocol.ApiKeys;
import com.minimq.protocol.MqFrame;
import com.minimq.protocol.ProtocolSerdes;
import com.minimq.protocol.codec.ByteBufSerde;
import com.minimq.protocol.codec.MqDecoder;
import com.minimq.protocol.codec.MqEncoder;
import com.minimq.protocol.message.MqRequestHeader;
import com.minimq.protocol.message.ProtocolMessages.AuthStatusRequest;
import com.minimq.protocol.message.ProtocolMessages.AuthStatusResponse;
import com.minimq.protocol.message.ProtocolMessages.CreateTopicAdminRequest;
import com.minimq.protocol.message.ProtocolMessages.CreateTopicAdminResponse;
import com.minimq.protocol.message.ProtocolMessages.CreateTopicRequest;
import com.minimq.protocol.message.ProtocolMessages.DeleteTopicRequest;
import com.minimq.protocol.message.ProtocolMessages.DeleteTopicResponse;
import com.minimq.protocol.message.ProtocolMessages.ConfigureTopicReliabilityRequest;
import com.minimq.protocol.message.ProtocolMessages.ConfigureTopicReliabilityResponse;
import com.minimq.protocol.message.ProtocolMessages.DescribeTopicRequest;
import com.minimq.protocol.message.ProtocolMessages.DescribeTopicResponse;
import com.minimq.protocol.message.ProtocolMessages.DiscoverControllerRequest;
import com.minimq.protocol.message.ProtocolMessages.DiscoverControllerResponse;
import com.minimq.protocol.message.ProtocolMessages.FetchRequest;
import com.minimq.protocol.message.ProtocolMessages.FetchResponse;
import com.minimq.protocol.message.ProtocolMessages.FetchV2Request;
import com.minimq.protocol.message.ProtocolMessages.FetchV2Response;
import com.minimq.protocol.message.ProtocolMessages.GroupStatusRequest;
import com.minimq.protocol.message.ProtocolMessages.GroupStatusResponse;
import com.minimq.protocol.message.ProtocolMessages.HeartbeatRequest;
import com.minimq.protocol.message.ProtocolMessages.IncreasePartitionsRequest;
import com.minimq.protocol.message.ProtocolMessages.IncreasePartitionsResponse;
import com.minimq.protocol.message.ProtocolMessages.JoinGroupRequest;
import com.minimq.protocol.message.ProtocolMessages.JoinGroupResponse;
import com.minimq.protocol.message.ProtocolMessages.LeaveGroupRequest;
import com.minimq.protocol.message.ProtocolMessages.ListTopicsRequest;
import com.minimq.protocol.message.ProtocolMessages.ListTopicsResponse;
import com.minimq.protocol.message.ProtocolMessages.MaintenanceMetricsRequest;
import com.minimq.protocol.message.ProtocolMessages.MaintenanceMetricsResponse;
import com.minimq.protocol.message.ProtocolMessages.MaintenanceReportRequest;
import com.minimq.protocol.message.ProtocolMessages.MaintenanceReportResponse;
import com.minimq.protocol.message.ProtocolMessages.MaintenanceRunRequest;
import com.minimq.protocol.message.ProtocolMessages.MetadataRequest;
import com.minimq.protocol.message.ProtocolMessages.MarkBrokerOfflineAdminRequest;
import com.minimq.protocol.message.ProtocolMessages.MarkBrokerOfflineAdminResponse;
import com.minimq.protocol.message.ProtocolMessages.MetadataV2Request;
import com.minimq.protocol.message.ProtocolMessages.MetadataV2Response;
import com.minimq.protocol.message.ProtocolMessages.OffsetCommitRequest;
import com.minimq.protocol.message.ProtocolMessages.OffsetFetchRequest;
import com.minimq.protocol.message.ProtocolMessages.OffsetFetchResponse;
import com.minimq.protocol.message.ProtocolMessages.OpsStatusRequest;
import com.minimq.protocol.message.ProtocolMessages.OpsStatusResponse;
import com.minimq.protocol.message.ProtocolMessages.ProduceRecord;
import com.minimq.protocol.message.ProtocolMessages.ProduceRequest;
import com.minimq.protocol.message.ProtocolMessages.ProduceResponse;
import com.minimq.protocol.message.ProtocolMessages.ProduceV2Request;
import com.minimq.protocol.message.ProtocolMessages.ProduceV2Response;
import com.minimq.protocol.message.ProtocolMessages.RegisterBrokerRequest;
import com.minimq.protocol.message.ProtocolMessages.BrokerHeartbeatRequest;
import com.minimq.protocol.message.ProtocolMessages.ReplicaFetchRequest;
import com.minimq.protocol.message.ProtocolMessages.ReplicaFetchResponse;
import com.minimq.protocol.message.ProtocolMessages.ReplicaFetchV2Request;
import com.minimq.protocol.message.ProtocolMessages.ReplicaFetchV2Response;
import com.minimq.protocol.message.ProtocolMessages.RuntimeMetricsRequest;
import com.minimq.protocol.message.ProtocolMessages.RuntimeMetricsResponse;
import com.minimq.protocol.message.ProtocolMessages.RuntimeMetricsV2Request;
import com.minimq.protocol.message.ProtocolMessages.RuntimeMetricsV2Response;
import com.minimq.protocol.message.ProtocolMessages.ScheduledProduceRecord;
import com.minimq.protocol.message.ProtocolMessages.SyncGroupRequest;
import com.minimq.protocol.message.ProtocolMessages.SyncGroupResponse;
import com.minimq.protocol.message.ProtocolMessages.TopicDeleteAckRequest;
import com.minimq.protocol.message.ProtocolMessages.TopicDeleteAckResponse;
import com.minimq.protocol.message.ProtocolMessages.UpdateReplicaLeoRequest;
import com.minimq.protocol.message.ProtocolMessages.UpdateReplicaStateV2Request;
import com.minimq.protocol.message.ProtocolMessages.UpdateReplicaStateV2Response;
import com.minimq.protocol.message.ProtocolMessages.V4BoundedList;
import com.minimq.storage.CompactionPolicy;
import com.minimq.storage.FlushPolicy;
import com.minimq.storage.LogMaintenanceConfig;
import com.minimq.storage.LogManager;
import com.minimq.storage.LogManagerConfig;
import com.minimq.storage.RetentionPolicy;
import com.minimq.storage.SegmentedLogConfig;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.ReferenceCountUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EmbeddedBrokerChannelTest {
    private final List<MaintenanceAdminService> maintenanceServices = new ArrayList<>();

    @TempDir
    Path tempDir;

    @AfterEach
    void closeMaintenanceServices() throws InterruptedException {
        for (MaintenanceAdminService service : maintenanceServices) {
            service.close();
        }
    }

    @Test
    void legacyCreateTopicIsDeniedUnlessCompatibilityFlagIsSet() {
        BrokerConfig defaultConfig = defaultConfig(tempDir.resolve("legacy-create-denied"), true);
        EmbeddedChannel denied = newChannel(
                defaultConfig,
                new ClusterController(new MetadataStore(defaultConfig.dataDir())));

        assertEquals(ErrorCode.UNAUTHORIZED, send(
                denied, ApiKeys.CREATE_TOPIC, new CreateTopicRequest("orders", 1)).header().errorCode());
        denied.finishAndReleaseAll();

        BrokerConfig legacyConfig = withLegacyApis(defaultConfig(tempDir.resolve("legacy-create-allowed"), true));
        ClusterController allowedController = new ClusterController(new MetadataStore(legacyConfig.dataDir()));
        assertEquals(ErrorCode.NONE,
                allowedController.registerBroker(new RegisterBrokerRequest(0, "127.0.0.1", 9092)).errorCode());
        EmbeddedChannel allowed = newChannel(
                legacyConfig,
                allowedController);
        assertEquals(ErrorCode.NONE, send(
                allowed, ApiKeys.CREATE_TOPIC, new CreateTopicRequest("orders", 1)).header().errorCode());
        allowed.finishAndReleaseAll();
    }

    @Test
    void legacyInternalApisAreDeniedBeforeBodyDecodeUnlessCompatibilityFlagIsSet() {
        short[] internalApis = {
                ApiKeys.REPLICA_FETCH,
                ApiKeys.MARK_BROKER_OFFLINE,
                ApiKeys.UPDATE_REPLICA_LEO
        };
        for (short apiKey : internalApis) {
            BrokerConfig defaultConfig = defaultConfig(tempDir.resolve("legacy-internal-denied-" + apiKey), true);
            EmbeddedChannel denied = newChannel(
                    defaultConfig,
                    new ClusterController(new MetadataStore(defaultConfig.dataDir())));

            assertEquals(ErrorCode.UNAUTHORIZED, sendHeaderOnly(denied, apiKey).header().errorCode());
            denied.finishAndReleaseAll();

            BrokerConfig legacyConfig =
                    withLegacyApis(defaultConfig(tempDir.resolve("legacy-internal-allowed-" + apiKey), true));
            EmbeddedChannel allowed = newChannel(
                    legacyConfig,
                    new ClusterController(new MetadataStore(legacyConfig.dataDir())));

            assertEquals(ErrorCode.INVALID_REQUEST, sendHeaderOnly(allowed, apiKey).header().errorCode());
            allowed.finishAndReleaseAll();
        }
    }

    @Test
    void operatorApisAuthorizeCredentialBeforeFullBodyDecode() {
        BrokerConfig config = BrokerConfig.parse(new String[] {
                "--broker-id", "0",
                "--host", "127.0.0.1",
                "--port", "9092",
                "--data-dir", tempDir.resolve("operator-auth").toString(),
                "--admin-token", BrokerTestHarness.ADMIN_TOKEN,
                "--operator-credential", "status-token=STATUS",
                "--operator-credential", "topic-token=TOPIC_ADMIN"
        });
        EmbeddedChannel channel = newChannel(
                config,
                new ClusterController(new MetadataStore(config.dataDir())));

        assertEquals(ErrorCode.UNAUTHORIZED,
                sendCredentialPrefixOnly(channel, ApiKeys.OPS_STATUS, "unknown-token").header().errorCode());
        assertEquals(ErrorCode.FORBIDDEN,
                sendCredentialPrefixOnly(channel, ApiKeys.OPS_STATUS, "topic-token").header().errorCode());
        assertEquals(ErrorCode.INVALID_REQUEST,
                sendCredentialPrefixOnly(channel, ApiKeys.OPS_STATUS, "status-token").header().errorCode());
        assertEquals(ErrorCode.UNAUTHORIZED,
                sendCredentialPrefixOnly(channel, ApiKeys.AUTH_STATUS, "unknown-token").header().errorCode());
        channel.finishAndReleaseAll();
    }

    @Test
    void brokerOfflineAdminRoutesAuthorizedMutation() {
        BrokerConfig config = withLegacyApis(defaultConfig(tempDir.resolve("broker-offline-admin"), true));
        ClusterController controller = new ClusterController(new MetadataStore(config.dataDir()));
        assertEquals(ErrorCode.NONE,
                controller.registerBroker(new RegisterBrokerRequest(0, "127.0.0.1", 9092)).errorCode());
        EmbeddedChannel channel = newChannel(config, controller);

        MqDecoder.DecodedResponse response = send(
                channel,
                ApiKeys.MARK_BROKER_OFFLINE_ADMIN,
                new MarkBrokerOfflineAdminRequest(BrokerTestHarness.ADMIN_TOKEN, 0));
        assertEquals(ErrorCode.NONE, response.header().errorCode());
        assertEquals(new MarkBrokerOfflineAdminResponse(0, controller.snapshot().metadataVersion), response.body());
        assertFalse(controller.snapshot().brokers.get(0).online);

        MqDecoder.DecodedResponse unknown = send(
                channel,
                ApiKeys.MARK_BROKER_OFFLINE_ADMIN,
                new MarkBrokerOfflineAdminRequest(BrokerTestHarness.ADMIN_TOKEN, 99));
        assertEquals(ErrorCode.UNKNOWN_BROKER, unknown.header().errorCode());
        channel.finishAndReleaseAll();
    }

    @Test
    void deleteTopicWaitsForCleanupWithoutBlockingEventLoopOrReleasingPermit() throws Exception {
        Path dataDir = tempDir.resolve("delete-topic-async");
        BrokerConfig config = defaultConfig(dataDir, true);
        ClusterController controller = new ClusterController(new MetadataStore(dataDir));
        assertEquals(ErrorCode.NONE,
                controller.registerBroker(new RegisterBrokerRequest(0, "127.0.0.1", 9092)).errorCode());
        assertEquals(ErrorCode.NONE, controller.createTopic(new CreateTopicRequest("orders", 1)));

        GroupCoordinator groupCoordinator = new GroupCoordinator(controller);
        PartitionManager partitionManager = new PartitionManager(new LogManager(dataDir));
        BrokerAvailabilityGuard guard = new BrokerAvailabilityGuard();
        guard.activate();
        RequestThrottler throttler = new RequestThrottler(2);
        MaintenanceAdminService maintenanceService =
                new MaintenanceAdminService(OperatorAuthorizer.fromConfig(config), partitionManager);
        TopicAdminService topicAdminService = new TopicAdminService(controller, 10_000L);
        EmbeddedChannel channel = new EmbeddedChannel(
                new MqDecoder(),
                new MqRequestHandler(
                        config,
                        controller,
                        groupCoordinator,
                        partitionManager,
                        new ReplicaStateManager(),
                        new FetchTransferMetrics(),
                        new ReplicaStateReporter(config, controller, guard),
                        guard,
                        maintenanceService,
                        ControllerRole.legacy(config),
                        OperatorAuthorizer.fromConfig(config),
                        new BrokerRuntimeMetrics(),
                        throttler,
                        null,
                        null,
                        topicAdminService),
                new MqEncoder());
        try {
            int deleteCorrelationId = 42;
            channel.writeInbound(ProtocolSerdes.encodeRequest(
                    new MqRequestHeader(ApiKeys.DELETE_TOPIC, deleteCorrelationId, "test"),
                    new DeleteTopicRequest(BrokerTestHarness.ADMIN_TOKEN, "orders")));

            ByteBuf earlyResponse = channel.readOutbound();
            try {
                assertNull(earlyResponse);
            } finally {
                if (earlyResponse != null) {
                    earlyResponse.release();
                }
            }
            assertEquals(1, throttler.inFlight());

            MqDecoder.DecodedResponse discovery = send(
                    channel, ApiKeys.DISCOVER_CONTROLLER, new DiscoverControllerRequest());
            assertEquals(ErrorCode.NONE, discovery.header().errorCode());
            assertNotNull(discovery.body());
            assertEquals(1, throttler.inFlight());

            ClusterMetadata.TopicTombstone tombstone = null;
            long tombstoneDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
            while (tombstone == null && System.nanoTime() < tombstoneDeadline) {
                tombstone = controller.getTopicTombstone("orders");
                if (tombstone == null) {
                    LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(5));
                }
            }
            var broker = controller.getBroker(0);
            assertNotNull(tombstone);
            assertNotNull(broker);
            assertEquals(ErrorCode.NONE, controller.acknowledgeTopicDeletion(
                    "orders",
                    tombstone.tombstoneVersion,
                    0,
                    broker.brokerEpoch,
                    controller.snapshot().controllerEpoch));

            ByteBuf responseFrame = awaitOutbound(channel);
            try {
                MqDecoder.DecodedResponse response = MqDecoder.decodeResponse(
                        unwrapPayload(responseFrame), ApiKeys.DELETE_TOPIC);
                assertEquals(deleteCorrelationId, response.header().correlationId());
                assertEquals(ErrorCode.NONE, response.header().errorCode());
                DeleteTopicResponse body = (DeleteTopicResponse) response.body();
                assertEquals("orders", body.topicName());
                assertEquals("DELETED", body.tombstoneState());
            } finally {
                responseFrame.release();
            }
            assertEquals(0, throttler.inFlight());
        } finally {
            topicAdminService.close();
            channel.runPendingTasks();
            channel.finishAndReleaseAll();
            maintenanceService.close();
            partitionManager.close();
        }
    }

    @Test
    void createTopicOverEmbeddedChannel() {
        MetadataStore store = new MetadataStore(tempDir);
        ClusterController clusterController = new ClusterController(store);
        GroupCoordinator groupCoordinator = new GroupCoordinator(clusterController);
        LogManager logManager = new LogManager(tempDir);
        PartitionManager partitionManager = new PartitionManager(logManager);
        BrokerConfig config = withLegacyApis(new BrokerConfig(
                0, "127.0.0.1", 9092, tempDir, true, "127.0.0.1", 9092, false,
                BrokerTestHarness.ADMIN_TOKEN));

        EmbeddedChannel channel = new EmbeddedChannel(
                new MqDecoder(),
                new MqRequestHandler(
                        config,
                        clusterController,
                        groupCoordinator,
                        partitionManager,
                        maintenanceServiceFor(config, partitionManager)),
                new MqEncoder());

        MqRequestHeader header = new MqRequestHeader(ApiKeys.CREATE_TOPIC, 1, "test");
        ByteBuf frame = ProtocolSerdes.encodeRequest(header, new CreateTopicRequest("orders", 1));
        channel.writeInbound(frame);
        channel.runPendingTasks();
        ByteBuf response = channel.readOutbound();
        assertNotNull(response);
        response.release();
        channel.finishAndReleaseAll();
    }

    @Test
    void malformedBodyReturnsInvalidRequestWithCorrelationId() {
        MetadataStore store = new MetadataStore(tempDir);
        ClusterController clusterController = new ClusterController(store);
        GroupCoordinator groupCoordinator = new GroupCoordinator(clusterController);
        LogManager logManager = new LogManager(tempDir);
        PartitionManager partitionManager = new PartitionManager(logManager);
        BrokerConfig config = withLegacyApis(new BrokerConfig(
                0, "127.0.0.1", 9092, tempDir, true, "127.0.0.1", 9092, false,
                BrokerTestHarness.ADMIN_TOKEN));

        EmbeddedChannel channel = new EmbeddedChannel(
                new MqDecoder(),
                new MqRequestHandler(
                        config,
                        clusterController,
                        groupCoordinator,
                        partitionManager,
                        maintenanceServiceFor(config, partitionManager)),
                new MqEncoder());

        ByteBuf payload = channel.alloc().buffer();
        payload.writeShort(ApiKeys.CREATE_TOPIC);
        payload.writeInt(99);
        payload.writeShort(4);
        payload.writeBytes(new byte[] {'t', 'e', 's', 't'});
        payload.writeShort(6);
        payload.writeByte('o');

        ByteBuf frame = channel.alloc().buffer();
        frame.writeShort(MqFrame.MAGIC);
        frame.writeByte(MqFrame.VERSION);
        frame.writeInt(payload.readableBytes());
        frame.writeBytes(payload);
        payload.release();

        channel.writeInbound(frame);
        ByteBuf responseFrame = channel.readOutbound();
        assertNotNull(responseFrame);
        MqDecoder.DecodedResponse response = MqDecoder.decodeResponse(
                unwrapPayload(responseFrame), ApiKeys.CREATE_TOPIC);
        assertEquals(99, response.header().correlationId());
        assertEquals(ErrorCode.INVALID_REQUEST, response.header().errorCode());
        responseFrame.release();
        channel.finishAndReleaseAll();
    }

    private static ByteBuf unwrapPayload(ByteBuf frame) {
        frame.readShort();
        frame.readByte();
        int length = frame.readInt();
        return frame.readSlice(length);
    }

    @Test
    void nonControllerRejectsControllerOnlyApis() {
        MetadataStore store = new MetadataStore(tempDir);
        ClusterController clusterController = new ClusterController(store);
        EmbeddedChannel channel = newChannel(clusterController, false);

        MqDecoder.DecodedResponse response = send(channel, ApiKeys.METADATA, new MetadataRequest("orders"));

        assertEquals(ErrorCode.NOT_CONTROLLER, response.header().errorCode());
        channel.finishAndReleaseAll();
    }

    @Test
    void anyBrokerAnswersControllerDiscovery() {
        ClusterController clusterController = new ClusterController(new MetadataStore(tempDir));
        clusterController.registerBroker(new RegisterBrokerRequest(0, "127.0.0.1", 9092));
        EmbeddedChannel channel = newChannel(clusterController, false, 1);

        MqDecoder.DecodedResponse decoded = send(
                channel, ApiKeys.DISCOVER_CONTROLLER, new DiscoverControllerRequest());
        DiscoverControllerResponse response = (DiscoverControllerResponse) decoded.body();

        assertEquals(ErrorCode.NONE, decoded.header().errorCode());
        assertEquals(0, response.controller().brokerId());
        assertEquals("127.0.0.1", response.controller().host());
        assertEquals(9092, response.controller().port());
        channel.finishAndReleaseAll();
    }

    @Test
    void fencedBrokerRejectsDataAndMaintenanceMutationButAllowsMaintenanceReads() {
        ClusterController clusterController = new ClusterController(new MetadataStore(tempDir));
        clusterController.registerBroker(new RegisterBrokerRequest(0, "127.0.0.1", 9092));
        clusterController.createTopic(new CreateTopicRequest("orders", 1));
        BrokerAvailabilityGuard guard = new BrokerAvailabilityGuard();
        guard.fence();
        EmbeddedChannel channel = newChannel(clusterController, true, 0, guard);

        assertEquals(ErrorCode.BROKER_FENCED, send(
                channel,
                ApiKeys.PRODUCE,
                new ProduceRequest("orders", 0, List.of(new ProduceRecord(null, new byte[] {1}))))
                .header().errorCode());
        assertEquals(ErrorCode.BROKER_FENCED, send(
                channel,
                ApiKeys.FETCH,
                new FetchRequest("orders", 0, 0L, 1024)).header().errorCode());
        assertEquals(ErrorCode.BROKER_FENCED, send(
                channel,
                ApiKeys.REPLICA_FETCH,
                new ReplicaFetchRequest("orders", 0, 1, 0L, 1024)).header().errorCode());
        assertEquals(ErrorCode.BROKER_FENCED, send(
                channel,
                ApiKeys.MAINTENANCE_RUN,
                new MaintenanceRunRequest(BrokerTestHarness.ADMIN_TOKEN)).header().errorCode());
        assertEquals(ErrorCode.NONE, send(
                channel,
                ApiKeys.MAINTENANCE_REPORT,
                new MaintenanceReportRequest(BrokerTestHarness.ADMIN_TOKEN)).header().errorCode());
        assertEquals(ErrorCode.NONE, send(
                channel,
                ApiKeys.MAINTENANCE_METRICS,
                new MaintenanceMetricsRequest(BrokerTestHarness.ADMIN_TOKEN)).header().errorCode());
        channel.finishAndReleaseAll();
    }

    @Test
    void controllerHandlesCurrentAndStaleBrokerHeartbeatEpochs() {
        ClusterController clusterController = new ClusterController(new MetadataStore(tempDir));
        clusterController.registerBroker(new RegisterBrokerRequest(0, "127.0.0.1", 9092));
        EmbeddedChannel channel = newChannel(clusterController, true);

        assertEquals(ErrorCode.NONE, send(
                channel,
                ApiKeys.BROKER_HEARTBEAT,
                new BrokerHeartbeatRequest(0, 1L)).header().errorCode());
        assertEquals(ErrorCode.BROKER_FENCED, send(
                channel,
                ApiKeys.BROKER_HEARTBEAT,
                new BrokerHeartbeatRequest(0, 0L)).header().errorCode());
        channel.finishAndReleaseAll();
    }

    @Test
    void controllerAcceptsReplicaLeoUpdates() {
        ClusterController clusterController = new ClusterController(new MetadataStore(tempDir));
        clusterController.registerBroker(new RegisterBrokerRequest(0, "127.0.0.1", 9092));
        clusterController.registerBroker(new RegisterBrokerRequest(1, "127.0.0.1", 9093));
        clusterController.createTopic(new CreateTopicRequest("orders", 1, 2));
        EmbeddedChannel channel = newChannel(clusterController, true);

        MqDecoder.DecodedResponse response = send(
                channel,
                ApiKeys.UPDATE_REPLICA_LEO,
                new UpdateReplicaLeoRequest("orders", 0, 1, 1L, 0L, 9L));

        assertEquals(ErrorCode.NONE, response.header().errorCode());
        assertEquals(9L, clusterController.replicaLogEndOffset("orders", 0, 1));
        channel.finishAndReleaseAll();
    }

    @Test
    void nonControllerRejectsReplicaLeoUpdates() {
        ClusterController clusterController = new ClusterController(new MetadataStore(tempDir));
        EmbeddedChannel channel = newChannel(clusterController, false);

        MqDecoder.DecodedResponse response = send(
                channel,
                ApiKeys.UPDATE_REPLICA_LEO,
                new UpdateReplicaLeoRequest("orders", 0, 1, 9L));

        assertEquals(ErrorCode.NOT_CONTROLLER, response.header().errorCode());
        channel.finishAndReleaseAll();
    }

    @Test
    void produceToUnknownTopicReturnsUnknownTopic() {
        MetadataStore store = new MetadataStore(tempDir);
        ClusterController clusterController = new ClusterController(store);
        EmbeddedChannel channel = newChannel(clusterController, true);

        MqDecoder.DecodedResponse response = send(channel, ApiKeys.PRODUCE,
                new ProduceRequest("missing", 0, List.of(new ProduceRecord(null, new byte[] {1}))));

        assertEquals(ErrorCode.UNKNOWN_TOPIC, response.header().errorCode());
        channel.finishAndReleaseAll();
    }

    @Test
    void produceToUnknownPartitionReturnsUnknownPartition() {
        MetadataStore store = new MetadataStore(tempDir);
        ClusterController clusterController = new ClusterController(store);
        clusterController.registerBroker(new RegisterBrokerRequest(0, "127.0.0.1", 9092));
        clusterController.createTopic(new CreateTopicRequest("orders", 1));
        EmbeddedChannel channel = newChannel(clusterController, true);

        MqDecoder.DecodedResponse response = send(channel, ApiKeys.PRODUCE,
                new ProduceRequest("orders", 9, List.of(new ProduceRecord(null, new byte[] {1}))));

        assertEquals(ErrorCode.UNKNOWN_PARTITION, response.header().errorCode());
        channel.finishAndReleaseAll();
    }

    @Test
    void produceFetchAndGroupApisRoundTripOverEmbeddedChannel() {
        MetadataStore store = new MetadataStore(tempDir);
        ClusterController clusterController = new ClusterController(store);
        EmbeddedChannel channel = newChannel(clusterController, true);

        assertEquals(ErrorCode.NONE, send(channel, ApiKeys.REGISTER_BROKER,
                new RegisterBrokerRequest(0, "127.0.0.1", 9092)).header().errorCode());
        assertEquals(ErrorCode.NONE, send(channel, ApiKeys.CREATE_TOPIC,
                new CreateTopicRequest("orders", 1)).header().errorCode());

        MqDecoder.DecodedResponse produceResponse = send(channel, ApiKeys.PRODUCE,
                new ProduceRequest("orders", 0, List.of(
                        new ProduceRecord("k1".getBytes(), "v1".getBytes()),
                        new ProduceRecord("k2".getBytes(), "v2".getBytes()))));
        assertEquals(ErrorCode.NONE, produceResponse.header().errorCode());
        assertEquals(1, ((ProduceResponse) produceResponse.body()).baseOffset());
        assertEquals(2L, clusterController.replicaLogEndOffset("orders", 0, 0));

        MqDecoder.DecodedResponse fetchResponse = send(channel, ApiKeys.FETCH,
                new FetchRequest("orders", 0, 0, 1024));
        FetchResponse fetch = (FetchResponse) fetchResponse.body();
        assertEquals(ErrorCode.NONE, fetchResponse.header().errorCode());
        assertEquals(2, fetch.highWatermark());
        assertEquals(2, fetch.records().size());

        MqDecoder.DecodedResponse joinResponse = send(channel, ApiKeys.JOIN_GROUP,
                new JoinGroupRequest("g1", "", "orders"));
        JoinGroupResponse join = (JoinGroupResponse) joinResponse.body();
        assertEquals(ErrorCode.NONE, joinResponse.header().errorCode());
        assertEquals("g1", join.memberId().substring(0, 2));

        MqDecoder.DecodedResponse syncResponse = send(channel, ApiKeys.SYNC_GROUP,
                new SyncGroupRequest("g1", join.generationId(), join.memberId(), new byte[0]));
        assertEquals(ErrorCode.NONE, syncResponse.header().errorCode());
        assertNotNull(((SyncGroupResponse) syncResponse.body()).assignment());

        assertEquals(ErrorCode.NONE, send(channel, ApiKeys.HEARTBEAT,
                new HeartbeatRequest("g1", join.memberId(), join.generationId())).header().errorCode());
        assertEquals(ErrorCode.NONE, send(channel, ApiKeys.OFFSET_COMMIT,
                new OffsetCommitRequest(
                        "g1", "orders", 0, 2, join.memberId(), join.generationId())).header().errorCode());

        MqDecoder.DecodedResponse offsetFetchResponse = send(channel, ApiKeys.OFFSET_FETCH,
                new OffsetFetchRequest("g1", "orders", 0));
        assertEquals(2, ((OffsetFetchResponse) offsetFetchResponse.body()).offset());

        channel.finishAndReleaseAll();
    }

    @Test
    void replicaFetchOnlyReportsFollowerConfirmedFetchOffset() {
        ClusterController clusterController = new ClusterController(new MetadataStore(tempDir));
        EmbeddedChannel channel = newChannel(clusterController, true);
        assertEquals(ErrorCode.NONE, send(channel, ApiKeys.REGISTER_BROKER,
                new RegisterBrokerRequest(0, "127.0.0.1", 9092)).header().errorCode());
        assertEquals(ErrorCode.NONE, send(channel, ApiKeys.REGISTER_BROKER,
                new RegisterBrokerRequest(1, "127.0.0.1", 9093)).header().errorCode());
        assertEquals(ErrorCode.NONE, send(channel, ApiKeys.CREATE_TOPIC,
                new CreateTopicRequest("orders", 1, 2)).header().errorCode());
        assertEquals(ErrorCode.NONE, send(channel, ApiKeys.PRODUCE,
                new ProduceRequest("orders", 0, List.of(
                        new ProduceRecord(null, new byte[] {1})))).header().errorCode());

        MqDecoder.DecodedResponse response = send(channel, ApiKeys.REPLICA_FETCH,
                new ReplicaFetchRequest("orders", 0, 1, 0L, 1024));

        assertEquals(ErrorCode.NONE, response.header().errorCode());
        assertEquals(1, ((ReplicaFetchResponse) response.body()).records().size());
        assertEquals(0L, clusterController.replicaLogEndOffset("orders", 0, 1));
        channel.finishAndReleaseAll();
    }

    @Test
    void produceAcksAllSucceedsOnSingleBrokerIsr() {
        ClusterController clusterController = new ClusterController(new MetadataStore(tempDir));
        clusterController.registerBroker(new RegisterBrokerRequest(0, "127.0.0.1", 9092));
        clusterController.createTopic(new CreateTopicRequest("acks-single", 1));
        EmbeddedChannel channel = newChannel(clusterController, true);

        MqDecoder.DecodedResponse response = send(
                channel,
                ApiKeys.PRODUCE,
                new ProduceRequest(
                        "acks-single",
                        0,
                        0L,
                        List.of(new ProduceRecord(null, new byte[] {1})),
                        ProduceAcks.ALL.code(),
                        50));

        assertEquals(ErrorCode.NONE, response.header().errorCode());
        assertEquals(0L, ((ProduceResponse) response.body()).baseOffset());
        channel.finishAndReleaseAll();
    }

    @Test
    void produceAcksAllTimesOutWhenFollowerStaysBehindAndKeepsLocalAppend() {
        ClusterController clusterController = new ClusterController(new MetadataStore(tempDir));
        clusterController.registerBroker(new RegisterBrokerRequest(0, "127.0.0.1", 9092));
        clusterController.registerBroker(new RegisterBrokerRequest(1, "127.0.0.1", 9093));
        clusterController.createTopic(new CreateTopicRequest("acks-timeout", 1, 2));
        EmbeddedChannel channel = newChannel(clusterController, true);

        MqDecoder.DecodedResponse timedOut = send(
                channel,
                ApiKeys.PRODUCE,
                new ProduceRequest(
                        "acks-timeout",
                        0,
                        0L,
                        List.of(new ProduceRecord(null, new byte[] {1})),
                        ProduceAcks.ALL.code(),
                        40));
        assertEquals(ErrorCode.REQUEST_TIMED_OUT, timedOut.header().errorCode());
        assertNull(timedOut.body());
        assertEquals(1L, clusterController.replicaLogEndOffset("acks-timeout", 0, 0));

        MqDecoder.DecodedResponse timedOutV2 = send(
                channel,
                ApiKeys.PRODUCE_V2,
                new ProduceV2Request(
                        "acks-timeout",
                        0,
                        0L,
                        new V4BoundedList<>(
                                1,
                                false,
                                List.of(new ScheduledProduceRecord(null, new byte[] {2}, 0L))),
                        ProduceAcks.ALL.code(),
                        40));
        assertEquals(ErrorCode.REQUEST_TIMED_OUT, timedOutV2.header().errorCode());
        assertNull(timedOutV2.body());
        assertEquals(2L, clusterController.replicaLogEndOffset("acks-timeout", 0, 0));
        channel.finishAndReleaseAll();
    }

    @Test
    void produceAcksAllReturnsNotEnoughReplicasWhenIsrIsEmpty() {
        ClusterController clusterController = new ClusterController(new MetadataStore(tempDir)) {
            @Override
            public synchronized List<Integer> getIsrBrokerIds(String topicName, int partitionId) {
                return List.of();
            }
        };
        clusterController.registerBroker(new RegisterBrokerRequest(0, "127.0.0.1", 9092));
        clusterController.createTopic(new CreateTopicRequest("acks-empty-isr", 1));
        EmbeddedChannel channel = newChannel(clusterController, true);

        MqDecoder.DecodedResponse response = send(
                channel,
                ApiKeys.PRODUCE,
                new ProduceRequest(
                        "acks-empty-isr",
                        0,
                        0L,
                        List.of(new ProduceRecord(null, new byte[] {1})),
                        ProduceAcks.ALL.code(),
                        50));

        assertEquals(ErrorCode.NOT_ENOUGH_REPLICAS, response.header().errorCode());
        assertNull(response.body());
        assertEquals(1L, clusterController.replicaLogEndOffset("acks-empty-isr", 0, 0));
        channel.finishAndReleaseAll();
    }

    @Test
    void produceAcksAllReturnsNotEnoughReplicasWhenIsrBelowMinInSync() {
        ClusterController clusterController = new ClusterController(new MetadataStore(tempDir)) {
            @Override
            public synchronized List<Integer> getIsrBrokerIds(String topicName, int partitionId) {
                return List.of(0);
            }
        };
        clusterController.registerBroker(new RegisterBrokerRequest(0, "127.0.0.1", 9092));
        clusterController.registerBroker(new RegisterBrokerRequest(1, "127.0.0.1", 9093));
        clusterController.createTopic(new CreateTopicRequest("acks-min-isr", 1, 2, 2));
        EmbeddedChannel channel = newChannel(clusterController, true);

        MqDecoder.DecodedResponse response = send(
                channel,
                ApiKeys.PRODUCE,
                new ProduceRequest(
                        "acks-min-isr",
                        0,
                        0L,
                        List.of(new ProduceRecord(null, new byte[] {1})),
                        ProduceAcks.ALL.code(),
                        50));

        assertEquals(ErrorCode.NOT_ENOUGH_REPLICAS, response.header().errorCode());
        assertNull(response.body());
        assertEquals(1L, clusterController.replicaLogEndOffset("acks-min-isr", 0, 0));
        channel.finishAndReleaseAll();
    }

    @Test
    void produceAcksAllDoesNotBlockEventLoopWhileWaiting() {
        ClusterController clusterController = new ClusterController(new MetadataStore(tempDir));
        clusterController.registerBroker(new RegisterBrokerRequest(0, "127.0.0.1", 9092));
        clusterController.registerBroker(new RegisterBrokerRequest(1, "127.0.0.1", 9093));
        clusterController.createTopic(new CreateTopicRequest("acks-async", 1, 2));
        EmbeddedChannel channel = newChannel(clusterController, true);

        channel.writeInbound(ProtocolSerdes.encodeRequest(
                new MqRequestHeader(ApiKeys.PRODUCE, 11, "test"),
                new ProduceRequest(
                        "acks-async",
                        0,
                        0L,
                        List.of(new ProduceRecord(null, new byte[] {1})),
                        ProduceAcks.ALL.code(),
                        300)));
        long startedNanos = System.nanoTime();
        MqDecoder.DecodedResponse metadata = send(
                channel, ApiKeys.METADATA, new MetadataRequest("acks-async"));
        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedNanos);
        assertEquals(ErrorCode.NONE, metadata.header().errorCode());
        assertTrue(elapsedMs < 200, "event loop blocked for " + elapsedMs + "ms");

        ByteBuf produceFrame = awaitOutbound(channel);
        try {
            MqDecoder.DecodedResponse timedOut =
                    MqDecoder.decodeResponse(unwrapPayload(produceFrame), ApiKeys.PRODUCE);
            assertEquals(ErrorCode.REQUEST_TIMED_OUT, timedOut.header().errorCode());
        } finally {
            produceFrame.release();
        }
        channel.finishAndReleaseAll();
    }

    @Test
    void leaveGroupRemovesMember() {
        ClusterController clusterController = new ClusterController(new MetadataStore(tempDir));
        clusterController.registerBroker(new RegisterBrokerRequest(0, "127.0.0.1", 9092));
        clusterController.createTopic(new CreateTopicRequest("orders", 1));
        EmbeddedChannel channel = newChannel(clusterController, true);

        MqDecoder.DecodedResponse join = send(
                channel, ApiKeys.JOIN_GROUP, new JoinGroupRequest("g1", "", "orders"));
        JoinGroupResponse joined = (JoinGroupResponse) join.body();
        assertEquals(ErrorCode.NONE, send(
                channel,
                ApiKeys.LEAVE_GROUP,
                new LeaveGroupRequest("g1", joined.memberId(), joined.generationId()))
                .header().errorCode());
        assertEquals(ErrorCode.GROUP_NOT_FOUND, send(
                channel,
                ApiKeys.HEARTBEAT,
                new HeartbeatRequest("g1", joined.memberId(), joined.generationId()))
                .header().errorCode());
        channel.finishAndReleaseAll();
    }

    @Test
    void produceAcksAllSucceedsWhenFollowerCatchesUp() throws Exception {
        Path dataDir = tempDir.resolve("acks-all-catch-up");
        BrokerConfig config = defaultConfig(dataDir, true);
        ClusterController controller = new ClusterController(new MetadataStore(dataDir.resolve("metadata")));
        assertEquals(ErrorCode.NONE,
                controller.registerBroker(new RegisterBrokerRequest(0, "127.0.0.1", 9092)).errorCode());
        assertEquals(ErrorCode.NONE,
                controller.registerBroker(new RegisterBrokerRequest(1, "127.0.0.1", 9093)).errorCode());
        assertEquals(ErrorCode.NONE, controller.createTopic(new CreateTopicRequest("acks-catch-up", 1, 2)));
        PartitionManager partitionManager = new PartitionManager(new LogManager(dataDir.resolve("log")));
        ReplicaStateManager replicaStateManager = new ReplicaStateManager();
        EmbeddedChannel channel = newChannel(
                config,
                controller,
                partitionManager,
                replicaStateManager,
                ControllerRole.legacy(config),
                new BrokerRuntimeMetrics());
        Thread catchUp = new Thread(() -> {
            for (int i = 0; i < 80; i++) {
                replicaStateManager.updateReplicaLeo("acks-catch-up", 0, 1, 100);
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(5));
            }
        }, "acks-all-catch-up");
        catchUp.setDaemon(true);
        catchUp.start();
        try {
            MqDecoder.DecodedResponse response = send(
                    channel,
                    ApiKeys.PRODUCE,
                    new ProduceRequest(
                            "acks-catch-up",
                            0,
                            0L,
                            List.of(new ProduceRecord(null, new byte[] {1})),
                            ProduceAcks.ALL.code(),
                            500));
            assertEquals(ErrorCode.NONE, response.header().errorCode());
            assertEquals(0L, ((ProduceResponse) response.body()).baseOffset());
        } finally {
            catchUp.join(1_000);
            channel.finishAndReleaseAll();
            partitionManager.close();
        }
    }

    @Test
    void produceToFollowerReturnsNotLeader() {
        MetadataStore store = new MetadataStore(tempDir);
        ClusterController clusterController = new ClusterController(store);
        clusterController.registerBroker(new RegisterBrokerRequest(0, "127.0.0.1", 9092));
        clusterController.createTopic(new CreateTopicRequest("orders", 1));
        EmbeddedChannel channel = newChannel(clusterController, true, 1);

        MqDecoder.DecodedResponse response = send(channel, ApiKeys.PRODUCE,
                new ProduceRequest("orders", 0, List.of(new ProduceRecord(null, new byte[] {1}))));

        assertEquals(ErrorCode.NOT_LEADER, response.header().errorCode());
        channel.finishAndReleaseAll();
    }

    @Test
    void maintenanceAdminApisReturnReportsAndMetrics() {
        ClusterController clusterController = new ClusterController(new MetadataStore(tempDir));
        EmbeddedChannel channel = newChannel(clusterController, true);

        MqDecoder.DecodedResponse reportResponse = send(
                channel, ApiKeys.MAINTENANCE_REPORT,
                new MaintenanceReportRequest(BrokerTestHarness.ADMIN_TOKEN));
        assertEquals(ErrorCode.NONE, reportResponse.header().errorCode());
        assertEquals(11, reportResponse.header().correlationId());
        MaintenanceReportResponse dryRun = (MaintenanceReportResponse) reportResponse.body();
        assertEquals(true, dryRun.dryRun());
        assertEquals("SKIPPED_DISABLED", dryRun.status());
        assertEquals(0, dryRun.segmentReports().size());

        MqDecoder.DecodedResponse runResponse = send(
                channel, ApiKeys.MAINTENANCE_RUN,
                new MaintenanceRunRequest(BrokerTestHarness.ADMIN_TOKEN));
        assertEquals(ErrorCode.NONE, runResponse.header().errorCode());
        MaintenanceReportResponse run = (MaintenanceReportResponse) runResponse.body();
        assertEquals(false, run.dryRun());
        assertEquals("SKIPPED_DISABLED", run.status());

        MqDecoder.DecodedResponse metricsResponse = send(
                channel, ApiKeys.MAINTENANCE_METRICS,
                new MaintenanceMetricsRequest(BrokerTestHarness.ADMIN_TOKEN));
        assertEquals(ErrorCode.NONE, metricsResponse.header().errorCode());
        MaintenanceMetricsResponse metrics = (MaintenanceMetricsResponse) metricsResponse.body();
        assertEquals(0L, metrics.runs());
        assertEquals(1L, metrics.dryRuns());
        assertEquals("SKIPPED_DISABLED", metrics.lastRunStatus());

        assertEquals(ErrorCode.NONE, send(
                channel, ApiKeys.METADATA, new MetadataRequest("")).header().errorCode());
        MqDecoder.DecodedResponse runtimeMetricsResponse = send(
                channel, ApiKeys.RUNTIME_METRICS,
                new RuntimeMetricsRequest(BrokerTestHarness.ADMIN_TOKEN));
        assertEquals(ErrorCode.NONE, runtimeMetricsResponse.header().errorCode());
        RuntimeMetricsResponse runtimeMetrics = (RuntimeMetricsResponse) runtimeMetricsResponse.body();
        assertTrue(runtimeMetrics.requestsByApiKey().stream()
                .anyMatch(count -> count.apiKey() == ApiKeys.METADATA && count.count() >= 1L));
        assertTrue(runtimeMetrics.responsesByErrorCode().stream()
                .anyMatch(count -> count.errorCode() == ErrorCode.NONE.code && count.count() >= 1L));
        assertTrue(runtimeMetrics.latencyByApiKey().stream()
                .anyMatch(summary -> summary.apiKey() == ApiKeys.METADATA && summary.count() >= 1L));
        channel.finishAndReleaseAll();
    }

    @Test
    void controllerAcceptsReplicaStateV2WithClusterCredential() {
        ClusterController clusterController = new ClusterController(new MetadataStore(tempDir));
        clusterController.registerBroker(new RegisterBrokerRequest(0, "127.0.0.1", 9092));
        clusterController.createTopic(new CreateTopicRequest("orders", 1));
        EmbeddedChannel channel = newChannel(clusterController, true);

        MqDecoder.DecodedResponse accepted = send(
                channel,
                ApiKeys.UPDATE_REPLICA_STATE_V2,
                new UpdateReplicaStateV2Request(
                        BrokerTestHarness.ADMIN_TOKEN,
                        "orders",
                        0,
                        0,
                        1L,
                        0L,
                        0L,
                        9L,
                        7L,
                        1_000L));

        assertEquals(ErrorCode.NONE, accepted.header().errorCode());
        assertTrue(accepted.body() instanceof UpdateReplicaStateV2Response);
        assertEquals(
                7L,
                clusterController.snapshot().topics.get("orders").partitions.get(0)
                        .replicaProgress.get(0).visibleLogEndOffset);

        MqDecoder.DecodedResponse denied = send(
                channel,
                ApiKeys.UPDATE_REPLICA_STATE_V2,
                new UpdateReplicaStateV2Request(
                        "wrong-cluster-token",
                        "orders",
                        0,
                        0,
                        1L,
                        0L,
                        0L,
                        9L,
                        9L,
                        -1L));
        assertEquals(ErrorCode.UNAUTHORIZED, denied.header().errorCode());
        channel.finishAndReleaseAll();
    }

    @Test
    void replicaFetchV2FencesStaleBrokerEpochBeforeReturningRecords() {
        Path dataDir = tempDir.resolve("replica-fetch-v2-fenced");
        BrokerConfig config = defaultConfig(dataDir, true);
        ClusterController controller =
                new ClusterController(new MetadataStore(dataDir.resolve("metadata")));
        assertEquals(ErrorCode.NONE,
                controller.registerBroker(new RegisterBrokerRequest(0, "127.0.0.1", 9092)).errorCode());
        assertEquals(ErrorCode.NONE,
                controller.registerBroker(new RegisterBrokerRequest(1, "127.0.0.1", 9093)).errorCode());
        assertEquals(ErrorCode.NONE,
                controller.createTopic(new CreateTopicRequest("orders", 1, 2)));
        PartitionManager partitionManager = new PartitionManager(new LogManager(dataDir.resolve("log")));
        ReplicaStateManager replicaStateManager = new ReplicaStateManager();
        EmbeddedChannel channel = newChannel(
                config,
                controller,
                partitionManager,
                replicaStateManager,
                ControllerRole.legacy(config),
                new BrokerRuntimeMetrics());

        try {
            MqDecoder.DecodedResponse response = send(
                    channel,
                    ApiKeys.REPLICA_FETCH_V2,
                    new ReplicaFetchV2Request(
                            BrokerTestHarness.ADMIN_TOKEN,
                            "orders",
                            0,
                            1,
                            0L,
                            0L,
                            0L,
                            9L,
                            1_024));

            assertEquals(ErrorCode.BROKER_FENCED, response.header().errorCode());
            assertNull(response.body());
            assertEquals(0L, replicaStateManager.replicaLeo("orders", 0, 1));
        } finally {
            channel.finishAndReleaseAll();
            partitionManager.close();
        }
    }

    @Test
    void replicaFetchV2ChecksBrokerIdentityBeforeTopicRouting() {
        Path dataDir = tempDir.resolve("replica-fetch-v2-identity-precedence");
        BrokerConfig config = defaultConfig(dataDir, true);
        ClusterController controller =
                new ClusterController(new MetadataStore(dataDir.resolve("metadata")));
        assertEquals(ErrorCode.NONE,
                controller.registerBroker(new RegisterBrokerRequest(0, "127.0.0.1", 9092)).errorCode());
        PartitionManager partitionManager = new PartitionManager(new LogManager(dataDir.resolve("log")));
        EmbeddedChannel channel = newChannel(
                config,
                controller,
                partitionManager,
                new ReplicaStateManager(),
                ControllerRole.legacy(config),
                new BrokerRuntimeMetrics());

        try {
            MqDecoder.DecodedResponse response = send(
                    channel,
                    ApiKeys.REPLICA_FETCH_V2,
                    new ReplicaFetchV2Request(
                            BrokerTestHarness.ADMIN_TOKEN,
                            "missing",
                            0,
                            99,
                            1L,
                            0L,
                            0L,
                            0L,
                            1_024));

            assertEquals(ErrorCode.UNKNOWN_BROKER, response.header().errorCode());
            assertNull(response.body());
        } finally {
            channel.finishAndReleaseAll();
            partitionManager.close();
        }
    }

    @Test
    void replicaFetchV2ChecksBrokerIdentityBeforeTombstoneState() {
        Path dataDir = tempDir.resolve("replica-fetch-v2-tombstone-precedence");
        BrokerConfig config = defaultConfig(dataDir, true);
        ClusterController controller =
                new ClusterController(new MetadataStore(dataDir.resolve("metadata")));
        assertEquals(ErrorCode.NONE,
                controller.registerBroker(new RegisterBrokerRequest(0, "127.0.0.1", 9092)).errorCode());
        assertEquals(ErrorCode.NONE, controller.createTopic(new CreateTopicRequest("orders", 1)));
        assertEquals(ErrorCode.NONE, controller.deleteTopic("orders", 1_000L));
        PartitionManager partitionManager = new PartitionManager(new LogManager(dataDir.resolve("log")));
        EmbeddedChannel channel = newChannel(
                config,
                controller,
                partitionManager,
                new ReplicaStateManager(),
                ControllerRole.legacy(config),
                new BrokerRuntimeMetrics());

        try {
            MqDecoder.DecodedResponse response = send(
                    channel,
                    ApiKeys.REPLICA_FETCH_V2,
                    new ReplicaFetchV2Request(
                            BrokerTestHarness.ADMIN_TOKEN,
                            "orders",
                            0,
                            99,
                            1L,
                            0L,
                            0L,
                            0L,
                            1_024));

            assertEquals(ErrorCode.UNKNOWN_BROKER, response.header().errorCode());
            assertNull(response.body());
        } finally {
            channel.finishAndReleaseAll();
            partitionManager.close();
        }
    }

    @Test
    void fetchV2CapsNullPayloadRecordsAndReportsCandidateTotal() {
        Path dataDir = tempDir.resolve("fetch-v2-cap");
        BrokerConfig config = defaultConfig(dataDir, true);
        ClusterController controller = new ClusterController(new MetadataStore(dataDir.resolve("metadata")));
        assertEquals(ErrorCode.NONE,
                controller.registerBroker(new RegisterBrokerRequest(0, "127.0.0.1", 9092)).errorCode());
        assertEquals(ErrorCode.NONE, controller.createTopic(new CreateTopicRequest("orders", 1)));
        PartitionManager partitionManager = new PartitionManager(new LogManager(dataDir.resolve("log")));
        for (int i = 0; i < 10_001; i++) {
            partitionManager.append("orders", 0, null, null);
        }
        EmbeddedChannel channel = newChannel(
                config,
                controller,
                partitionManager,
                new ReplicaStateManager(),
                ControllerRole.legacy(config),
                new BrokerRuntimeMetrics());

        try {
            MqDecoder.DecodedResponse decoded = send(
                    channel,
                    ApiKeys.FETCH_V2,
                    new FetchV2Request("orders", 0, 0L, 0L, 10_001 * 16));
            FetchV2Response response = (FetchV2Response) decoded.body();

            assertEquals(ErrorCode.NONE, decoded.header().errorCode());
            assertEquals(10_001, response.records().total());
            assertTrue(response.records().truncated());
            assertEquals(10_000, response.records().items().size());
            assertEquals(9_999L, response.records().items().get(9_999).offset());
        } finally {
            channel.finishAndReleaseAll();
            partitionManager.close();
        }
    }

    @Test
    void fetchV2FailedWriteDoesNotRecordReturnedPayloadBytes() {
        Path dataDir = tempDir.resolve("fetch-v2-failed-write");
        BrokerConfig config = defaultConfig(dataDir, true);
        ClusterController controller = new ClusterController(new MetadataStore(dataDir.resolve("metadata")));
        assertEquals(ErrorCode.NONE,
                controller.registerBroker(new RegisterBrokerRequest(0, "127.0.0.1", 9092)).errorCode());
        assertEquals(ErrorCode.NONE, controller.createTopic(new CreateTopicRequest("orders", 1)));
        PartitionManager partitionManager = new PartitionManager(new LogManager(dataDir.resolve("log")));
        partitionManager.append("orders", 0, "key".getBytes(), "value".getBytes());
        BrokerRuntimeMetrics runtimeMetrics = new BrokerRuntimeMetrics();
        EmbeddedChannel channel = newChannel(
                config,
                controller,
                partitionManager,
                new ReplicaStateManager(),
                ControllerRole.legacy(config),
                runtimeMetrics);
        channel.pipeline().addBefore(
                channel.pipeline().context(MqRequestHandler.class).name(),
                "failing-outbound-write",
                new ChannelOutboundHandlerAdapter() {
                    @Override
                    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
                        ReferenceCountUtil.release(msg);
                        promise.setFailure(new IOException("simulated outbound write failure"));
                    }
                });

        try {
            channel.writeInbound(ProtocolSerdes.encodeRequest(
                    new MqRequestHeader(ApiKeys.FETCH_V2, 11, "test"),
                    new FetchV2Request("orders", 0, 0L, 0L, 1_024)));
            channel.runPendingTasks();

            assertNull(channel.readOutbound());
            assertEquals(0L, runtimeMetrics.snapshot().fetchBytes());
        } finally {
            channel.finishAndReleaseAll();
            partitionManager.close();
        }
    }

    @Test
    void fetchV2EncodingFailureDoesNotRecordReturnedPayloadBytes() {
        Path dataDir = tempDir.resolve("fetch-v2-encoding-failure");
        BrokerConfig config = defaultConfig(dataDir, true);
        ClusterController controller = new ClusterController(new MetadataStore(dataDir.resolve("metadata")));
        assertEquals(ErrorCode.NONE,
                controller.registerBroker(new RegisterBrokerRequest(0, "127.0.0.1", 9092)).errorCode());
        assertEquals(ErrorCode.NONE, controller.createTopic(new CreateTopicRequest("orders", 1)));
        PartitionManager partitionManager = new PartitionManager(new LogManager(dataDir.resolve("log")));
        partitionManager.append("orders", 0, null, new byte[MqFrame.MAX_FRAME_BYTES - 64]);
        BrokerRuntimeMetrics runtimeMetrics = new BrokerRuntimeMetrics();
        EmbeddedChannel channel = newChannel(
                config,
                controller,
                partitionManager,
                new ReplicaStateManager(),
                ControllerRole.legacy(config),
                runtimeMetrics);

        try {
            channel.writeInbound(ProtocolSerdes.encodeRequest(
                    new MqRequestHeader(ApiKeys.FETCH_V2, 11, "test"),
                    new FetchV2Request("orders", 0, 0L, 0L, MqFrame.MAX_FRAME_BYTES)));
            channel.runPendingTasks();

            assertFalse(channel.isActive());
            assertNull(channel.readOutbound());
            assertEquals(0L, runtimeMetrics.snapshot().fetchBytes());
        } finally {
            channel.finishAndReleaseAll();
            partitionManager.close();
        }
    }

    @Test
    void replicaFetchV2CapsCandidatesAndUpdatesFollowerLeoAfterFencing() {
        Path dataDir = tempDir.resolve("replica-fetch-v2-cap");
        BrokerConfig config = defaultConfig(dataDir, true);
        ClusterController controller = new ClusterController(new MetadataStore(dataDir.resolve("metadata")));
        assertEquals(ErrorCode.NONE,
                controller.registerBroker(new RegisterBrokerRequest(0, "127.0.0.1", 9092)).errorCode());
        assertEquals(ErrorCode.NONE,
                controller.registerBroker(new RegisterBrokerRequest(1, "127.0.0.1", 9093)).errorCode());
        assertEquals(ErrorCode.NONE, controller.createTopic(new CreateTopicRequest("orders", 1, 2)));
        PartitionManager partitionManager = new PartitionManager(new LogManager(dataDir.resolve("log")));
        for (int i = 0; i < 10_002; i++) {
            partitionManager.append("orders", 0, null, null);
        }
        ReplicaStateManager replicaStateManager = new ReplicaStateManager();
        EmbeddedChannel channel = newChannel(
                config,
                controller,
                partitionManager,
                replicaStateManager,
                ControllerRole.legacy(config),
                new BrokerRuntimeMetrics());

        try {
            MqDecoder.DecodedResponse decoded = send(
                    channel,
                    ApiKeys.REPLICA_FETCH_V2,
                    new ReplicaFetchV2Request(
                            BrokerTestHarness.ADMIN_TOKEN,
                            "orders",
                            0,
                            1,
                            controller.getBroker(1).brokerEpoch,
                            controller.snapshot().controllerEpoch,
                            controller.getTopic("orders").partitions.get(0).leaderEpoch,
                            1L,
                            10_001 * 16));
            ReplicaFetchV2Response response = (ReplicaFetchV2Response) decoded.body();

            assertEquals(ErrorCode.NONE, decoded.header().errorCode());
            assertEquals(1L, replicaStateManager.replicaLeo("orders", 0, 1));
            assertEquals(10_001, response.records().total());
            assertTrue(response.records().truncated());
            assertEquals(10_000, response.records().items().size());
            assertEquals(10_000L, response.records().items().get(9_999).offset());
        } finally {
            channel.finishAndReleaseAll();
            partitionManager.close();
        }
    }

    @Test
    void metadataV2ReadsCurrentControllerOncePerResponse() {
        Path dataDir = tempDir.resolve("metadata-v2-controller-snapshot");
        BrokerConfig config = defaultConfig(dataDir, true);
        ClusterController controller = new ClusterController(new MetadataStore(dataDir.resolve("metadata")));
        AtomicInteger currentLeaderCalls = new AtomicInteger();
        ControllerRole controllerRole = new ControllerRole() {
            @Override
            public boolean isActive() {
                return true;
            }

            @Override
            public ControllerIdentity currentLeader() {
                currentLeaderCalls.incrementAndGet();
                return new ControllerIdentity(0, "127.0.0.1", 9092, 0L, "test-session");
            }
        };
        PartitionManager partitionManager = new PartitionManager(new LogManager(dataDir.resolve("log")));
        EmbeddedChannel channel = newChannel(
                config,
                controller,
                partitionManager,
                new ReplicaStateManager(),
                controllerRole,
                new BrokerRuntimeMetrics());

        try {
            currentLeaderCalls.set(0);
            MqDecoder.DecodedResponse decoded = send(
                    channel, ApiKeys.METADATA_V2, new MetadataV2Request(null, 10, 10));
            MetadataV2Response response = (MetadataV2Response) decoded.body();

            assertEquals(ErrorCode.NONE, decoded.header().errorCode());
            assertNotNull(response.controller());
            assertEquals(1, currentLeaderCalls.get());
        } finally {
            channel.finishAndReleaseAll();
            partitionManager.close();
        }
    }

    @Test
    void metadataV2IncludesTopicTombstonesAfterControllerDelete() {
        ClusterController clusterController = new ClusterController(new MetadataStore(tempDir));
        assertEquals(ErrorCode.NONE, clusterController.registerBroker(
                new RegisterBrokerRequest(0, "127.0.0.1", 9092)).errorCode());
        assertEquals(ErrorCode.NONE, clusterController.createTopic(new CreateTopicRequest("orders", 2, 1)));
        assertEquals(ErrorCode.NONE, clusterController.deleteTopic("orders", 1_234L));
        EmbeddedChannel channel = newChannel(clusterController, true);

        MqDecoder.DecodedResponse response = send(
                channel, ApiKeys.METADATA_V2, new MetadataV2Request(null, 10, 10));

        assertEquals(ErrorCode.NONE, response.header().errorCode());
        MetadataV2Response body = (MetadataV2Response) response.body();
        assertEquals(0, body.topics().items().size());
        assertEquals(1, body.tombstones().total());
        assertEquals("orders", body.tombstones().items().get(0).topicName());
        assertEquals("DELETING", body.tombstones().items().get(0).tombstone().state());
        assertEquals(List.of(0), body.tombstones().items().get(0).tombstone().pendingBrokerIds().items());
        channel.finishAndReleaseAll();
    }

    @Test
    void metadataV2AppliesIndependentTopicAndTombstoneCursors() {
        ClusterController clusterController = new ClusterController(new MetadataStore(tempDir));
        assertEquals(ErrorCode.NONE, clusterController.registerBroker(
                new RegisterBrokerRequest(0, "127.0.0.1", 9092)).errorCode());
        for (String topicName : List.of("active-a", "active-b", "active-c")) {
            assertEquals(ErrorCode.NONE, clusterController.createTopic(new CreateTopicRequest(topicName, 1, 1)));
        }
        for (String topicName : List.of("deleted-a", "deleted-b", "deleted-c")) {
            assertEquals(ErrorCode.NONE, clusterController.createTopic(new CreateTopicRequest(topicName, 1, 1)));
            assertEquals(ErrorCode.NONE, clusterController.deleteTopic(topicName, 1_234L));
        }
        EmbeddedChannel channel = newChannel(clusterController, true);

        try {
            MqDecoder.DecodedResponse response = send(
                    channel,
                    ApiKeys.METADATA_V2,
                    new MetadataV2Request(null, "active-a", "deleted-a", 10, 10));

            assertEquals(ErrorCode.NONE, response.header().errorCode());
            MetadataV2Response body = (MetadataV2Response) response.body();
            assertEquals(2, body.topics().total());
            assertEquals(List.of("active-b", "active-c"),
                    body.topics().items().stream().map(topic -> topic.topicName()).toList());
            assertEquals(2, body.tombstones().total());
            assertEquals(List.of("deleted-b", "deleted-c"),
                    body.tombstones().items().stream().map(tombstone -> tombstone.topicName()).toList());
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    void operatorAdminStatusAndTopicApisReturnSuccessfulResponses() {
        ClusterController clusterController = new ClusterController(new MetadataStore(tempDir));
        assertEquals(ErrorCode.NONE, clusterController.registerBroker(
                new RegisterBrokerRequest(0, "127.0.0.1", 9092)).errorCode());
        EmbeddedChannel channel = newChannel(clusterController, true);

        MqDecoder.DecodedResponse create = send(
                channel,
                ApiKeys.CREATE_TOPIC_ADMIN,
                new CreateTopicAdminRequest(BrokerTestHarness.ADMIN_TOKEN, "orders", 2, 1));
        assertEquals(ErrorCode.NONE, create.header().errorCode());
        assertEquals("orders", ((CreateTopicAdminResponse) create.body()).topicName());

        MqDecoder.DecodedResponse list = send(
                channel,
                ApiKeys.LIST_TOPICS,
                new ListTopicsRequest(BrokerTestHarness.ADMIN_TOKEN, 10));
        assertEquals(ErrorCode.NONE, list.header().errorCode());
        assertEquals(List.of("orders"), ((ListTopicsResponse) list.body()).topicNames().items());

        MqDecoder.DecodedResponse describe = send(
                channel,
                ApiKeys.DESCRIBE_TOPIC,
                new DescribeTopicRequest(BrokerTestHarness.ADMIN_TOKEN, "orders", 10, false));
        assertEquals(ErrorCode.NONE, describe.header().errorCode());
        DescribeTopicResponse described = (DescribeTopicResponse) describe.body();
        assertTrue(described.active());
        assertEquals(2, described.partitions().total());

        MqDecoder.DecodedResponse increase = send(
                channel,
                ApiKeys.INCREASE_PARTITIONS,
                new IncreasePartitionsRequest(BrokerTestHarness.ADMIN_TOKEN, "orders", 3));
        assertEquals(ErrorCode.NONE, increase.header().errorCode());
        IncreasePartitionsResponse increased = (IncreasePartitionsResponse) increase.body();
        assertEquals(2, increased.previousPartitionCount());
        assertEquals(3, increased.newPartitionCount());

        MqDecoder.DecodedResponse ops = send(
                channel,
                ApiKeys.OPS_STATUS,
                new OpsStatusRequest(BrokerTestHarness.ADMIN_TOKEN, 10, 10));
        assertEquals(ErrorCode.NONE, ops.header().errorCode());
        OpsStatusResponse opsBody = (OpsStatusResponse) ops.body();
        assertEquals(0, opsBody.targetBrokerId());
        assertEquals("CONTROLLER", opsBody.role());
        assertTrue(opsBody.brokers().items().stream().anyMatch(broker -> broker.brokerId() == 0));

        MqDecoder.DecodedResponse join = send(
                channel,
                ApiKeys.JOIN_GROUP,
                new JoinGroupRequest("g1", "member-a", "orders"));
        assertEquals(ErrorCode.NONE, join.header().errorCode());

        MqDecoder.DecodedResponse group = send(
                channel,
                ApiKeys.GROUP_STATUS,
                new GroupStatusRequest(BrokerTestHarness.ADMIN_TOKEN, "g1", null, 10, 10));
        assertEquals(ErrorCode.NONE, group.header().errorCode());
        GroupStatusResponse groupBody = (GroupStatusResponse) group.body();
        assertEquals("g1", groupBody.groupId());
        assertEquals("orders", groupBody.topicName());
        assertEquals(1, groupBody.members().total());

        channel.finishAndReleaseAll();
    }

    @Test
    void fetchV1PublishesVisibleHighWatermarkWhenRecordsAreDelayed() {
        Path dataDir = tempDir.resolve("fetch-v1-visible-hw");
        BrokerConfig config = defaultConfig(dataDir, true);
        ClusterController controller = new ClusterController(new MetadataStore(dataDir.resolve("metadata")));
        assertEquals(ErrorCode.NONE,
                controller.registerBroker(new RegisterBrokerRequest(0, "127.0.0.1", 9092)).errorCode());
        assertEquals(ErrorCode.NONE, controller.createTopic(new CreateTopicRequest("orders", 1)));
        PartitionManager partitionManager = new PartitionManager(new LogManager(dataDir.resolve("log")));
        EmbeddedChannel channel = newChannel(
                config,
                controller,
                partitionManager,
                new ReplicaStateManager(),
                ControllerRole.legacy(config),
                new BrokerRuntimeMetrics());

        try {
            long deliverAtEpochMs = System.currentTimeMillis() + 60_000L;
            MqDecoder.DecodedResponse produced = send(
                    channel,
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
                                                    "ready".getBytes(), "now".getBytes(), 0L),
                                            new ScheduledProduceRecord(
                                                    "later".getBytes(), "wait".getBytes(), deliverAtEpochMs)))));
            assertEquals(ErrorCode.NONE, produced.header().errorCode());

            MqDecoder.DecodedResponse decoded = send(
                    channel,
                    ApiKeys.FETCH,
                    new FetchRequest("orders", 0, 0, 1_024));
            FetchResponse response = (FetchResponse) decoded.body();

            assertEquals(ErrorCode.NONE, decoded.header().errorCode());
            assertEquals(1L, response.highWatermark());
            assertEquals(1, response.records().size());
            assertEquals(0L, response.records().get(0).offset());
        } finally {
            channel.finishAndReleaseAll();
            partitionManager.close();
        }
    }

    @Test
    void produceV2CoversSuccessTruncationAndDeletedTopicPaths() {
        Path dataDir = tempDir.resolve("produce-v2-paths");
        BrokerConfig config = defaultConfig(dataDir, true);
        ClusterController controller = new ClusterController(
                new MetadataStore(dataDir.resolve("metadata")));
        assertEquals(ErrorCode.NONE, controller.registerBroker(
                new RegisterBrokerRequest(0, "127.0.0.1", 9092)).errorCode());
        assertEquals(ErrorCode.NONE, controller.createTopic(new CreateTopicRequest("orders", 1)));
        PartitionManager partitionManager =
                new PartitionManager(new LogManager(dataDir.resolve("log")));
        BrokerRuntimeMetrics runtimeMetrics = new BrokerRuntimeMetrics();
        EmbeddedChannel channel = newChannel(
                config,
                controller,
                partitionManager,
                new ReplicaStateManager(),
                ControllerRole.legacy(config),
                runtimeMetrics);

        try {
            MqDecoder.DecodedResponse success = send(
                    channel,
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
                                                    "k1".getBytes(), "v1".getBytes(), 0L),
                                            new ScheduledProduceRecord(
                                                    null, "v2".getBytes(), 1L)))));
            assertEquals(ErrorCode.NONE, success.header().errorCode());
            assertEquals(0L, ((ProduceV2Response) success.body()).baseOffset());
            assertEquals(6L, runtimeMetrics.snapshot().produceBytes());

            MqDecoder.DecodedResponse truncated = send(
                    channel,
                    ApiKeys.PRODUCE_V2,
                    new ProduceV2Request(
                            "orders",
                            0,
                            0L,
                            new V4BoundedList<>(
                                    2,
                                    true,
                                    List.of(new ScheduledProduceRecord(null, null, 0L)))));
            assertEquals(ErrorCode.INVALID_REQUEST, truncated.header().errorCode());

            assertEquals(ErrorCode.NONE, controller.deleteTopic("orders", 123L));
            MqDecoder.DecodedResponse deleted = send(
                    channel,
                    ApiKeys.PRODUCE_V2,
                    new ProduceV2Request(
                            "orders",
                            0,
                            0L,
                            new V4BoundedList<>(0, false, List.of())));
            assertEquals(ErrorCode.UNKNOWN_TOPIC, deleted.header().errorCode());
        } finally {
            channel.finishAndReleaseAll();
            partitionManager.close();
        }
    }

    @Test
    void tombstoneFilterAndDeleteAckUseControllerPaths() {
        Path dataDir = tempDir.resolve("tombstone-filter-ack");
        BrokerConfig config = withLegacyApis(defaultConfig(dataDir, true));
        ClusterController controller = new ClusterController(
                new MetadataStore(dataDir.resolve("metadata")));
        assertEquals(ErrorCode.NONE, controller.registerBroker(
                new RegisterBrokerRequest(0, "127.0.0.1", 9092)).errorCode());
        assertEquals(ErrorCode.NONE, controller.createTopic(new CreateTopicRequest("orders", 1)));
        assertEquals(ErrorCode.NONE, controller.deleteTopic("orders", 123L));
        ClusterMetadata.TopicTombstone tombstone = controller.getTopicTombstone("orders");
        ClusterMetadata.BrokerEntry broker = controller.getBroker(0);
        EmbeddedChannel channel = newChannel(config, controller);

        MqDecoder.DecodedResponse filtered = send(
                channel,
                ApiKeys.METADATA_V2,
                new MetadataV2Request("orders", 10, 10));
        assertEquals(ErrorCode.UNKNOWN_TOPIC, filtered.header().errorCode());

        MqDecoder.DecodedResponse acknowledged = send(
                channel,
                ApiKeys.TOPIC_DELETE_ACK,
                new TopicDeleteAckRequest(
                        config.clusterToken(),
                        "orders",
                        tombstone.tombstoneVersion,
                        0,
                        broker.brokerEpoch,
                        controller.snapshot().controllerEpoch));
        assertEquals(ErrorCode.NONE, acknowledged.header().errorCode());
        assertTrue(acknowledged.body() instanceof TopicDeleteAckResponse);
        assertEquals("DELETED", controller.getTopicTombstone("orders").state);

        channel.finishAndReleaseAll();
    }

    @Test
    void authStatusReturnsCanonicalGrantedScopes() {
        EmbeddedChannel channel = newChannel(
                new ClusterController(new MetadataStore(tempDir.resolve("auth-status"))),
                true);

        MqDecoder.DecodedResponse decoded = send(
                channel,
                ApiKeys.AUTH_STATUS,
                new AuthStatusRequest(BrokerTestHarness.ADMIN_TOKEN));

        assertEquals(ErrorCode.NONE, decoded.header().errorCode());
        AuthStatusResponse response = (AuthStatusResponse) decoded.body();
        assertEquals(0, response.targetBrokerId());
        assertFalse(response.scopes().items().isEmpty());
        assertEquals(response.scopes().items().size(), response.scopes().total());
        channel.finishAndReleaseAll();
    }

    @Test
    void configureTopicReliabilityRoutesThroughScopedAdminApi() {
        ClusterController clusterController = new ClusterController(new MetadataStore(tempDir));
        clusterController.registerBroker(new RegisterBrokerRequest(0, "127.0.0.1", 9092));
        clusterController.createTopic(new CreateTopicRequest("orders", 1));
        clusterController.createTopic(new CreateTopicRequest("orders.retry", 1));
        EmbeddedChannel channel = newChannel(clusterController, true);

        MqDecoder.DecodedResponse response = send(
                channel,
                ApiKeys.CONFIGURE_TOPIC_RELIABILITY,
                new ConfigureTopicReliabilityRequest(
                        BrokerTestHarness.ADMIN_TOKEN,
                        "orders",
                        true,
                        "orders.retry",
                        false,
                        false,
                        null,
                        false));

        assertEquals(ErrorCode.NONE, response.header().errorCode());
        assertTrue(response.body() instanceof ConfigureTopicReliabilityResponse);
        ConfigureTopicReliabilityResponse body =
                (ConfigureTopicReliabilityResponse) response.body();
        assertEquals("orders.retry", body.retryTopicName());
        assertEquals("orders.retry",
                clusterController.snapshot().retryTopicBySource.get("orders"));
        channel.finishAndReleaseAll();
    }

    @Test
    void storageDegradedMatrixRejectsMutationsButAllowsReadsAndMetrics() {
        ClusterController clusterController = new ClusterController(new MetadataStore(tempDir));
        BrokerAvailabilityGuard guard = new BrokerAvailabilityGuard();
        guard.activate();
        EmbeddedChannel channel = newChannel(clusterController, true, 0, guard);

        assertEquals(ErrorCode.NONE, send(channel, ApiKeys.REGISTER_BROKER,
                new RegisterBrokerRequest(0, "127.0.0.1", 9092)).header().errorCode());
        assertEquals(ErrorCode.NONE, send(channel, ApiKeys.CREATE_TOPIC,
                new CreateTopicRequest("orders", 1)).header().errorCode());
        assertEquals(ErrorCode.NONE, send(channel, ApiKeys.PRODUCE,
                new ProduceRequest("orders", 0,
                        List.of(new ProduceRecord(null, new byte[] {1})))).header().errorCode());

        guard.degradeStorage();

        assertEquals(ErrorCode.MAINTENANCE_BUSY, send(channel, ApiKeys.PRODUCE,
                new ProduceRequest("orders", 0,
                        List.of(new ProduceRecord(null, new byte[] {2})))).header().errorCode());
        assertEquals(ErrorCode.NONE, send(channel, ApiKeys.FETCH,
                new FetchRequest("orders", 0, 0L, 1024)).header().errorCode());
        assertEquals(ErrorCode.NONE, send(channel, ApiKeys.METADATA,
                new MetadataRequest("orders")).header().errorCode());
        assertEquals(ErrorCode.NONE, send(channel, ApiKeys.MAINTENANCE_METRICS,
                new MaintenanceMetricsRequest(BrokerTestHarness.ADMIN_TOKEN)).header().errorCode());
        assertEquals(ErrorCode.NONE, send(channel, ApiKeys.RUNTIME_METRICS,
                new RuntimeMetricsRequest(BrokerTestHarness.ADMIN_TOKEN)).header().errorCode());
        assertEquals(ErrorCode.MAINTENANCE_BUSY, send(channel, ApiKeys.MAINTENANCE_RUN,
                new MaintenanceRunRequest(BrokerTestHarness.ADMIN_TOKEN)).header().errorCode());

        channel.finishAndReleaseAll();
    }

    @Test
    void throttledRequestsReturnThrottledAndUpdateRuntimeMetrics() {
        ClusterController clusterController = new ClusterController(new MetadataStore(tempDir));
        BrokerAvailabilityGuard guard = new BrokerAvailabilityGuard();
        guard.activate();
        BrokerRuntimeMetrics runtimeMetrics = new BrokerRuntimeMetrics();
        RequestThrottler requestThrottler = new RequestThrottler(1);
        ThrottleResult heldSlot = requestThrottler.tryAcquire(ApiKeys.PRODUCE);
        EmbeddedChannel channel = newChannel(clusterController, true, 0, guard, runtimeMetrics, requestThrottler);

        try {
            MqDecoder.DecodedResponse response = send(channel, ApiKeys.METADATA, new MetadataRequest(""));

            assertEquals(ErrorCode.THROTTLED, response.header().errorCode());
            BrokerRuntimeMetricsSnapshot snapshot = runtimeMetrics.snapshot();
            assertEquals(1L, snapshot.throttledByApiKey().get(ApiKeys.METADATA));
            assertEquals(1L, snapshot.responsesByErrorCode().get(ErrorCode.THROTTLED));
            assertEquals(1L, snapshot.requestsByApiKey().get(ApiKeys.METADATA));
        } finally {
            heldSlot.release();
            channel.finishAndReleaseAll();
        }
    }

    @Test
    void runtimeMetricsV2IncludesBoundedPartitionLagSnapshot() {
        ClusterController clusterController = new ClusterController(new MetadataStore(tempDir));
        assertEquals(ErrorCode.NONE, clusterController.registerBroker(
                new RegisterBrokerRequest(0, "127.0.0.1", 9092)).errorCode());
        assertEquals(ErrorCode.NONE, clusterController.createTopic(new CreateTopicRequest("orders", 2, 1)));
        assertEquals(ErrorCode.NONE, clusterController.updateReplicaStateV2(
                "orders", 0, 0, 1L, 0L, 0L, 10L, 7L, 9_000L));
        assertEquals(ErrorCode.NONE, clusterController.updateReplicaStateV2(
                "orders", 1, 0, 1L, 0L, 0L, 20L, 11L, 10_000L));
        GroupCoordinator coordinator = new GroupCoordinator(clusterController);
        var join = coordinator.joinGroup(new JoinGroupRequest("g1", "member-a", "orders"));
        coordinator.offsetCommit(new OffsetCommitRequest(
                "g1", "orders", 0, 4L, join.memberId(), join.generationId()));
        coordinator.offsetCommit(new OffsetCommitRequest(
                "g1", "orders", 1, 12L, join.memberId(), join.generationId()));
        EmbeddedChannel channel = newChannel(clusterController, true);

        MqDecoder.DecodedResponse response = send(
                channel,
                ApiKeys.RUNTIME_METRICS_V2,
                new RuntimeMetricsV2Request(BrokerTestHarness.ADMIN_TOKEN, 10, 10, 1));

        assertEquals(ErrorCode.NONE, response.header().errorCode());
        RuntimeMetricsV2Response metrics = (RuntimeMetricsV2Response) response.body();
        assertEquals(2, metrics.lagMetrics().total());
        assertTrue(metrics.lagMetrics().truncated());
        assertEquals(1, metrics.lagMetrics().items().size());
        assertEquals("orders", metrics.lagMetrics().items().get(0).topicName());
        assertEquals(0, metrics.lagMetrics().items().get(0).partitionId());
        assertEquals(6L, metrics.lagMetrics().items().get(0).physicalLag());
        assertEquals(3L, metrics.lagMetrics().items().get(0).visibleLag());
        channel.finishAndReleaseAll();
    }

    @Test
    void maintenanceAdminApisRejectInvalidToken() {
        ClusterController clusterController = new ClusterController(new MetadataStore(tempDir));
        EmbeddedChannel channel = newChannel(clusterController, true);

        MqDecoder.DecodedResponse response = send(
                channel, ApiKeys.MAINTENANCE_RUN, new MaintenanceRunRequest("wrong-token"));

        assertEquals(11, response.header().correlationId());
        assertEquals(ErrorCode.UNAUTHORIZED, response.header().errorCode());
        assertNull(response.body());

        MqDecoder.DecodedResponse metricsResponse = send(
                channel, ApiKeys.RUNTIME_METRICS, new RuntimeMetricsRequest("wrong-token"));
        assertEquals(ErrorCode.UNAUTHORIZED, metricsResponse.header().errorCode());
        assertNull(metricsResponse.body());
        channel.finishAndReleaseAll();
    }

    @Test
    void maintenanceReportIncludesEnabledSegmentActions() throws Exception {
        Path dataDir = tempDir.resolve("enabled-maintenance");
        LogManager logManager = new LogManager(
                dataDir,
                new LogManagerConfig(
                        FlushPolicy.manualOnClose(),
                        0,
                        new SegmentedLogConfig(128, 1),
                        LogMaintenanceConfig.of(
                                RetentionPolicy.of(0, 256),
                                CompactionPolicy.disabled(),
                                0)));
        PartitionManager partitionManager = new PartitionManager(logManager);
        ClusterController clusterController =
                new ClusterController(new MetadataStore(dataDir.resolve("metadata")));
        GroupCoordinator groupCoordinator = new GroupCoordinator(clusterController);
        BrokerConfig config = withLegacyApis(new BrokerConfig(
                0,
                "127.0.0.1",
                9092,
                dataDir,
                true,
                "127.0.0.1",
                9092,
                false,
                BrokerTestHarness.ADMIN_TOKEN));
        MaintenanceAdminService service =
                new MaintenanceAdminService(OperatorAuthorizer.fromConfig(config), partitionManager);
        EmbeddedChannel channel = new EmbeddedChannel(
                new MqDecoder(),
                new MqRequestHandler(
                        config,
                        clusterController,
                        groupCoordinator,
                        partitionManager,
                        service),
                new MqEncoder());
        try {
            for (int i = 0; i < 6; i++) {
                partitionManager.append(
                        "orders",
                        0,
                        "shared-key".getBytes(),
                        ("value-" + i + "-" + "x".repeat(96)).getBytes());
            }

            MqDecoder.DecodedResponse response = send(
                    channel,
                    ApiKeys.MAINTENANCE_REPORT,
                    new MaintenanceReportRequest(BrokerTestHarness.ADMIN_TOKEN));
            MaintenanceReportResponse report = (MaintenanceReportResponse) response.body();

            assertEquals(ErrorCode.NONE, response.header().errorCode());
            assertFalse(report.segmentReports().isEmpty());
            assertEquals("RETENTION_DELETE", report.segmentReports().get(0).action());
        } finally {
            channel.finishAndReleaseAll();
            service.close();
            partitionManager.close();
        }
    }

    private EmbeddedChannel newChannel(ClusterController clusterController, boolean controller) {
        return newChannel(clusterController, controller, 0);
    }

    private EmbeddedChannel newChannel(ClusterController clusterController, boolean controller, int brokerId) {
        BrokerAvailabilityGuard guard = new BrokerAvailabilityGuard();
        guard.activate();
        return newChannel(clusterController, controller, brokerId, guard);
    }

    private EmbeddedChannel newChannel(
            ClusterController clusterController,
            boolean controller,
            int brokerId,
            BrokerAvailabilityGuard guard) {
        return newChannel(
                clusterController,
                controller,
                brokerId,
                guard,
                new BrokerRuntimeMetrics(),
                new RequestThrottler(0));
    }

    private EmbeddedChannel newChannel(
            ClusterController clusterController,
            boolean controller,
            int brokerId,
            BrokerAvailabilityGuard guard,
            BrokerRuntimeMetrics runtimeMetrics,
            RequestThrottler requestThrottler) {
        BrokerConfig config = withLegacyApis(defaultConfig(brokerId, tempDir, controller));
        return newChannel(config, clusterController, guard, runtimeMetrics, requestThrottler);
    }

    private EmbeddedChannel newChannel(BrokerConfig config, ClusterController clusterController) {
        BrokerAvailabilityGuard guard = new BrokerAvailabilityGuard();
        guard.activate();
        return newChannel(
                config,
                clusterController,
                guard,
                new BrokerRuntimeMetrics(),
                new RequestThrottler(0));
    }

    private EmbeddedChannel newChannel(
            BrokerConfig config,
            ClusterController clusterController,
            BrokerAvailabilityGuard guard,
            BrokerRuntimeMetrics runtimeMetrics,
            RequestThrottler requestThrottler) {
        GroupCoordinator groupCoordinator = new GroupCoordinator(clusterController);
        LogManager logManager = new LogManager(config.dataDir());
        PartitionManager partitionManager = new PartitionManager(logManager);
        return new EmbeddedChannel(
                new MqDecoder(),
                new MqRequestHandler(
                        config,
                        clusterController,
                        groupCoordinator,
                        partitionManager,
                        new ReplicaStateManager(),
                        new FetchTransferMetrics(),
                        new ReplicaStateReporter(config, clusterController, guard),
                        guard,
                        maintenanceServiceFor(config, partitionManager),
                        ControllerRole.legacy(config),
                        runtimeMetrics,
                        requestThrottler),
                new MqEncoder());
    }

    private EmbeddedChannel newChannel(
            BrokerConfig config,
            ClusterController clusterController,
            PartitionManager partitionManager,
            ReplicaStateManager replicaStateManager,
            ControllerRole controllerRole,
            BrokerRuntimeMetrics runtimeMetrics) {
        BrokerAvailabilityGuard guard = new BrokerAvailabilityGuard();
        guard.activate();
        return new EmbeddedChannel(
                new MqDecoder(),
                new MqRequestHandler(
                        config,
                        clusterController,
                        new GroupCoordinator(clusterController),
                        partitionManager,
                        replicaStateManager,
                        new FetchTransferMetrics(),
                        new ReplicaStateReporter(config, clusterController, guard),
                        guard,
                        maintenanceServiceFor(config, partitionManager),
                        controllerRole,
                        runtimeMetrics,
                        new RequestThrottler(0)),
                new MqEncoder());
    }

    private static BrokerConfig defaultConfig(Path dataDir, boolean controller) {
        return defaultConfig(0, dataDir, controller);
    }

    private static BrokerConfig defaultConfig(int brokerId, Path dataDir, boolean controller) {
        return new BrokerConfig(
                brokerId, "127.0.0.1", 9092, dataDir, controller, "127.0.0.1", 9092, false,
                BrokerTestHarness.ADMIN_TOKEN);
    }

    private static BrokerConfig withLegacyApis(BrokerConfig config) {
        return new BrokerConfig(
                config.brokerId(),
                config.host(),
                config.port(),
                config.dataDir(),
                config.controller(),
                config.controllerHost(),
                config.controllerPort(),
                config.recoverCorruptMetadata(),
                config.flushPolicy(),
                config.checkpointEveryNAppends(),
                config.segmentMaxBytes(),
                config.sparseIndexIntervalRecords(),
                config.retentionMs(),
                config.retentionBytes(),
                config.compactionEnabled(),
                config.compactionMaxSegments(),
                config.compactionMaxBytes(),
                config.maintenanceWindowStartHour(),
                config.maintenanceWindowEndHour(),
                config.maintenanceEveryNAppends(),
                config.fetchTransferMode(),
                config.fetchTransferTimeoutMs(),
                config.networkTransportMode(),
                config.maxInFlightRequests(),
                config.brokerHeartbeatIntervalMs(),
                config.brokerSelfFenceTimeoutMs(),
                config.controllerBrokerLeaseTimeoutMs(),
                config.clusterId(),
                config.etcdEndpoints(),
                config.controllerSessionTtlSeconds(),
                config.controllerFailoverGraceMs(),
                config.leaderActivationTimeoutMs(),
                config.initializeEmptyCluster(),
                config.initializeClusterFromLocal(),
                config.operatorCredentials(),
                config.clusterToken(),
                true,
                true,
                config.adminToken());
    }

    private MaintenanceAdminService maintenanceServiceFor(
            BrokerConfig config, PartitionManager partitionManager) {
        MaintenanceAdminService service =
                new MaintenanceAdminService(OperatorAuthorizer.fromConfig(config), partitionManager);
        maintenanceServices.add(service);
        return service;
    }

    private static MqDecoder.DecodedResponse send(EmbeddedChannel channel, short apiKey, Object body) {
        MqRequestHeader header = new MqRequestHeader(apiKey, 11, "test");
        channel.writeInbound(ProtocolSerdes.encodeRequest(header, body));
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        ByteBuf responseFrame;
        while ((responseFrame = channel.readOutbound()) == null) {
            channel.runPendingTasks();
            if (System.nanoTime() >= deadline) {
                throw new AssertionError("timed out waiting for broker response");
            }
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(5));
        }
        try {
            return MqDecoder.decodeResponse(unwrapPayload(responseFrame), apiKey);
        } finally {
            responseFrame.release();
        }
    }

    private static ByteBuf awaitOutbound(EmbeddedChannel channel) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        ByteBuf responseFrame;
        while ((responseFrame = channel.readOutbound()) == null) {
            channel.runPendingTasks();
            channel.runScheduledPendingTasks();
            if (System.nanoTime() >= deadline) {
                throw new AssertionError("timed out waiting for broker response");
            }
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(5));
        }
        return responseFrame;
    }

    private static MqDecoder.DecodedResponse sendHeaderOnly(EmbeddedChannel channel, short apiKey) {
        ByteBuf payload = channel.alloc().buffer();
        payload.writeShort(apiKey);
        payload.writeInt(11);
        ByteBufSerde.writeString(payload, "test");
        return sendRawPayload(channel, apiKey, payload);
    }

    private static MqDecoder.DecodedResponse sendCredentialPrefixOnly(
            EmbeddedChannel channel, short apiKey, String credential) {
        ByteBuf payload = channel.alloc().buffer();
        payload.writeShort(apiKey);
        payload.writeInt(11);
        ByteBufSerde.writeString(payload, "test");
        ByteBufSerde.writeString(payload, credential);
        return sendRawPayload(channel, apiKey, payload);
    }

    private static MqDecoder.DecodedResponse sendRawPayload(
            EmbeddedChannel channel, short apiKey, ByteBuf payload) {
        ByteBuf frame = channel.alloc().buffer();
        try {
            frame.writeShort(MqFrame.MAGIC);
            frame.writeByte(MqFrame.VERSION);
            frame.writeInt(payload.readableBytes());
            frame.writeBytes(payload);
            channel.writeInbound(frame);
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
            ByteBuf responseFrame;
            while ((responseFrame = channel.readOutbound()) == null) {
                channel.runPendingTasks();
                if (System.nanoTime() >= deadline) {
                    throw new AssertionError("timed out waiting for broker response");
                }
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(5));
            }
            try {
                return MqDecoder.decodeResponse(unwrapPayload(responseFrame), apiKey);
            } finally {
                responseFrame.release();
            }
        } finally {
            payload.release();
        }
    }
}
