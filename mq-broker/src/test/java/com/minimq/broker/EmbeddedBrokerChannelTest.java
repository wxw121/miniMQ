package com.minimq.broker;

import com.minimq.broker.controller.ClusterController;
import com.minimq.broker.controller.GroupCoordinator;
import com.minimq.broker.handler.MqRequestHandler;
import com.minimq.broker.metadata.MetadataStore;
import com.minimq.broker.metrics.BrokerRuntimeMetrics;
import com.minimq.broker.metrics.BrokerRuntimeMetricsSnapshot;
import com.minimq.broker.replication.ReplicaLeoReporter;
import com.minimq.broker.replication.ReplicaStateManager;
import com.minimq.broker.resilience.RequestThrottler;
import com.minimq.broker.resilience.ThrottleResult;
import com.minimq.broker.support.BrokerTestHarness;
import com.minimq.common.error.ErrorCode;
import com.minimq.protocol.ApiKeys;
import com.minimq.protocol.MqFrame;
import com.minimq.protocol.ProtocolSerdes;
import com.minimq.protocol.codec.MqDecoder;
import com.minimq.protocol.codec.MqEncoder;
import com.minimq.protocol.message.MqRequestHeader;
import com.minimq.protocol.message.ProtocolMessages.CreateTopicRequest;
import com.minimq.protocol.message.ProtocolMessages.DiscoverControllerRequest;
import com.minimq.protocol.message.ProtocolMessages.DiscoverControllerResponse;
import com.minimq.protocol.message.ProtocolMessages.FetchRequest;
import com.minimq.protocol.message.ProtocolMessages.FetchResponse;
import com.minimq.protocol.message.ProtocolMessages.HeartbeatRequest;
import com.minimq.protocol.message.ProtocolMessages.JoinGroupRequest;
import com.minimq.protocol.message.ProtocolMessages.JoinGroupResponse;
import com.minimq.protocol.message.ProtocolMessages.MaintenanceMetricsRequest;
import com.minimq.protocol.message.ProtocolMessages.MaintenanceMetricsResponse;
import com.minimq.protocol.message.ProtocolMessages.MaintenanceReportRequest;
import com.minimq.protocol.message.ProtocolMessages.MaintenanceReportResponse;
import com.minimq.protocol.message.ProtocolMessages.MaintenanceRunRequest;
import com.minimq.protocol.message.ProtocolMessages.MetadataRequest;
import com.minimq.protocol.message.ProtocolMessages.OffsetCommitRequest;
import com.minimq.protocol.message.ProtocolMessages.OffsetFetchRequest;
import com.minimq.protocol.message.ProtocolMessages.OffsetFetchResponse;
import com.minimq.protocol.message.ProtocolMessages.ProduceRecord;
import com.minimq.protocol.message.ProtocolMessages.ProduceRequest;
import com.minimq.protocol.message.ProtocolMessages.ProduceResponse;
import com.minimq.protocol.message.ProtocolMessages.RegisterBrokerRequest;
import com.minimq.protocol.message.ProtocolMessages.BrokerHeartbeatRequest;
import com.minimq.protocol.message.ProtocolMessages.ReplicaFetchRequest;
import com.minimq.protocol.message.ProtocolMessages.ReplicaFetchResponse;
import com.minimq.protocol.message.ProtocolMessages.RuntimeMetricsRequest;
import com.minimq.protocol.message.ProtocolMessages.RuntimeMetricsResponse;
import com.minimq.protocol.message.ProtocolMessages.SyncGroupRequest;
import com.minimq.protocol.message.ProtocolMessages.SyncGroupResponse;
import com.minimq.protocol.message.ProtocolMessages.UpdateReplicaLeoRequest;
import com.minimq.storage.CompactionPolicy;
import com.minimq.storage.FlushPolicy;
import com.minimq.storage.LogMaintenanceConfig;
import com.minimq.storage.LogManager;
import com.minimq.storage.LogManagerConfig;
import com.minimq.storage.RetentionPolicy;
import com.minimq.storage.SegmentedLogConfig;
import io.netty.buffer.ByteBuf;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
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
    void createTopicOverEmbeddedChannel() {
        MetadataStore store = new MetadataStore(tempDir);
        ClusterController clusterController = new ClusterController(store);
        GroupCoordinator groupCoordinator = new GroupCoordinator(clusterController);
        LogManager logManager = new LogManager(tempDir);
        PartitionManager partitionManager = new PartitionManager(logManager);
        BrokerConfig config = new BrokerConfig(
                0, "127.0.0.1", 9092, tempDir, true, "127.0.0.1", 9092, false,
                BrokerTestHarness.ADMIN_TOKEN);

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
        BrokerConfig config = new BrokerConfig(
                0, "127.0.0.1", 9092, tempDir, true, "127.0.0.1", 9092, false,
                BrokerTestHarness.ADMIN_TOKEN);

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
                new HeartbeatRequest("g1", join.memberId())).header().errorCode());
        assertEquals(ErrorCode.NONE, send(channel, ApiKeys.OFFSET_COMMIT,
                new OffsetCommitRequest("g1", "orders", 0, 2)).header().errorCode());

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
        BrokerConfig config = new BrokerConfig(
                0,
                "127.0.0.1",
                9092,
                dataDir,
                true,
                "127.0.0.1",
                9092,
                false,
                BrokerTestHarness.ADMIN_TOKEN);
        MaintenanceAdminService service =
                new MaintenanceAdminService(config.adminToken(), partitionManager);
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
        GroupCoordinator groupCoordinator = new GroupCoordinator(clusterController);
        LogManager logManager = new LogManager(tempDir);
        PartitionManager partitionManager = new PartitionManager(logManager);
        BrokerConfig config = new BrokerConfig(
                brokerId, "127.0.0.1", 9092, tempDir, controller, "127.0.0.1", 9092, false,
                BrokerTestHarness.ADMIN_TOKEN);
        return new EmbeddedChannel(
                new MqDecoder(),
                new MqRequestHandler(
                        config,
                        clusterController,
                        groupCoordinator,
                        partitionManager,
                        new ReplicaStateManager(),
                        new FetchTransferMetrics(),
                        new ReplicaLeoReporter(config, clusterController, guard),
                        guard,
                        maintenanceServiceFor(config, partitionManager),
                        ControllerRole.legacy(config),
                        runtimeMetrics,
                        requestThrottler),
                new MqEncoder());
    }

    private MaintenanceAdminService maintenanceServiceFor(
            BrokerConfig config, PartitionManager partitionManager) {
        MaintenanceAdminService service =
                new MaintenanceAdminService(config.adminToken(), partitionManager);
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
}
