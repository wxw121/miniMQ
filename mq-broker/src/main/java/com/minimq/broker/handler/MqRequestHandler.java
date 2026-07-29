package com.minimq.broker.handler;

import com.minimq.broker.BrokerConfig;
import com.minimq.broker.BrokerAvailabilityGuard;
import com.minimq.broker.FetchTransferMetrics;
import com.minimq.broker.MetadataSync;
import com.minimq.broker.MaintenanceAdminService;
import com.minimq.broker.PartitionManager;
import com.minimq.broker.audit.FailoverAuditLog;
import com.minimq.broker.controller.ClusterController;
import com.minimq.broker.controller.GroupCoordinator;
import com.minimq.broker.metadata.ClusterMetadata;
import com.minimq.broker.metrics.BrokerRuntimeMetrics;
import com.minimq.broker.metrics.BrokerRuntimeMetricsSnapshot;
import com.minimq.broker.metrics.RuntimeMetricsV2Service;
import com.minimq.broker.resilience.RequestThrottler;
import com.minimq.broker.resilience.ThrottleResult;
import com.minimq.broker.replication.ReplicaStateManager;
import com.minimq.broker.replication.ReplicaStateReporter;
import com.minimq.broker.replication.LeaderActivationService;
import com.minimq.broker.replication.LeaderEpochStateStore;
import com.minimq.broker.ControllerClient;
import com.minimq.broker.ControllerRole;
import com.minimq.broker.auth.OperatorAuthorizer;
import com.minimq.broker.auth.OperatorScope;
import com.minimq.broker.admin.TopicAdminService;
import com.minimq.broker.status.BrokerStatusService;
import com.minimq.broker.status.GroupStatusService;
import com.minimq.common.error.ErrorCode;
import com.minimq.common.model.Record;
import com.minimq.protocol.ApiKeys;
import com.minimq.protocol.ProtocolSerdes;
import com.minimq.protocol.message.MqRequestHeader;
import com.minimq.protocol.message.MqResponseHeader;
import com.minimq.protocol.message.ProtocolMessages;
import com.minimq.protocol.message.ProtocolMessages.*;
import com.minimq.storage.LogMaintenanceMetrics;
import com.minimq.storage.LogMaintenanceReport;
import com.minimq.storage.LogManager;
import com.minimq.storage.MaintenanceSegmentReport;
import com.minimq.storage.PartitionLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;

/**
 * Broker 请求处理器，将网络帧解码成业务请求，并按 API 类型路由到存储或控制器。
 */
public class MqRequestHandler extends SimpleChannelInboundHandler<ByteBuf> {
    private static final Logger log = LoggerFactory.getLogger(MqRequestHandler.class);
    private static final int MAX_V4_FETCH_RECORDS = 10_000;
    private final BrokerConfig config;
    private final ClusterController clusterController;
    private final GroupCoordinator groupCoordinator;
    private final PartitionManager partitionManager;
    private final ReplicaStateManager replicaStateManager;
    private final ReplicaStateReporter replicaStateReporter;
    private final FetchResponseWriter fetchResponseWriter;
    private final MaintenanceAdminService maintenanceAdminService;
    private final BrokerAvailabilityGuard availabilityGuard;
    private final LeaderActivationService leaderActivationService;
    private final ControllerRole controllerRole;
    private final BrokerRuntimeMetrics runtimeMetrics;
    private final RequestThrottler requestThrottler;
    private final OperatorAuthorizer operatorAuthorizer;
    private final BrokerStatusService brokerStatusService;
    private final GroupStatusService groupStatusService;
    private final TopicAdminService topicAdminService;
    private final RuntimeMetricsV2Service runtimeMetricsV2Service;
    private final FailoverAuditLog auditLog;

    public MqRequestHandler(
            BrokerConfig config,
            ClusterController clusterController,
            GroupCoordinator groupCoordinator,
            PartitionManager partitionManager,
            MaintenanceAdminService maintenanceAdminService) {
        this(
                config,
                clusterController,
                groupCoordinator,
                partitionManager,
                activeGuard(),
                maintenanceAdminService);
    }

    public MqRequestHandler(
            BrokerConfig config,
            ClusterController clusterController,
            GroupCoordinator groupCoordinator,
            PartitionManager partitionManager,
            BrokerAvailabilityGuard availabilityGuard,
            MaintenanceAdminService maintenanceAdminService) {
        this(
                config,
                clusterController,
                groupCoordinator,
                partitionManager,
                new ReplicaStateManager(),
                new FetchTransferMetrics(),
                new ReplicaStateReporter(config, clusterController, availabilityGuard),
                availabilityGuard,
                maintenanceAdminService);
    }

    public MqRequestHandler(
            BrokerConfig config,
            ClusterController clusterController,
            GroupCoordinator groupCoordinator,
            PartitionManager partitionManager,
            ReplicaStateManager replicaStateManager,
            FetchTransferMetrics fetchTransferMetrics,
            ReplicaStateReporter replicaStateReporter,
            MaintenanceAdminService maintenanceAdminService) {
        this(
                config,
                clusterController,
                groupCoordinator,
                partitionManager,
                replicaStateManager,
                fetchTransferMetrics,
                replicaStateReporter,
                activeGuard(),
                maintenanceAdminService);
    }

    public MqRequestHandler(
            BrokerConfig config,
            ClusterController clusterController,
            GroupCoordinator groupCoordinator,
            PartitionManager partitionManager,
            ReplicaStateManager replicaStateManager,
            FetchTransferMetrics fetchTransferMetrics,
            ReplicaStateReporter replicaStateReporter,
            BrokerAvailabilityGuard availabilityGuard,
            MaintenanceAdminService maintenanceAdminService) {
        this(config, clusterController, groupCoordinator, partitionManager, replicaStateManager,
                fetchTransferMetrics, replicaStateReporter, availabilityGuard,
                maintenanceAdminService, ControllerRole.legacy(config));
    }

    public MqRequestHandler(
            BrokerConfig config,
            ClusterController clusterController,
            GroupCoordinator groupCoordinator,
            PartitionManager partitionManager,
            ReplicaStateManager replicaStateManager,
            FetchTransferMetrics fetchTransferMetrics,
            ReplicaStateReporter replicaStateReporter,
            BrokerAvailabilityGuard availabilityGuard,
            MaintenanceAdminService maintenanceAdminService,
            ControllerRole controllerRole) {
        this(config, clusterController, groupCoordinator, partitionManager, replicaStateManager,
                fetchTransferMetrics, replicaStateReporter, availabilityGuard,
                maintenanceAdminService, controllerRole,
                new BrokerRuntimeMetrics(), new RequestThrottler(config.maxInFlightRequests()));
    }

    public MqRequestHandler(
            BrokerConfig config,
            ClusterController clusterController,
            GroupCoordinator groupCoordinator,
            PartitionManager partitionManager,
            ReplicaStateManager replicaStateManager,
            FetchTransferMetrics fetchTransferMetrics,
            ReplicaStateReporter replicaStateReporter,
            BrokerAvailabilityGuard availabilityGuard,
            MaintenanceAdminService maintenanceAdminService,
            ControllerRole controllerRole,
            BrokerRuntimeMetrics runtimeMetrics,
            RequestThrottler requestThrottler) {
        this(config, clusterController, groupCoordinator, partitionManager, replicaStateManager,
                fetchTransferMetrics, replicaStateReporter, availabilityGuard,
                maintenanceAdminService, controllerRole,
                OperatorAuthorizer.fromConfig(config), runtimeMetrics, requestThrottler);
    }

    public MqRequestHandler(
            BrokerConfig config,
            ClusterController clusterController,
            GroupCoordinator groupCoordinator,
            PartitionManager partitionManager,
            ReplicaStateManager replicaStateManager,
            FetchTransferMetrics fetchTransferMetrics,
            ReplicaStateReporter replicaStateReporter,
            BrokerAvailabilityGuard availabilityGuard,
            MaintenanceAdminService maintenanceAdminService,
            ControllerRole controllerRole,
            OperatorAuthorizer operatorAuthorizer,
            BrokerRuntimeMetrics runtimeMetrics,
            RequestThrottler requestThrottler) {
        this(
                config,
                clusterController,
                groupCoordinator,
                partitionManager,
                replicaStateManager,
                fetchTransferMetrics,
                replicaStateReporter,
                availabilityGuard,
                maintenanceAdminService,
                controllerRole,
                operatorAuthorizer,
                runtimeMetrics,
                requestThrottler,
                null);
    }

    public MqRequestHandler(
            BrokerConfig config,
            ClusterController clusterController,
            GroupCoordinator groupCoordinator,
            PartitionManager partitionManager,
            ReplicaStateManager replicaStateManager,
            FetchTransferMetrics fetchTransferMetrics,
            ReplicaStateReporter replicaStateReporter,
            BrokerAvailabilityGuard availabilityGuard,
            MaintenanceAdminService maintenanceAdminService,
            ControllerRole controllerRole,
            OperatorAuthorizer operatorAuthorizer,
            BrokerRuntimeMetrics runtimeMetrics,
            RequestThrottler requestThrottler,
            BrokerStatusService brokerStatusService) {
        this(
                config,
                clusterController,
                groupCoordinator,
                partitionManager,
                replicaStateManager,
                fetchTransferMetrics,
                replicaStateReporter,
                availabilityGuard,
                maintenanceAdminService,
                controllerRole,
                operatorAuthorizer,
                runtimeMetrics,
                requestThrottler,
                brokerStatusService,
                null);
    }

    public MqRequestHandler(
            BrokerConfig config,
            ClusterController clusterController,
            GroupCoordinator groupCoordinator,
            PartitionManager partitionManager,
            ReplicaStateManager replicaStateManager,
            FetchTransferMetrics fetchTransferMetrics,
            ReplicaStateReporter replicaStateReporter,
            BrokerAvailabilityGuard availabilityGuard,
            MaintenanceAdminService maintenanceAdminService,
            ControllerRole controllerRole,
            OperatorAuthorizer operatorAuthorizer,
            BrokerRuntimeMetrics runtimeMetrics,
            RequestThrottler requestThrottler,
            BrokerStatusService brokerStatusService,
            FailoverAuditLog auditLog) {
        this(
                config,
                clusterController,
                groupCoordinator,
                partitionManager,
                replicaStateManager,
                fetchTransferMetrics,
                replicaStateReporter,
                availabilityGuard,
                maintenanceAdminService,
                controllerRole,
                operatorAuthorizer,
                runtimeMetrics,
                requestThrottler,
                brokerStatusService,
                auditLog,
                null);
    }

    public MqRequestHandler(
            BrokerConfig config,
            ClusterController clusterController,
            GroupCoordinator groupCoordinator,
            PartitionManager partitionManager,
            ReplicaStateManager replicaStateManager,
            FetchTransferMetrics fetchTransferMetrics,
            ReplicaStateReporter replicaStateReporter,
            BrokerAvailabilityGuard availabilityGuard,
            MaintenanceAdminService maintenanceAdminService,
            ControllerRole controllerRole,
            OperatorAuthorizer operatorAuthorizer,
            BrokerRuntimeMetrics runtimeMetrics,
            RequestThrottler requestThrottler,
            BrokerStatusService brokerStatusService,
            FailoverAuditLog auditLog,
            TopicAdminService topicAdminService) {
        this.config = config;
        this.replicaStateReporter = replicaStateReporter;
        this.clusterController = clusterController;
        this.groupCoordinator = groupCoordinator;
        this.partitionManager = partitionManager;
        this.replicaStateManager = replicaStateManager;
        this.fetchResponseWriter = new FetchResponseWriter(
                config, fetchTransferMetrics, runtimeMetrics::recordFetchBytes);
        this.maintenanceAdminService = maintenanceAdminService;
        this.availabilityGuard = availabilityGuard;
        this.controllerRole = controllerRole;
        this.runtimeMetrics = runtimeMetrics;
        this.requestThrottler = requestThrottler;
        this.operatorAuthorizer = operatorAuthorizer;
        this.auditLog = auditLog != null
                ? auditLog
                : new FailoverAuditLog(config.brokerId(), System::currentTimeMillis);
        this.brokerStatusService = brokerStatusService != null
                ? brokerStatusService
                : new BrokerStatusService(
                        config,
                        clusterController,
                        availabilityGuard,
                        controllerRole,
                        System::currentTimeMillis,
                        System.currentTimeMillis());
        this.groupStatusService = new GroupStatusService(clusterController, System::currentTimeMillis);
        this.topicAdminService = topicAdminService != null
                ? topicAdminService
                : new TopicAdminService(clusterController, config.topicDeleteTimeoutMs());
        this.runtimeMetricsV2Service = new RuntimeMetricsV2Service(
                runtimeMetrics,
                System::currentTimeMillis,
                this.auditLog::overwrittenCount);
        this.leaderActivationService = new LeaderActivationService(
                partitionManager,
                new LeaderEpochStateStore(config.dataDir().resolve("leader-epochs")),
                this.auditLog);
    }

    @Override
    // 单个请求的异常会被转换为协议错误响应，避免异常直接打断 Netty channel。
    protected void channelRead0(ChannelHandlerContext ctx, ByteBuf payload) {
        MqRequestHeader header = null;
        Object responseBody = null;
        ErrorCode errorCode = ErrorCode.NONE;
        ThrottleResult throttle = null;
        long startedNanos = System.nanoTime();

        try {
            header = ProtocolSerdes.decodeRequestHeader(payload);
            runtimeMetrics.recordRequest(header.apiKey());
            throttle = requestThrottler.tryAcquire(header.apiKey());
            if (!throttle.allowed()) {
                errorCode = ErrorCode.THROTTLED;
                runtimeMetrics.recordThrottled(header.apiKey());
                writeResponse(ctx, header.correlationId(), errorCode, null);
                recordCompletion(header, errorCode, startedNanos);
                return;
            }
            errorCode = authorizeBeforeDecode(header.apiKey(), payload);
            if (errorCode != ErrorCode.NONE) {
                writeResponse(ctx, header.correlationId(), errorCode, null);
                recordCompletion(header, errorCode, startedNanos);
                throttle.release();
                return;
            }
            Object requestBody = ProtocolSerdes.decodeRequestBody(header.apiKey(), payload);
            switch (header.apiKey()) {
                case ApiKeys.DISCOVER_CONTROLLER -> responseBody = discoverController();
                case ApiKeys.CREATE_TOPIC -> {
                    ensureController();
                    errorCode = clusterController.createTopic((CreateTopicRequest) requestBody);
                    if (errorCode == ErrorCode.NONE) {
                        responseBody = new CreateTopicResponse(((CreateTopicRequest) requestBody).topicName());
                    }
                }
                case ApiKeys.METADATA -> {
                    ensureController();
                    MetadataRequest request = (MetadataRequest) requestBody;
                    if (!request.topicName().isEmpty()
                            && clusterController.getTopicTombstone(request.topicName()) != null) {
                        errorCode = ErrorCode.UNKNOWN_TOPIC;
                    } else {
                        responseBody = clusterController.metadata(request);
                    }
                }
                case ApiKeys.REGISTER_BROKER -> {
                    ensureController();
                    var result = clusterController.registerBroker((RegisterBrokerRequest) requestBody);
                    errorCode = result.errorCode();
                    responseBody = result.response();
                }
                case ApiKeys.BROKER_HEARTBEAT -> {
                    ensureController();
                    errorCode = clusterController.heartbeatBroker((BrokerHeartbeatRequest) requestBody);
                    if (errorCode == ErrorCode.NONE) {
                        responseBody = new BrokerHeartbeatResponse();
                    }
                }
                case ApiKeys.MARK_BROKER_OFFLINE -> {
                    ensureController();
                    errorCode = clusterController.markBrokerOffline(
                            ((MarkBrokerOfflineRequest) requestBody).brokerId());
                    if (errorCode == ErrorCode.NONE) {
                        responseBody = new MarkBrokerOfflineResponse();
                    }
                }
                case ApiKeys.MARK_BROKER_OFFLINE_ADMIN -> {
                    ensureController();
                    MarkBrokerOfflineAdminRequest request = (MarkBrokerOfflineAdminRequest) requestBody;
                    errorCode = clusterController.markBrokerOffline(request.brokerId());
                    if (errorCode == ErrorCode.NONE) {
                        responseBody = new MarkBrokerOfflineAdminResponse(
                                request.brokerId(), clusterController.snapshot().metadataVersion);
                    }
                }
                case ApiKeys.UPDATE_REPLICA_LEO -> {
                    ensureController();
                    UpdateReplicaLeoRequest request = (UpdateReplicaLeoRequest) requestBody;
                    errorCode = clusterController.updateReplicaLogEndOffset(
                            request.topicName(),
                            request.partitionId(),
                            request.replicaBrokerId(),
                            request.brokerEpoch(),
                            request.controllerEpoch(),
                            request.logEndOffset());
                    if (errorCode == ErrorCode.NONE) {
                        responseBody = new UpdateReplicaLeoResponse();
                    }
                }
                case ApiKeys.LEADER_READY -> {
                    ensureController();
                    errorCode = clusterController.leaderReady((LeaderReadyRequest) requestBody);
                    if (errorCode == ErrorCode.NONE) {
                        responseBody = new LeaderReadyResponse();
                    }
                }
                case ApiKeys.UPDATE_REPLICA_STATE_V2 -> {
                    ensureController();
                    UpdateReplicaStateV2Request request = (UpdateReplicaStateV2Request) requestBody;
                    errorCode = clusterController.updateReplicaStateV2(
                            request.topicName(),
                            request.partitionId(),
                            request.replicaBrokerId(),
                            request.brokerEpoch(),
                            request.controllerEpoch(),
                            request.leaderEpoch(),
                            request.physicalLogEndOffset(),
                            request.visibleLogEndOffset(),
                            request.nextVisibleAtEpochMs());
                    if (errorCode == ErrorCode.NONE) {
                        responseBody = new UpdateReplicaStateV2Response();
                    }
                }
                case ApiKeys.TOPIC_DELETE_ACK -> {
                    ensureController();
                    TopicDeleteAckRequest request = (TopicDeleteAckRequest) requestBody;
                    errorCode = clusterController.acknowledgeTopicDeletion(
                            request.topicName(),
                            request.tombstoneVersion(),
                            request.brokerId(),
                            request.brokerEpoch(),
                            request.controllerEpoch());
                    if (errorCode == ErrorCode.NONE) {
                        responseBody = new TopicDeleteAckResponse();
                    }
                }
                case ApiKeys.PRODUCE -> {
                    ProduceRequest request = (ProduceRequest) requestBody;
                    errorCode = requireWritableDataPlane();
                    if (errorCode == ErrorCode.NONE) {
                        errorCode = ensureLeader(request.topicName(), request.partitionId());
                    }
                    if (errorCode == ErrorCode.NONE) {
                        errorCode = clusterController.validateLeaderEpoch(
                                request.topicName(), request.partitionId(), request.leaderEpoch());
                    }
                    if (errorCode == ErrorCode.NONE) {
                        responseBody = handleProduce(request);
                    }
                }
                case ApiKeys.FETCH -> {
                    FetchRequest request = (FetchRequest) requestBody;
                    errorCode = requireReadableDataPlane();
                    if (errorCode == ErrorCode.NONE) {
                        errorCode = ensureLeader(request.topicName(), request.partitionId());
                    }
                    if (errorCode == ErrorCode.NONE) {
                        errorCode = clusterController.validateLeaderEpoch(
                                request.topicName(), request.partitionId(), request.leaderEpoch());
                    }
                    if (errorCode == ErrorCode.NONE) {
                        PartitionLog partitionLog =
                                partitionManager.getOrCreate(request.topicName(), request.partitionId());
                        long replicationHighWatermark = effectiveHighWatermark(
                                request.topicName(), request.partitionId(), partitionLog);
                        long visibleHighWatermark = partitionManager.visibleHighWatermark(
                                request.topicName(), request.partitionId());
                        long fetchHighWatermark = Math.min(replicationHighWatermark, visibleHighWatermark);
                        MqResponseHeader fetchHeader =
                                new MqResponseHeader(header.correlationId(), ErrorCode.NONE);
                        fetchResponseWriter.write(
                                ctx, fetchHeader, request, partitionLog, fetchHighWatermark, replicationHighWatermark);
                        recordCompletion(header, ErrorCode.NONE, startedNanos);
                        throttle.release();
                        return;
                    }
                }
                case ApiKeys.PRODUCE_V2 -> {
                    ProduceV2Request request = (ProduceV2Request) requestBody;
                    errorCode = requireWritableDataPlane();
                    if (errorCode == ErrorCode.NONE) {
                        errorCode = topicDeletedError(request.topicName());
                    }
                    if (errorCode == ErrorCode.NONE) {
                        errorCode = ensureLeader(request.topicName(), request.partitionId());
                    }
                    if (errorCode == ErrorCode.NONE) {
                        errorCode = clusterController.validateLeaderEpoch(
                                request.topicName(), request.partitionId(), request.leaderEpoch());
                    }
                    if (errorCode == ErrorCode.NONE) {
                        errorCode = validateProduceV2(request);
                    }
                    if (errorCode == ErrorCode.NONE) {
                        try {
                            responseBody = handleProduceV2(request);
                        } catch (LogManager.ScheduledCapacityExceededException e) {
                            errorCode = ErrorCode.THROTTLED;
                        }
                    }
                }
                case ApiKeys.REPLICA_FETCH -> {
                    ReplicaFetchRequest request = (ReplicaFetchRequest) requestBody;
                    errorCode = requireReadableDataPlane();
                    if (errorCode == ErrorCode.NONE) {
                        errorCode = handleReplicaFetch(request);
                    }
                    if (errorCode == ErrorCode.NONE) {
                        responseBody = buildReplicaFetchResponse(request);
                    }
                }
                case ApiKeys.JOIN_GROUP -> {
                    ensureController();
                    responseBody = groupCoordinator.joinGroup((JoinGroupRequest) requestBody);
                }
                case ApiKeys.SYNC_GROUP -> {
                    ensureController();
                    responseBody = groupCoordinator.syncGroup((SyncGroupRequest) requestBody);
                }
                case ApiKeys.HEARTBEAT -> {
                    ensureController();
                    errorCode = groupCoordinator.heartbeat((HeartbeatRequest) requestBody);
                    if (errorCode == ErrorCode.NONE) {
                        responseBody = new HeartbeatResponse();
                    }
                }
                case ApiKeys.OFFSET_COMMIT -> {
                    ensureController();
                    errorCode = groupCoordinator.offsetCommit((OffsetCommitRequest) requestBody);
                    if (errorCode == ErrorCode.NONE) {
                        responseBody = new OffsetCommitResponse();
                    }
                }
                case ApiKeys.OFFSET_FETCH -> {
                    ensureController();
                    responseBody = groupCoordinator.offsetFetch((OffsetFetchRequest) requestBody);
                }
                case ApiKeys.MAINTENANCE_REPORT -> {
                    MaintenanceReportRequest request = (MaintenanceReportRequest) requestBody;
                    writeAsyncMaintenanceReport(
                            ctx, header, maintenanceAdminService.report(request.adminToken()),
                            throttle, startedNanos);
                    return;
                }
                case ApiKeys.MAINTENANCE_RUN -> {
                    if (!availabilityGuard.allowsMaintenanceRun()) {
                        errorCode = availabilityGuard.rejectionError();
                    } else {
                        MaintenanceRunRequest request = (MaintenanceRunRequest) requestBody;
                        writeAsyncMaintenanceReport(
                                ctx, header, maintenanceAdminService.run(request.adminToken()),
                                throttle, startedNanos);
                        return;
                    }
                }
                case ApiKeys.REPLICA_FETCH_V2 -> {
                    ReplicaFetchV2Request request = (ReplicaFetchV2Request) requestBody;
                    errorCode = clusterController.validateReplicaFetchV2Identity(
                            request.topicName(),
                            request.partitionId(),
                            request.replicaBrokerId(),
                            request.brokerEpoch(),
                            request.controllerEpoch());
                    if (errorCode == ErrorCode.NONE) {
                        errorCode = requireReadableDataPlane();
                    }
                    if (errorCode == ErrorCode.NONE) {
                        errorCode = topicDeletedError(request.topicName());
                    }
                    if (errorCode == ErrorCode.NONE) {
                        errorCode = handleReplicaFetchV2(request);
                    }
                    if (errorCode == ErrorCode.NONE) {
                        responseBody = buildReplicaFetchV2Response(request);
                    }
                }
                case ApiKeys.MAINTENANCE_METRICS -> {
                    MaintenanceMetricsRequest request = (MaintenanceMetricsRequest) requestBody;
                    MaintenanceAdminService.Result<LogMaintenanceMetrics> result =
                            maintenanceAdminService.metrics(request.adminToken());
                    errorCode = result.errorCode();
                    if (errorCode == ErrorCode.NONE) {
                        responseBody = toMaintenanceMetricsResponse(result.body());
                    }
                }
                case ApiKeys.RUNTIME_METRICS -> {
                    RuntimeMetricsRequest request = (RuntimeMetricsRequest) requestBody;
                    errorCode = maintenanceAdminService.authorize(request.adminToken());
                    if (errorCode == ErrorCode.NONE) {
                        responseBody = toRuntimeMetricsResponse(runtimeMetrics.snapshot());
                    }
                }
                case ApiKeys.RUNTIME_METRICS_V2 -> {
                    RuntimeMetricsV2Request request = (RuntimeMetricsV2Request) requestBody;
                    responseBody = runtimeMetricsV2Service.snapshot(
                            request.counterLimit(),
                            request.latencyLimit(),
                            request.lagLimit(),
                            groupStatusService.lagMetricsSnapshot());
                }
                case ApiKeys.OPS_STATUS -> {
                    OpsStatusRequest request = (OpsStatusRequest) requestBody;
                    responseBody = brokerStatusService.status(
                            request.brokerLimit(), request.partitionLimit());
                }
                case ApiKeys.GROUP_STATUS -> {
                    ensureController();
                    GroupStatusRequest request = (GroupStatusRequest) requestBody;
                    GroupStatusService.Result result = groupStatusService.status(
                            request.groupId(),
                            request.topicFilter(),
                            request.memberLimit(),
                            request.partitionLimit());
                    errorCode = result.errorCode();
                    if (errorCode == ErrorCode.NONE) {
                        responseBody = result.response();
                    }
                }
                case ApiKeys.FAILOVER_AUDIT -> {
                    FailoverAuditRequest request = (FailoverAuditRequest) requestBody;
                    List<FailoverAuditLog.AuditEvent> events = auditLog.newestFirst(request.limit());
                    List<AuditEventInfo> eventInfos = events.stream()
                            .map(MqRequestHandler::toAuditEventInfo)
                            .toList();
                    int total = auditLog.size();
                    responseBody = new FailoverAuditResponse(
                            auditLog.brokerId(),
                            auditLog.startedAtEpochMs(),
                            auditLog.overwrittenCount(),
                            new V4BoundedList<>(total, total > eventInfos.size(), eventInfos));
                }
                case ApiKeys.LIST_TOPICS -> {
                    ensureController();
                    TopicAdminService.Result<ListTopicsResponse> result =
                            topicAdminService.list((ListTopicsRequest) requestBody);
                    errorCode = result.errorCode();
                    responseBody = result.response();
                }
                case ApiKeys.DESCRIBE_TOPIC -> {
                    ensureController();
                    TopicAdminService.Result<DescribeTopicResponse> result =
                            topicAdminService.describe((DescribeTopicRequest) requestBody);
                    errorCode = result.errorCode();
                    responseBody = result.response();
                }
                case ApiKeys.DELETE_TOPIC -> {
                    ensureController();
                    writeAsyncTopicDelete(
                            ctx,
                            header,
                            topicAdminService.deleteAndWait((DeleteTopicRequest) requestBody),
                            throttle,
                            startedNanos);
                    return;
                }
                case ApiKeys.INCREASE_PARTITIONS -> {
                    ensureController();
                    TopicAdminService.Result<IncreasePartitionsResponse> result =
                            topicAdminService.increase((IncreasePartitionsRequest) requestBody);
                    errorCode = result.errorCode();
                    responseBody = result.response();
                }
                case ApiKeys.CONFIGURE_TOPIC_RELIABILITY -> {
                    ensureController();
                    TopicAdminService.Result<ConfigureTopicReliabilityResponse> result =
                            topicAdminService.configureReliability(
                                    (ConfigureTopicReliabilityRequest) requestBody);
                    errorCode = result.errorCode();
                    responseBody = result.response();
                }
                case ApiKeys.CREATE_TOPIC_ADMIN -> {
                    ensureController();
                    TopicAdminService.Result<CreateTopicAdminResponse> result =
                            topicAdminService.create((CreateTopicAdminRequest) requestBody);
                    errorCode = result.errorCode();
                    responseBody = result.response();
                }
                case ApiKeys.FETCH_V2 -> {
                    FetchV2Request request = (FetchV2Request) requestBody;
                    errorCode = requireReadableDataPlane();
                    if (errorCode == ErrorCode.NONE) {
                        errorCode = topicDeletedError(request.topicName());
                    }
                    if (errorCode == ErrorCode.NONE) {
                        errorCode = ensureLeader(request.topicName(), request.partitionId());
                    }
                    if (errorCode == ErrorCode.NONE) {
                        errorCode = clusterController.validateLeaderEpoch(
                                request.topicName(), request.partitionId(), request.leaderEpoch());
                    }
                    if (errorCode == ErrorCode.NONE) {
                        responseBody = buildFetchV2Response(request);
                    }
                }
                case ApiKeys.METADATA_V2 -> {
                    ensureController();
                    MetadataV2Request request = (MetadataV2Request) requestBody;
                    if (request.topicFilter() != null
                            && clusterController.getTopicTombstone(request.topicFilter()) != null) {
                        errorCode = ErrorCode.UNKNOWN_TOPIC;
                    } else {
                        responseBody = buildMetadataV2Response(request);
                    }
                }
                case ApiKeys.AUTH_STATUS -> {
                    AuthStatusRequest request = (AuthStatusRequest) requestBody;
                    OperatorAuthorizer.Decision decision =
                            operatorAuthorizer.authenticateForAuthStatus(request.credential());
                    errorCode = authorizationErrorCode(decision);
                    if (errorCode == ErrorCode.NONE) {
                        List<String> scopes = scopeNames(decision.scopes());
                        responseBody = new AuthStatusResponse(
                                config.brokerId(),
                                new V4BoundedList<>(scopes.size(), false, scopes));
                    }
                }
                default -> errorCode = ErrorCode.INVALID_REQUEST;
            }
        } catch (NotControllerException ex) {
            errorCode = ErrorCode.NOT_CONTROLLER;
        } catch (LogManager.ScheduledCapacityExceededException ex) {
            errorCode = ErrorCode.THROTTLED;
        } catch (IllegalArgumentException | IllegalStateException | IndexOutOfBoundsException ex) {
            errorCode = ErrorCode.INVALID_REQUEST;
        } catch (Exception ex) {
            log.error("Request handling failed for apiKey={}", header != null ? header.apiKey() : "unknown", ex);
            errorCode = ErrorCode.INVALID_REQUEST;
        }

        int correlationId = header != null ? header.correlationId() : 0;
        writeResponse(ctx, correlationId, errorCode, responseBody);
        recordCompletion(header, errorCode, startedNanos);
        if (throttle != null) {
            throttle.release();
        }
    }

    private ErrorCode authorizeBeforeDecode(short apiKey, ByteBuf payload) {
        if (apiKey == ApiKeys.CREATE_TOPIC && !config.allowLegacyUnauthenticatedCreate()) {
            return ErrorCode.UNAUTHORIZED;
        }
        if (isLegacyInternalApi(apiKey) && !config.allowLegacyInternalApi()) {
            return ErrorCode.UNAUTHORIZED;
        }
        if (apiKey == ApiKeys.AUTH_STATUS) {
            return authorizationErrorCode(
                    operatorAuthorizer.authenticateForAuthStatus(
                            ProtocolSerdes.peekRequestCredential(apiKey, payload)));
        }
        OperatorScope requiredScope = requiredOperatorScope(apiKey);
        if (requiredScope != null) {
            return authorizationErrorCode(
                    operatorAuthorizer.authorize(
                            ProtocolSerdes.peekRequestCredential(apiKey, payload),
                            requiredScope));
        }
        if (isClusterCredentialApi(apiKey)) {
            return authorizeClusterCredential(ProtocolSerdes.peekRequestCredential(apiKey, payload));
        }
        return ErrorCode.NONE;
    }

    private ErrorCode authorizeClusterCredential(String suppliedCredential) {
        return OperatorAuthorizer.constantTimeEqualsUtf8(config.clusterToken(), suppliedCredential)
                ? ErrorCode.NONE
                : ErrorCode.UNAUTHORIZED;
    }

    private static boolean isLegacyInternalApi(short apiKey) {
        return apiKey == ApiKeys.REPLICA_FETCH
                || apiKey == ApiKeys.MARK_BROKER_OFFLINE
                || apiKey == ApiKeys.UPDATE_REPLICA_LEO;
    }

    private static boolean isClusterCredentialApi(short apiKey) {
        return apiKey == ApiKeys.REPLICA_FETCH_V2
                || apiKey == ApiKeys.TOPIC_DELETE_ACK
                || apiKey == ApiKeys.UPDATE_REPLICA_STATE_V2;
    }

    private static OperatorScope requiredOperatorScope(short apiKey) {
        return switch (apiKey) {
            case ApiKeys.OPS_STATUS,
                    ApiKeys.GROUP_STATUS,
                    ApiKeys.FAILOVER_AUDIT,
                    ApiKeys.RUNTIME_METRICS_V2 -> OperatorScope.STATUS;
            case ApiKeys.LIST_TOPICS,
                    ApiKeys.DESCRIBE_TOPIC,
                    ApiKeys.DELETE_TOPIC,
                    ApiKeys.INCREASE_PARTITIONS,
                    ApiKeys.CONFIGURE_TOPIC_RELIABILITY,
                    ApiKeys.CREATE_TOPIC_ADMIN -> OperatorScope.TOPIC_ADMIN;
            case ApiKeys.MARK_BROKER_OFFLINE_ADMIN -> OperatorScope.BROKER_OFFLINE;
            default -> null;
        };
    }

    private static ErrorCode authorizationErrorCode(OperatorAuthorizer.Decision decision) {
        if (!decision.authenticated()) {
            return ErrorCode.UNAUTHORIZED;
        }
        return decision.authorized() ? ErrorCode.NONE : ErrorCode.FORBIDDEN;
    }

    private static List<String> scopeNames(Set<OperatorScope> scopes) {
        List<String> names = new ArrayList<>();
        for (OperatorScope scope : OperatorScope.values()) {
            if (scopes.contains(scope)) {
                names.add(scope.name());
            }
        }
        return names;
    }

    private void writeAsyncMaintenanceReport(
            ChannelHandlerContext ctx,
            MqRequestHeader header,
            CompletionStage<MaintenanceAdminService.Result<LogMaintenanceReport>> stage,
            ThrottleResult throttle,
            long startedNanos) {
        stage.whenComplete((result, failure) -> ctx.executor().execute(() -> {
            if (failure != null) {
                log.error("Maintenance request failed for apiKey={}", header.apiKey(), failure);
                writeResponse(ctx, header.correlationId(), ErrorCode.INVALID_REQUEST, null);
                recordCompletion(header, ErrorCode.INVALID_REQUEST, startedNanos);
                throttle.release();
                return;
            }
            Object body = result.errorCode() == ErrorCode.NONE
                    ? toMaintenanceReportResponse(result.body())
                    : null;
            writeResponse(ctx, header.correlationId(), result.errorCode(), body);
            recordCompletion(header, result.errorCode(), startedNanos);
            throttle.release();
        }));
    }

    private void writeAsyncTopicDelete(
            ChannelHandlerContext ctx,
            MqRequestHeader header,
            CompletionStage<TopicAdminService.Result<DeleteTopicResponse>> stage,
            ThrottleResult throttle,
            long startedNanos) {
        stage.whenComplete((result, failure) -> ctx.executor().execute(() -> {
            if (failure != null) {
                log.error("Topic deletion request failed", failure);
                writeResponse(ctx, header.correlationId(), ErrorCode.CONTROLLER_NOT_READY, null);
                recordCompletion(header, ErrorCode.CONTROLLER_NOT_READY, startedNanos);
                throttle.release();
                return;
            }
            writeResponse(ctx, header.correlationId(), result.errorCode(), result.response());
            recordCompletion(header, result.errorCode(), startedNanos);
            throttle.release();
        }));
    }

    private void recordCompletion(MqRequestHeader header, ErrorCode errorCode, long startedNanos) {
        runtimeMetrics.recordResponse(errorCode);
        if (header != null) {
            runtimeMetrics.recordLatencyNanos(
                    header.apiKey(), System.nanoTime() - startedNanos);
        }
    }

    private ErrorCode requireActiveDataPlane() {
        return availabilityGuard.allowsDataPlane()
                ? ErrorCode.NONE
                : availabilityGuard.rejectionError();
    }

    private ErrorCode requireWritableDataPlane() {
        return availabilityGuard.allowsProduce()
                ? ErrorCode.NONE
                : availabilityGuard.rejectionError();
    }

    private ErrorCode requireReadableDataPlane() {
        return availabilityGuard.allowsFetch()
                ? ErrorCode.NONE
                : availabilityGuard.rejectionError();
    }

    private static BrokerAvailabilityGuard activeGuard() {
        BrokerAvailabilityGuard guard = new BrokerAvailabilityGuard();
        guard.activate();
        return guard;
    }

    private void writeResponse(
            ChannelHandlerContext ctx, int correlationId, ErrorCode errorCode, Object responseBody) {
        MqResponseHeader responseHeader = new MqResponseHeader(correlationId, errorCode);
        ByteBuf frame = ProtocolSerdes.encodeResponse(responseHeader, responseBody);
        if (responseBody instanceof FetchV2Response fetchResponse) {
            long fetchPayloadBytes = fetchV2PayloadBytes(fetchResponse);
            ctx.writeAndFlush(frame).addListener(future -> {
                if (future.isSuccess()) {
                    runtimeMetrics.recordFetchBytes(fetchPayloadBytes);
                }
            });
            return;
        }
        ctx.writeAndFlush(frame);
    }

    private static long fetchV2PayloadBytes(FetchV2Response response) {
        long payloadBytes = 0L;
        for (FetchedRecord record : response.records().items()) {
            payloadBytes = Math.addExact(
                    payloadBytes,
                    (record.key() == null ? 0L : record.key().length)
                            + (record.value() == null ? 0L : record.value().length));
        }
        return payloadBytes;
    }

    private static MaintenanceReportResponse toMaintenanceReportResponse(LogMaintenanceReport report) {
        return new MaintenanceReportResponse(
                report.dryRun(),
                report.status().name(),
                report.segmentReports().stream()
                        .map(MqRequestHandler::toMaintenanceSegmentInfo)
                        .toList(),
                report.skippedDueToReaders(),
                report.cleanupFailures(),
                report.deletedBytes(),
                report.deletedSegments(),
                report.compactedBytes(),
                report.compactedSegments());
    }

    private static MaintenanceSegmentInfo toMaintenanceSegmentInfo(MaintenanceSegmentReport report) {
        return new MaintenanceSegmentInfo(
                report.partition(),
                report.action().name(),
                report.baseOffset(),
                report.firstOffset(),
                report.lastOffset(),
                report.sizeBytes());
    }

    private static MaintenanceMetricsResponse toMaintenanceMetricsResponse(LogMaintenanceMetrics metrics) {
        return new MaintenanceMetricsResponse(
                metrics.runs(),
                metrics.dryRuns(),
                metrics.deletedBytes(),
                metrics.deletedSegments(),
                metrics.compactedBytes(),
                metrics.compactedSegments(),
                metrics.skippedDueToReaders(),
                metrics.cleanupFailures(),
                metrics.lastRunStatus().name());
    }

    private static RuntimeMetricsResponse toRuntimeMetricsResponse(BrokerRuntimeMetricsSnapshot snapshot) {
        return new RuntimeMetricsResponse(
                toRuntimeMetricCounts(snapshot.requestsByApiKey()),
                toRuntimeErrorCounts(snapshot.responsesByErrorCode()),
                toRuntimeLatencySummaries(snapshot),
                toRuntimeMetricCounts(snapshot.throttledByApiKey()));
    }

    private static List<RuntimeMetricCount> toRuntimeMetricCounts(Map<Short, Long> counts) {
        return counts.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .map(entry -> new RuntimeMetricCount(entry.getKey(), entry.getValue()))
                .toList();
    }

    private static List<RuntimeErrorCount> toRuntimeErrorCounts(Map<ErrorCode, Long> counts) {
        return counts.entrySet().stream()
                .sorted((left, right) -> Short.compare(left.getKey().code, right.getKey().code))
                .map(entry -> new RuntimeErrorCount(entry.getKey().code, entry.getValue()))
                .toList();
    }

    private static List<RuntimeLatencySummary> toRuntimeLatencySummaries(BrokerRuntimeMetricsSnapshot snapshot) {
        return snapshot.latencyCountsByApiKey().entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .map(entry -> new RuntimeLatencySummary(
                        entry.getKey(),
                        entry.getValue(),
                        snapshot.totalLatencyMsByApiKey().getOrDefault(entry.getKey(), 0L),
                        snapshot.averageLatencyMs(entry.getKey())))
                .toList();
    }

    private static AuditEventInfo toAuditEventInfo(FailoverAuditLog.AuditEvent event) {
        return new AuditEventInfo(
                event.sequence(),
                event.timestampEpochMs(),
                event.eventType().name(),
                event.brokerId(),
                event.topicName(),
                event.partitionId(),
                event.oldValue(),
                event.newValue(),
                event.detail());
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("Channel pipeline error", cause);
        ctx.close();
    }

    private ProduceResponse handleProduce(ProduceRequest request) {
        long baseOffset = -1;
        long payloadBytes = 0L;
        for (ProduceRecord record : request.records()) {
            payloadBytes = Math.addExact(
                    payloadBytes,
                    (record.key() == null ? 0L : record.key().length)
                            + (record.value() == null ? 0L : record.value().length));
            baseOffset = partitionManager.append(request.topicName(), request.partitionId(), record.key(), record.value());
        }
        runtimeMetrics.recordProduceBytes(payloadBytes);
        PartitionLog partitionLog = partitionManager.getOrCreate(request.topicName(), request.partitionId());
        replicaStateManager.updateReplicaLeo(
                request.topicName(),
                request.partitionId(),
                config.brokerId(),
                partitionLog.highWatermark());
        reportReplicaState(
                request.topicName(),
                request.partitionId(),
                config.brokerId(),
                partitionLog.highWatermark());
        return new ProduceResponse(request.topicName(), request.partitionId(), baseOffset);
    }

    private ErrorCode validateProduceV2(ProduceV2Request request) {
        if (request.records().truncated()
                || request.records().total() != request.records().items().size()) {
            return ErrorCode.INVALID_REQUEST;
        }
        return ErrorCode.NONE;
    }

    private ProduceV2Response handleProduceV2(ProduceV2Request request) {
        long payloadBytes = 0L;
        List<LogManager.AppendRecord> records = new ArrayList<>(request.records().items().size());
        for (ScheduledProduceRecord record : request.records().items()) {
            payloadBytes = Math.addExact(
                    payloadBytes,
                    (record.key() == null ? 0L : record.key().length)
                            + (record.value() == null ? 0L : record.value().length));
            records.add(new LogManager.AppendRecord(
                    record.key(), record.value(), record.deliverAtEpochMs()));
        }
        long baseOffset = partitionManager.appendBatch(
                request.topicName(), request.partitionId(), records);
        runtimeMetrics.recordProduceBytes(payloadBytes);
        LogManager.VisibilitySnapshot visibility = partitionManager.visibilitySnapshot(
                request.topicName(), request.partitionId());
        replicaStateManager.updateReplicaLeo(
                request.topicName(),
                request.partitionId(),
                config.brokerId(),
                visibility.physicalHighWatermark());
        reportReplicaState(
                request.topicName(),
                request.partitionId(),
                config.brokerId(),
                visibility.physicalHighWatermark(),
                visibility.visibleHighWatermark(),
                visibility.nextVisibleAtEpochMs());
        return new ProduceV2Response(request.topicName(), request.partitionId(), baseOffset);
    }

    private ErrorCode topicDeletedError(String topicName) {
        return clusterController.getTopicTombstone(topicName) != null
                ? ErrorCode.UNKNOWN_TOPIC : ErrorCode.NONE;
    }

    private ErrorCode handleReplicaFetchV2(ReplicaFetchV2Request request) {
        ErrorCode leaderError = ensureLeader(request.topicName(), request.partitionId());
        if (leaderError != ErrorCode.NONE) {
            return leaderError;
        }
        ErrorCode fencingError = validateReplicaFetchV2(request);
        if (fencingError == ErrorCode.NONE) {
            replicaStateManager.updateReplicaLeo(
                    request.topicName(),
                    request.partitionId(),
                    request.replicaBrokerId(),
                    request.fetchOffset());
        }
        return fencingError;
    }

    private ErrorCode validateReplicaFetchV2(ReplicaFetchV2Request request) {
        return clusterController.validateReplicaFetchV2(
                request.topicName(),
                request.partitionId(),
                config.brokerId(),
                request.replicaBrokerId(),
                request.brokerEpoch(),
                request.controllerEpoch(),
                request.leaderEpoch());
    }

    private ReplicaFetchV2Response buildReplicaFetchV2Response(ReplicaFetchV2Request request) {
        PartitionLog partitionLog = partitionManager.getOrCreate(request.topicName(), request.partitionId());
        LogManager.VisibilitySnapshot visibility = partitionManager.visibilitySnapshot(
                request.topicName(), request.partitionId());
        List<Record> records = partitionLog.read(request.fetchOffset(), request.maxBytes());
        List<ScheduledReplicaRecord> encoded = new ArrayList<>(
                Math.min(records.size(), MAX_V4_FETCH_RECORDS));
        int total = 0;
        for (Record record : records) {
            if (record.offset() >= visibility.physicalHighWatermark()) {
                break;
            }
            total++;
            if (encoded.size() < MAX_V4_FETCH_RECORDS) {
                encoded.add(new ScheduledReplicaRecord(
                        record.offset(),
                        record.key(),
                        record.value(),
                        partitionManager.deliveryAtEpochMs(
                                request.topicName(), request.partitionId(), record.offset())));
            }
        }
        return new ReplicaFetchV2Response(
                request.topicName(),
                request.partitionId(),
                visibility.physicalHighWatermark(),
                visibility.visibleHighWatermark(),
                visibility.nextVisibleAtEpochMs(),
                request.leaderEpoch(),
                visibility.physicalHighWatermark(),
                new V4BoundedList<>(total, total > encoded.size(), encoded));
    }

    private FetchV2Response buildFetchV2Response(FetchV2Request request) {
        PartitionLog partitionLog = partitionManager.getOrCreate(request.topicName(), request.partitionId());
        LogManager.VisibilitySnapshot visibility = partitionManager.visibilitySnapshot(
                request.topicName(), request.partitionId());
        long replicationHighWatermark = effectiveHighWatermark(
                request.topicName(), request.partitionId(), partitionLog);
        long fetchHighWatermark = Math.min(
                replicationHighWatermark, visibility.visibleHighWatermark());
        long replicatedLimit = Math.min(
                visibility.physicalHighWatermark(), replicationHighWatermark);
        long nextVisibleAtEpochMs = visibility.visibleHighWatermark() < replicatedLimit
                ? visibility.nextVisibleAtEpochMs()
                : -1L;
        List<Record> records = partitionLog.read(request.fetchOffset(), request.maxBytes());
        List<FetchedRecord> encoded = new ArrayList<>(
                Math.min(records.size(), MAX_V4_FETCH_RECORDS));
        int total = 0;
        for (Record record : records) {
            if (record.offset() >= fetchHighWatermark) {
                break;
            }
            total++;
            if (encoded.size() < MAX_V4_FETCH_RECORDS) {
                encoded.add(new FetchedRecord(record.offset(), record.key(), record.value()));
            }
        }
        return new FetchV2Response(
                request.topicName(),
                request.partitionId(),
                visibility.physicalHighWatermark(),
                fetchHighWatermark,
                nextVisibleAtEpochMs,
                new V4BoundedList<>(total, total > encoded.size(), encoded));
    }

    private MetadataV2Response buildMetadataV2Response(MetadataV2Request request) {
        ClusterMetadata snapshot = clusterController.snapshot();
        List<OpsBrokerInfo> brokers = snapshot.brokers.values().stream()
                .sorted(java.util.Comparator.comparingInt(broker -> broker.brokerId))
                .map(broker -> new OpsBrokerInfo(
                        broker.brokerId,
                        broker.host,
                        broker.port,
                        broker.online ? "ACTIVE" : "OFFLINE",
                        broker.brokerEpoch,
                        0L))
                .toList();
        List<TopicSummary> topics = snapshot.topics.values().stream()
                .filter(topic -> request.topicFilter() == null
                        || request.topicFilter().equals(topic.topicName))
                .filter(topic -> request.topicFilter() != null
                        || request.topicStartAfter() == null
                        || topic.topicName.compareTo(request.topicStartAfter()) > 0)
                .sorted(java.util.Comparator.comparing(topic -> topic.topicName))
                .map(topic -> new TopicSummary(
                        topic.topicName,
                        topic.partitions.size(),
                        topic.partitions.isEmpty()
                                ? 0 : topic.partitions.get(0).replicaBrokerIds.size(),
                        snapshot.retryTopicBySource.get(topic.topicName),
                        snapshot.deadLetterTopicBySource.get(topic.topicName)))
                .toList();
        List<TopicTombstoneSummary> tombstones = snapshot.topicTombstones.values().stream()
                .filter(tombstone -> request.topicFilter() == null
                        || request.topicFilter().equals(tombstone.topicName))
                .filter(tombstone -> request.topicFilter() != null
                        || request.tombstoneStartAfter() == null
                        || tombstone.topicName.compareTo(request.tombstoneStartAfter()) > 0)
                .sorted(java.util.Comparator.comparing(tombstone -> tombstone.topicName))
                .map(tombstone -> new TopicTombstoneSummary(
                        tombstone.topicName,
                        toTombstoneInfo(tombstone)))
                .toList();
        var currentLeader = controllerRole.currentLeader();
        ControllerMetadata controller = currentLeader == null
                ? null
                : new ControllerMetadata(
                        currentLeader.brokerId(),
                        currentLeader.host(),
                        currentLeader.port(),
                        snapshot.controllerEpoch);
        return new MetadataV2Response(
                boundedList(brokers, request.topicLimit()),
                boundedList(topics, request.topicLimit()),
                boundedList(tombstones, request.topicLimit()),
                controller,
                snapshot.metadataVersion,
                snapshot.metadataFormatVersion);
    }

    private static <T> V4BoundedList<T> boundedList(List<T> values, int limit) {
        int count = Math.min(values.size(), limit);
        return new V4BoundedList<>(values.size(), count < values.size(), values.subList(0, count));
    }

    private static TopicTombstoneInfo toTombstoneInfo(ClusterMetadata.TopicTombstone tombstone) {
        List<Integer> required = sorted(tombstone.requiredBrokerIds);
        List<Integer> acknowledged = sorted(tombstone.acknowledgedBrokerIds);
        List<Integer> pending = required.stream()
                .filter(id -> !acknowledged.contains(id))
                .toList();
        return new TopicTombstoneInfo(
                tombstone.state,
                tombstone.tombstoneVersion,
                tombstone.deletedAtEpochMs,
                new V4BoundedList<>(required.size(), false, required),
                new V4BoundedList<>(acknowledged.size(), false, acknowledged),
                new V4BoundedList<>(pending.size(), false, pending));
    }

    private static List<Integer> sorted(java.util.Collection<Integer> values) {
        return values == null ? List.of() : values.stream().sorted().toList();
    }

    private ErrorCode handleReplicaFetch(ReplicaFetchRequest request) {
        ErrorCode leaderError = ensureLeader(request.topicName(), request.partitionId());
        if (leaderError != ErrorCode.NONE) {
            return leaderError;
        }
        if (request.replicaBrokerId() == config.brokerId()) {
            return ErrorCode.INVALID_REQUEST;
        }
        if (!clusterController.isReplica(
                request.topicName(), request.partitionId(), request.replicaBrokerId())) {
            return ErrorCode.INVALID_REQUEST;
        }
        return clusterController.validateLeaderEpoch(
                request.topicName(), request.partitionId(), request.leaderEpoch());
    }

    private ReplicaFetchResponse buildReplicaFetchResponse(ReplicaFetchRequest request) {
        PartitionLog partitionLog = partitionManager.getOrCreate(request.topicName(), request.partitionId());
        long leaderHighWatermark = partitionLog.highWatermark();
        List<Record> records = partitionLog.read(request.fetchOffset(), request.maxBytes());
        List<FetchedRecord> fetched = new ArrayList<>(records.size());
        for (Record record : records) {
            if (record.offset() >= leaderHighWatermark) {
                break;
            }
            fetched.add(new FetchedRecord(record.offset(), record.key(), record.value()));
        }
        replicaStateManager.updateReplicaLeo(
                request.topicName(),
                request.partitionId(),
                request.replicaBrokerId(),
                request.fetchOffset());
        reportReplicaState(
                request.topicName(),
                request.partitionId(),
                request.replicaBrokerId(),
                request.fetchOffset());
        long replicationHighWatermark = effectiveHighWatermark(
                request.topicName(), request.partitionId(), partitionLog);
        return new ReplicaFetchResponse(
                request.topicName(),
                request.partitionId(),
                replicationHighWatermark,
                request.leaderEpoch(),
                partitionLog.highWatermark(),
                fetched);
    }

    private long effectiveHighWatermark(String topicName, int partitionId, PartitionLog partitionLog) {
        List<Integer> isrBrokerIds = clusterController.getIsrBrokerIds(topicName, partitionId);
        return replicaStateManager.replicationHighWatermark(
                topicName,
                partitionId,
                partitionLog.highWatermark(),
                isrBrokerIds,
                config.brokerId());
    }

    private ErrorCode ensureLeader(String topicName, int partitionId) {
        int leaderId = clusterController.getLeaderBrokerId(topicName, partitionId);
        if (leaderId < 0 && !controllerRole.isActive()) {
            try {
                MetadataSync.refresh(config, clusterController, controllerRole);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return ErrorCode.INVALID_REQUEST;
            }
            leaderId = clusterController.getLeaderBrokerId(topicName, partitionId);
        }
        if (leaderId < 0) {
            var topic = clusterController.getTopic(topicName);
            if (topic == null) {
                return ErrorCode.UNKNOWN_TOPIC;
            }
            boolean partitionExists = topic.partitions.stream()
                    .anyMatch(partition -> partition.partitionId == partitionId);
            return partitionExists ? ErrorCode.INVALID_REQUEST : ErrorCode.UNKNOWN_PARTITION;
        }
        if (leaderId != config.brokerId()) {
            if (!controllerRole.isActive()
                    && clusterController.isReplica(topicName, partitionId, config.brokerId())) {
                try {
                    MetadataSync.refresh(config, clusterController, controllerRole);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return ErrorCode.INVALID_REQUEST;
                }
                leaderId = clusterController.getLeaderBrokerId(topicName, partitionId);
            }
            if (leaderId != config.brokerId()) {
                return ErrorCode.NOT_LEADER;
            }
        }
        ErrorCode activationError = ensureLocalLeaderReady(topicName, partitionId);
        if (activationError != ErrorCode.NONE) {
            return activationError;
        }
        noteLeadership(topicName, partitionId);
        return ErrorCode.NONE;
    }

    private ErrorCode ensureLocalLeaderReady(String topicName, int partitionId) {
        ClusterMetadata.TopicEntry topic = clusterController.getTopic(topicName);
        if (topic == null) {
            return ErrorCode.UNKNOWN_TOPIC;
        }
        ClusterMetadata.PartitionEntry partition = topic.partitions.stream()
                .filter(candidate -> candidate.partitionId == partitionId)
                .findFirst()
                .orElse(null);
        if (partition == null) {
            return ErrorCode.UNKNOWN_PARTITION;
        }
        if (partition.leaderReady) {
            return ErrorCode.NONE;
        }
        ErrorCode activation = leaderActivationService.activate(
                topicName, partitionId, partition.leaderEpoch, partition.leaderEpochStartOffset);
        if (activation != ErrorCode.NONE) {
            return activation;
        }
        ClusterMetadata.BrokerEntry broker = clusterController.getBroker(config.brokerId());
        long brokerEpoch = broker == null || broker.brokerEpoch <= 0 ? 1L : broker.brokerEpoch;
        LeaderReadyRequest request = new LeaderReadyRequest(
                topicName, partitionId, config.brokerId(), brokerEpoch,
                clusterController.snapshot().controllerEpoch,
                partition.leaderEpoch,
                partitionManager.highWatermark(topicName, partitionId));
        try {
            if (controllerRole.isActive()) {
                return clusterController.leaderReady(request);
            }
            try (ControllerClient client = new ControllerClient()) {
                var identity = controllerRole.currentLeader();
                if (identity == null) {
                    return ErrorCode.LEADER_NOT_READY;
                }
                client.connect(identity.host(), identity.port());
                client.leaderReady(request);
            }
            MetadataSync.refresh(config, clusterController, controllerRole);
            return ErrorCode.NONE;
        } catch (ControllerClient.ControllerRequestException e) {
            return e.errorCode();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return ErrorCode.LEADER_NOT_READY;
        }
    }

    private void noteLeadership(String topicName, int partitionId) {
        LogManager.VisibilitySnapshot visibility =
                partitionManager.visibilitySnapshot(topicName, partitionId);
        replicaStateManager.noteLeadership(
                topicName,
                partitionId,
                config.brokerId(),
                visibility.physicalHighWatermark());
        reportReplicaState(
                topicName,
                partitionId,
                config.brokerId(),
                visibility.physicalHighWatermark(),
                visibility.visibleHighWatermark(),
                visibility.nextVisibleAtEpochMs());
    }

    private void reportReplicaState(String topicName, int partitionId, int brokerId, long logEndOffset) {
        reportReplicaState(topicName, partitionId, brokerId, logEndOffset, logEndOffset, -1L);
    }

    private void reportReplicaState(
            String topicName,
            int partitionId,
            int brokerId,
            long physicalLogEndOffset,
            long visibleLogEndOffset,
            long nextVisibleAtEpochMs) {
        ErrorCode error = replicaStateReporter.report(
                topicName,
                partitionId,
                brokerId,
                physicalLogEndOffset,
                visibleLogEndOffset,
                nextVisibleAtEpochMs);
        if (error != ErrorCode.NONE) {
            log.debug("Deferred replica state update topic={} partition={} broker={} physical={} visible={} error={}",
                    topicName, partitionId, brokerId, physicalLogEndOffset, visibleLogEndOffset, error);
        }
    }

    private void ensureController() {
        if (!controllerRole.isActive()) {
            throw new NotControllerException();
        }
    }

    private DiscoverControllerResponse discoverController() {
        var identity = controllerRole.currentLeader();
        if (identity == null) {
            throw new NotControllerException();
        }
        ControllerMetadata controller = new ControllerMetadata(
                identity.brokerId(), identity.host(), identity.port(), identity.controllerEpoch());
        return new DiscoverControllerResponse(controller, clusterController.snapshot().metadataVersion);
    }

    private static final class NotControllerException extends IllegalStateException {
    }
}
