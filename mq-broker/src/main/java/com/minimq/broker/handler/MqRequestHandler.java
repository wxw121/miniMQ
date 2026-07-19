package com.minimq.broker.handler;

import com.minimq.broker.BrokerConfig;
import com.minimq.broker.FetchTransferMetrics;
import com.minimq.broker.MetadataSync;
import com.minimq.broker.MaintenanceAdminService;
import com.minimq.broker.PartitionManager;
import com.minimq.broker.controller.ClusterController;
import com.minimq.broker.controller.GroupCoordinator;
import com.minimq.broker.replication.ReplicaStateManager;
import com.minimq.broker.replication.ReplicaLeoReporter;
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
import com.minimq.storage.MaintenanceSegmentReport;
import com.minimq.storage.PartitionLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;

/**
 * Broker 请求处理器，将网络帧解码成业务请求，并按 API 类型路由到存储或控制器。
 */
public class MqRequestHandler extends SimpleChannelInboundHandler<ByteBuf> {
    private static final Logger log = LoggerFactory.getLogger(MqRequestHandler.class);
    private final BrokerConfig config;
    private final ClusterController clusterController;
    private final GroupCoordinator groupCoordinator;
    private final PartitionManager partitionManager;
    private final ReplicaStateManager replicaStateManager;
    private final ReplicaLeoReporter replicaLeoReporter;
    private final FetchResponseWriter fetchResponseWriter;
    private final MaintenanceAdminService maintenanceAdminService;

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
                new ReplicaStateManager(),
                new FetchTransferMetrics(),
                new ReplicaLeoReporter(config, clusterController),
                maintenanceAdminService);
    }

    public MqRequestHandler(
            BrokerConfig config,
            ClusterController clusterController,
            GroupCoordinator groupCoordinator,
            PartitionManager partitionManager,
            ReplicaStateManager replicaStateManager,
            FetchTransferMetrics fetchTransferMetrics,
            ReplicaLeoReporter replicaLeoReporter,
            MaintenanceAdminService maintenanceAdminService) {
        this.config = config;
        this.replicaLeoReporter = replicaLeoReporter;
        this.clusterController = clusterController;
        this.groupCoordinator = groupCoordinator;
        this.partitionManager = partitionManager;
        this.replicaStateManager = replicaStateManager;
        this.fetchResponseWriter = new FetchResponseWriter(config, fetchTransferMetrics);
        this.maintenanceAdminService = maintenanceAdminService;
    }

    @Override
    // 单个请求的异常会被转换为协议错误响应，避免异常直接打断 Netty channel。
    protected void channelRead0(ChannelHandlerContext ctx, ByteBuf payload) {
        MqRequestHeader header = null;
        Object responseBody = null;
        ErrorCode errorCode = ErrorCode.NONE;

        try {
            header = ProtocolSerdes.decodeRequestHeader(payload);
            Object requestBody = ProtocolSerdes.decodeRequestBody(header.apiKey(), payload);
            switch (header.apiKey()) {
                case ApiKeys.CREATE_TOPIC -> {
                    ensureController();
                    errorCode = clusterController.createTopic((CreateTopicRequest) requestBody);
                    if (errorCode == ErrorCode.NONE) {
                        responseBody = new CreateTopicResponse(((CreateTopicRequest) requestBody).topicName());
                    }
                }
                case ApiKeys.METADATA -> {
                    ensureController();
                    responseBody = clusterController.metadata((MetadataRequest) requestBody);
                }
                case ApiKeys.REGISTER_BROKER -> {
                    ensureController();
                    var result = clusterController.registerBroker((RegisterBrokerRequest) requestBody);
                    errorCode = result.errorCode();
                    responseBody = result.response();
                }
                case ApiKeys.MARK_BROKER_OFFLINE -> {
                    ensureController();
                    errorCode = clusterController.markBrokerOffline(
                            ((MarkBrokerOfflineRequest) requestBody).brokerId());
                    if (errorCode == ErrorCode.NONE) {
                        responseBody = new MarkBrokerOfflineResponse();
                    }
                }
                case ApiKeys.UPDATE_REPLICA_LEO -> {
                    ensureController();
                    UpdateReplicaLeoRequest request = (UpdateReplicaLeoRequest) requestBody;
                    errorCode = clusterController.updateReplicaLogEndOffset(
                            request.topicName(),
                            request.partitionId(),
                            request.replicaBrokerId(),
                            request.logEndOffset());
                    if (errorCode == ErrorCode.NONE) {
                        responseBody = new UpdateReplicaLeoResponse();
                    }
                }
                case ApiKeys.PRODUCE -> {
                    ProduceRequest request = (ProduceRequest) requestBody;
                    errorCode = ensureLeader(request.topicName(), request.partitionId());
                    if (errorCode == ErrorCode.NONE) {
                        responseBody = handleProduce(request);
                    }
                }
                case ApiKeys.FETCH -> {
                    FetchRequest request = (FetchRequest) requestBody;
                    errorCode = ensureLeader(request.topicName(), request.partitionId());
                    if (errorCode == ErrorCode.NONE) {
                        PartitionLog partitionLog =
                                partitionManager.getOrCreate(request.topicName(), request.partitionId());
                        long effectiveHighWatermark = effectiveHighWatermark(
                                request.topicName(), request.partitionId(), partitionLog);
                        MqResponseHeader fetchHeader =
                                new MqResponseHeader(header.correlationId(), ErrorCode.NONE);
                        fetchResponseWriter.write(
                                ctx, fetchHeader, request, partitionLog, effectiveHighWatermark);
                        return;
                    }
                }
                case ApiKeys.REPLICA_FETCH -> {
                    ReplicaFetchRequest request = (ReplicaFetchRequest) requestBody;
                    errorCode = handleReplicaFetch(request);
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
                            ctx, header, maintenanceAdminService.report(request.adminToken()));
                    return;
                }
                case ApiKeys.MAINTENANCE_RUN -> {
                    MaintenanceRunRequest request = (MaintenanceRunRequest) requestBody;
                    writeAsyncMaintenanceReport(
                            ctx, header, maintenanceAdminService.run(request.adminToken()));
                    return;
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
                default -> errorCode = ErrorCode.INVALID_REQUEST;
            }
        } catch (IllegalArgumentException | IllegalStateException | IndexOutOfBoundsException ex) {
            errorCode = ErrorCode.INVALID_REQUEST;
        } catch (Exception ex) {
            log.error("Request handling failed for apiKey={}", header != null ? header.apiKey() : "unknown", ex);
            errorCode = ErrorCode.INVALID_REQUEST;
        }

        int correlationId = header != null ? header.correlationId() : 0;
        writeResponse(ctx, correlationId, errorCode, responseBody);
    }

    private void writeAsyncMaintenanceReport(
            ChannelHandlerContext ctx,
            MqRequestHeader header,
            CompletionStage<MaintenanceAdminService.Result<LogMaintenanceReport>> stage) {
        stage.whenComplete((result, failure) -> ctx.executor().execute(() -> {
            if (failure != null) {
                log.error("Maintenance request failed for apiKey={}", header.apiKey(), failure);
                writeResponse(ctx, header.correlationId(), ErrorCode.INVALID_REQUEST, null);
                return;
            }
            Object body = result.errorCode() == ErrorCode.NONE
                    ? toMaintenanceReportResponse(result.body())
                    : null;
            writeResponse(ctx, header.correlationId(), result.errorCode(), body);
        }));
    }

    private static void writeResponse(
            ChannelHandlerContext ctx, int correlationId, ErrorCode errorCode, Object responseBody) {
        MqResponseHeader responseHeader = new MqResponseHeader(correlationId, errorCode);
        ByteBuf frame = ProtocolSerdes.encodeResponse(responseHeader, responseBody);
        ctx.writeAndFlush(frame);
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

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("Channel pipeline error", cause);
        ctx.close();
    }

    private ProduceResponse handleProduce(ProduceRequest request) {
        long baseOffset = -1;
        for (ProduceRecord record : request.records()) {
            baseOffset = partitionManager.append(request.topicName(), request.partitionId(), record.key(), record.value());
        }
        PartitionLog partitionLog = partitionManager.getOrCreate(request.topicName(), request.partitionId());
        replicaStateManager.updateReplicaLeo(
                request.topicName(),
                request.partitionId(),
                config.brokerId(),
                partitionLog.highWatermark());
        reportReplicaLeo(
                request.topicName(),
                request.partitionId(),
                config.brokerId(),
                partitionLog.highWatermark());
        return new ProduceResponse(request.topicName(), request.partitionId(), baseOffset);
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
        return ErrorCode.NONE;
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
        reportReplicaLeo(
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
        if (leaderId < 0 && !config.controller()) {
            try {
                MetadataSync.registerAndSync(config, clusterController);
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
            if (!config.controller()
                    && clusterController.isReplica(topicName, partitionId, config.brokerId())) {
                try {
                    MetadataSync.registerAndSync(config, clusterController);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return ErrorCode.INVALID_REQUEST;
                }
                leaderId = clusterController.getLeaderBrokerId(topicName, partitionId);
                if (leaderId == config.brokerId()) {
                    noteLeadership(topicName, partitionId);
                    return ErrorCode.NONE;
                }
            }
            return ErrorCode.NOT_LEADER;
        }
        noteLeadership(topicName, partitionId);
        return ErrorCode.NONE;
    }

    private void noteLeadership(String topicName, int partitionId) {
        long highWatermark = partitionManager.highWatermark(topicName, partitionId);
        replicaStateManager.noteLeadership(
                topicName,
                partitionId,
                config.brokerId(),
                highWatermark);
        reportReplicaLeo(topicName, partitionId, config.brokerId(), highWatermark);
    }

    private void reportReplicaLeo(String topicName, int partitionId, int brokerId, long logEndOffset) {
        if (replicaLeoReporter.report(topicName, partitionId, brokerId, logEndOffset) != ErrorCode.NONE) {
            throw new IllegalStateException("Controller did not accept replica LEO update");
        }
    }

    private void ensureController() {
        if (!config.controller()) {
            throw new IllegalStateException("Not controller");
        }
    }
}
