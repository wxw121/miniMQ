package com.minimq.protocol;

import com.minimq.common.error.ErrorCode;
import com.minimq.protocol.codec.ByteBufSerde;
import com.minimq.protocol.message.MqRequestHeader;
import com.minimq.protocol.message.MqResponseHeader;
import com.minimq.protocol.message.ProtocolMessages;
import com.minimq.protocol.message.ProtocolMessages.*;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * MiniMQ 协议序列化入口，负责请求/响应 body 与完整网络帧之间的双向转换。
 */
public final class ProtocolSerdes {
    private static final int MAX_COLLECTION_ITEMS = 10_000;
    private static final int MAX_SHORT_BYTES = 0xFFFF;
    private static final int MAX_CREDENTIAL_BYTES = 256;

    private ProtocolSerdes() {
    }

    // 请求编码先写业务 payload，再统一包裹 magic、version 和 length 组成完整网络帧。
    public static ByteBuf encodeRequest(MqRequestHeader header, Object body) {
        ByteBuf payload = Unpooled.buffer();
        payload.writeShort(header.apiKey());
        payload.writeInt(header.correlationId());
        ByteBufSerde.writeString(payload, header.clientId());
        writeRequestBody(header.apiKey(), payload, body);
        return wrapFrame(payload);
    }

    public static ByteBuf encodeResponse(MqResponseHeader header, Object body) {
        ByteBuf payload = Unpooled.buffer();
        payload.writeInt(header.correlationId());
        payload.writeShort(header.errorCode().code);
        writeResponseBody(header.errorCode(), payload, body);
        return wrapFrame(payload);
    }

    public static ByteBuf encodeFetchResponsePrefix(
            MqResponseHeader header,
            String topicName,
            int partitionId,
            long highWatermark,
            int recordCount,
            int recordBytes) {
        if (header.errorCode() != ErrorCode.NONE) {
            throw new IllegalArgumentException("fetch prefix requires a successful response header");
        }
        if (recordCount < 0 || recordCount > MAX_COLLECTION_ITEMS || recordBytes < 0) {
            throw new IllegalArgumentException("invalid fetch prefix record count or bytes");
        }
        ByteBuf payload = Unpooled.buffer();
        payload.writeInt(header.correlationId());
        payload.writeShort(header.errorCode().code);
        ByteBufSerde.writeString(payload, topicName);
        payload.writeInt(partitionId);
        payload.writeLong(highWatermark);
        writeItemCount(payload, recordCount, "fetched records");
        return wrapFrame(payload, recordBytes);
    }

    public static MqRequestHeader decodeRequestHeader(ByteBuf payload) {
        requireReadable(payload, Short.BYTES + Integer.BYTES, "request header");
        short apiKey = payload.readShort();
        int correlationId = payload.readInt();
        String clientId = ByteBufSerde.readString(payload);
        return new MqRequestHeader(apiKey, correlationId, clientId);
    }

    // apiKey 决定 body 的结构，新增 API 时必须同步维护这里的解码分支。
    public static Object decodeRequestBody(short apiKey, ByteBuf payload) {
        try {
            return decodeRequestBodyUnchecked(apiKey, payload);
        } catch (IndexOutOfBoundsException e) {
            throw new IllegalArgumentException(
                    "Malformed request body for apiKey " + apiKey, e);
        }
    }

    private static Object decodeRequestBodyUnchecked(short apiKey, ByteBuf payload) {
        Object body = switch (apiKey) {
            case ApiKeys.CREATE_TOPIC -> new CreateTopicRequest(
                    ByteBufSerde.readString(payload), payload.readInt(), payload.readInt());
            case ApiKeys.METADATA -> new MetadataRequest(ByteBufSerde.readString(payload));
            case ApiKeys.PRODUCE -> decodeProduceRequest(payload);
            case ApiKeys.FETCH -> new FetchRequest(
                    ByteBufSerde.readString(payload),
                    payload.readInt(),
                    payload.readLong(),
                    payload.readLong(),
                    payload.readInt());
            case ApiKeys.JOIN_GROUP -> new JoinGroupRequest(
                    ByteBufSerde.readString(payload),
                    ByteBufSerde.readString(payload),
                    ByteBufSerde.readString(payload));
            case ApiKeys.SYNC_GROUP -> new SyncGroupRequest(
                    ByteBufSerde.readString(payload),
                    payload.readInt(),
                    ByteBufSerde.readString(payload),
                    readByteArrayWithShortPrefix(payload));
            case ApiKeys.HEARTBEAT -> decodeHeartbeatRequest(payload);
            case ApiKeys.OFFSET_COMMIT -> decodeOffsetCommitRequest(payload);
            case ApiKeys.OFFSET_FETCH -> new OffsetFetchRequest(
                    ByteBufSerde.readString(payload),
                    ByteBufSerde.readString(payload),
                    payload.readInt());
            case ApiKeys.REGISTER_BROKER -> new RegisterBrokerRequest(
                    payload.readInt(), ByteBufSerde.readString(payload), payload.readInt(), payload.readLong());
            case ApiKeys.BROKER_HEARTBEAT ->
                    new BrokerHeartbeatRequest(payload.readInt(), payload.readLong(), payload.readLong());
            case ApiKeys.REPLICA_FETCH -> new ReplicaFetchRequest(
                    ByteBufSerde.readString(payload),
                    payload.readInt(),
                    payload.readInt(),
                    payload.readLong(),
                    payload.readLong(),
                    payload.readInt());
            case ApiKeys.MARK_BROKER_OFFLINE -> new MarkBrokerOfflineRequest(payload.readInt());
            case ApiKeys.UPDATE_REPLICA_LEO -> new UpdateReplicaLeoRequest(
                    ByteBufSerde.readString(payload),
                    payload.readInt(),
                    payload.readInt(),
                    payload.readLong(),
                    payload.readLong(),
                    payload.readLong());
            case ApiKeys.MAINTENANCE_REPORT ->
                    new MaintenanceReportRequest(ByteBufSerde.readString(payload));
            case ApiKeys.MAINTENANCE_RUN ->
                    new MaintenanceRunRequest(ByteBufSerde.readString(payload));
            case ApiKeys.MAINTENANCE_METRICS ->
                    new MaintenanceMetricsRequest(ByteBufSerde.readString(payload));
            case ApiKeys.RUNTIME_METRICS ->
                    new RuntimeMetricsRequest(ByteBufSerde.readString(payload));
            case ApiKeys.DISCOVER_CONTROLLER -> new DiscoverControllerRequest();
            case ApiKeys.LEADER_READY -> new LeaderReadyRequest(
                    ByteBufSerde.readString(payload),
                    payload.readInt(),
                    payload.readInt(),
                    payload.readLong(),
                    payload.readLong(),
                    payload.readLong(),
                    payload.readLong());
            case ApiKeys.OPS_STATUS -> new OpsStatusRequest(
                    readV4Credential(payload), payload.readInt(), payload.readInt());
            case ApiKeys.GROUP_STATUS -> new GroupStatusRequest(
                    readV4Credential(payload),
                    readV4String(payload, "group id"),
                    readOptionalString(payload, "topic filter"),
                    payload.readInt(),
                    payload.readInt());
            case ApiKeys.LIST_TOPICS ->
                    new ListTopicsRequest(readV4Credential(payload), payload.readInt());
            case ApiKeys.DESCRIBE_TOPIC -> new DescribeTopicRequest(
                    readV4Credential(payload),
                    readV4String(payload, "topic name"),
                    payload.readInt(),
                    readBoolean(payload, "includeDeleted"));
            case ApiKeys.DELETE_TOPIC -> new DeleteTopicRequest(
                    readV4Credential(payload), readV4String(payload, "topic name"));
            case ApiKeys.INCREASE_PARTITIONS -> new IncreasePartitionsRequest(
                    readV4Credential(payload),
                    readV4String(payload, "topic name"),
                    payload.readInt());
            case ApiKeys.FAILOVER_AUDIT ->
                    new FailoverAuditRequest(readV4Credential(payload), payload.readInt());
            case ApiKeys.AUTH_STATUS -> new AuthStatusRequest(readV4Credential(payload));
            case ApiKeys.CONFIGURE_TOPIC_RELIABILITY ->
                    decodeConfigureTopicReliabilityRequest(payload);
            case ApiKeys.MARK_BROKER_OFFLINE_ADMIN -> new MarkBrokerOfflineAdminRequest(
                    readV4Credential(payload), payload.readInt());
            case ApiKeys.CREATE_TOPIC_ADMIN -> new CreateTopicAdminRequest(
                    readV4Credential(payload),
                    readV4String(payload, "topic name"),
                    payload.readInt(),
                    payload.readInt());
            case ApiKeys.PRODUCE_V2 -> decodeProduceV2Request(payload);
            case ApiKeys.REPLICA_FETCH_V2 -> new ReplicaFetchV2Request(
                    readV4Credential(payload),
                    readV4String(payload, "topic name"),
                    payload.readInt(),
                    payload.readInt(),
                    payload.readLong(),
                    payload.readLong(),
                    payload.readLong(),
                    payload.readLong(),
                    payload.readInt());
            case ApiKeys.TOPIC_DELETE_ACK -> new TopicDeleteAckRequest(
                    readV4Credential(payload),
                    readV4String(payload, "topic name"),
                    payload.readLong(),
                    payload.readInt(),
                    payload.readLong(),
                    payload.readLong());
            case ApiKeys.FETCH_V2 -> new FetchV2Request(
                    readV4String(payload, "topic name"),
                    payload.readInt(),
                    payload.readLong(),
                    payload.readLong(),
                    payload.readInt());
            case ApiKeys.METADATA_V2 -> new MetadataV2Request(
                    readOptionalString(payload, "topic filter"),
                    readOptionalString(payload, "topic start-after"),
                    readOptionalString(payload, "tombstone start-after"),
                    payload.readInt(),
                    payload.readInt());
            case ApiKeys.RUNTIME_METRICS_V2 -> new RuntimeMetricsV2Request(
                    readV4Credential(payload),
                    payload.readInt(),
                    payload.readInt(),
                    payload.readInt());
            case ApiKeys.UPDATE_REPLICA_STATE_V2 -> new UpdateReplicaStateV2Request(
                    readV4Credential(payload),
                    readV4String(payload, "topic name"),
                    payload.readInt(),
                    payload.readInt(),
                    payload.readLong(),
                    payload.readLong(),
                    payload.readLong(),
                    payload.readLong(),
                    payload.readLong(),
                    payload.readLong());
            default -> throw new IllegalArgumentException("Unknown apiKey: " + apiKey);
        };
        requireFullyConsumed(payload, "request body for apiKey " + apiKey);
        return body;
    }

    public static String peekRequestCredential(short apiKey, ByteBuf payload) {
        if (!hasCredentialPrefix(apiKey)) {
            throw new IllegalArgumentException(
                    "apiKey does not carry a credential prefix: " + apiKey);
        }
        if (payload.readableBytes() > MqFrame.MAX_FRAME_BYTES) {
            throw new IllegalArgumentException(
                    "Request body is too large: " + payload.readableBytes());
        }
        int readerIndex = payload.readerIndex();
        try {
            return readCredentialPrefix(payload);
        } finally {
            payload.readerIndex(readerIndex);
        }
    }

    public static MqResponseHeader decodeResponseHeader(ByteBuf payload) {
        requireReadable(payload, Integer.BYTES + Short.BYTES, "response header");
        int correlationId = payload.readInt();
        ErrorCode errorCode = ErrorCode.fromCode(payload.readShort());
        return new MqResponseHeader(correlationId, errorCode);
    }

    public static Object decodeResponseBody(short apiKey, ByteBuf payload) {
        try {
            return decodeResponseBodyUnchecked(apiKey, payload);
        } catch (IndexOutOfBoundsException e) {
            throw new IllegalArgumentException(
                    "Malformed response body for apiKey " + apiKey, e);
        }
    }

    private static Object decodeResponseBodyUnchecked(short apiKey, ByteBuf payload) {
        Object body = switch (apiKey) {
            case ApiKeys.CREATE_TOPIC -> new CreateTopicResponse(ByteBufSerde.readString(payload));
            case ApiKeys.METADATA -> decodeMetadataResponse(payload);
            case ApiKeys.PRODUCE -> new ProduceResponse(
                    ByteBufSerde.readString(payload), payload.readInt(), payload.readLong());
            case ApiKeys.FETCH -> decodeFetchResponse(payload);
            case ApiKeys.JOIN_GROUP -> decodeJoinGroupResponse(payload);
            case ApiKeys.SYNC_GROUP -> new SyncGroupResponse(readByteArrayWithShortPrefix(payload));
            case ApiKeys.HEARTBEAT -> new HeartbeatResponse();
            case ApiKeys.OFFSET_COMMIT -> new OffsetCommitResponse();
            case ApiKeys.OFFSET_FETCH -> new OffsetFetchResponse(payload.readLong());
            case ApiKeys.REGISTER_BROKER ->
                    new RegisterBrokerResponse(payload.readLong(), payload.readLong());
            case ApiKeys.BROKER_HEARTBEAT -> new BrokerHeartbeatResponse();
            case ApiKeys.REPLICA_FETCH -> decodeReplicaFetchResponse(payload);
            case ApiKeys.MARK_BROKER_OFFLINE -> new MarkBrokerOfflineResponse();
            case ApiKeys.UPDATE_REPLICA_LEO -> new UpdateReplicaLeoResponse();
            case ApiKeys.MAINTENANCE_REPORT, ApiKeys.MAINTENANCE_RUN -> decodeMaintenanceReportResponse(payload);
            case ApiKeys.MAINTENANCE_METRICS -> decodeMaintenanceMetricsResponse(payload);
            case ApiKeys.RUNTIME_METRICS -> decodeRuntimeMetricsResponse(payload);
            case ApiKeys.DISCOVER_CONTROLLER -> new DiscoverControllerResponse(
                    readControllerMetadata(payload), payload.readLong());
            case ApiKeys.LEADER_READY -> new LeaderReadyResponse();
            case ApiKeys.OPS_STATUS -> decodeOpsStatusResponse(payload);
            case ApiKeys.GROUP_STATUS -> decodeGroupStatusResponse(payload);
            case ApiKeys.LIST_TOPICS -> new ListTopicsResponse(
                    readBoundedList(
                            payload,
                            "topic names",
                            target -> readV4String(target, "topic name")));
            case ApiKeys.DESCRIBE_TOPIC -> decodeDescribeTopicResponse(payload);
            case ApiKeys.DELETE_TOPIC -> decodeDeleteTopicResponse(payload);
            case ApiKeys.INCREASE_PARTITIONS -> new IncreasePartitionsResponse(
                    readV4String(payload, "topic name"),
                    payload.readInt(),
                    payload.readInt(),
                    payload.readLong());
            case ApiKeys.FAILOVER_AUDIT -> decodeFailoverAuditResponse(payload);
            case ApiKeys.AUTH_STATUS -> new AuthStatusResponse(
                    payload.readInt(),
                    readBoundedList(
                            payload,
                            "auth scopes",
                            target -> readV4String(target, "auth scope")));
            case ApiKeys.CONFIGURE_TOPIC_RELIABILITY ->
                    new ConfigureTopicReliabilityResponse(
                            readV4String(payload, "source topic name"),
                            readOptionalString(payload, "retry topic"),
                            readOptionalString(payload, "dead-letter topic"),
                            payload.readLong());
            case ApiKeys.MARK_BROKER_OFFLINE_ADMIN ->
                    new MarkBrokerOfflineAdminResponse(
                            payload.readInt(), payload.readLong());
            case ApiKeys.CREATE_TOPIC_ADMIN -> new CreateTopicAdminResponse(
                    readV4String(payload, "topic name"), payload.readLong());
            case ApiKeys.PRODUCE_V2 -> new ProduceV2Response(
                    readV4String(payload, "topic name"), payload.readInt(), payload.readLong());
            case ApiKeys.REPLICA_FETCH_V2 -> decodeReplicaFetchV2Response(payload);
            case ApiKeys.TOPIC_DELETE_ACK -> new TopicDeleteAckResponse();
            case ApiKeys.FETCH_V2 -> decodeFetchV2Response(payload);
            case ApiKeys.METADATA_V2 -> decodeMetadataV2Response(payload);
            case ApiKeys.RUNTIME_METRICS_V2 -> decodeRuntimeMetricsV2Response(payload);
            case ApiKeys.UPDATE_REPLICA_STATE_V2 -> new UpdateReplicaStateV2Response();
            default -> throw new IllegalArgumentException("Unknown apiKey: " + apiKey);
        };
        requireFullyConsumed(payload, "response body for apiKey " + apiKey);
        return body;
    }

    private static void writeRequestBody(short apiKey, ByteBuf buf, Object body) {
        switch (apiKey) {
            case ApiKeys.CREATE_TOPIC -> {
                CreateTopicRequest req = (CreateTopicRequest) body;
                ByteBufSerde.writeString(buf, req.topicName());
                buf.writeInt(req.numPartitions());
                buf.writeInt(req.replicationFactor());
            }
            case ApiKeys.METADATA -> ByteBufSerde.writeString(buf, ((MetadataRequest) body).topicName());
            case ApiKeys.PRODUCE -> writeProduceRequest(buf, (ProduceRequest) body);
            case ApiKeys.FETCH -> {
                FetchRequest req = (FetchRequest) body;
                ByteBufSerde.writeString(buf, req.topicName());
                buf.writeInt(req.partitionId());
                buf.writeLong(req.leaderEpoch());
                buf.writeLong(req.fetchOffset());
                buf.writeInt(req.maxBytes());
            }
            case ApiKeys.JOIN_GROUP -> {
                JoinGroupRequest req = (JoinGroupRequest) body;
                ByteBufSerde.writeString(buf, req.groupId());
                ByteBufSerde.writeString(buf, req.memberId());
                ByteBufSerde.writeString(buf, req.topicName());
            }
            case ApiKeys.SYNC_GROUP -> {
                SyncGroupRequest req = (SyncGroupRequest) body;
                ByteBufSerde.writeString(buf, req.groupId());
                buf.writeInt(req.generationId());
                ByteBufSerde.writeString(buf, req.memberId());
                writeByteArrayWithShortPrefix(buf, req.assignment());
            }
            case ApiKeys.HEARTBEAT -> {
                HeartbeatRequest req = (HeartbeatRequest) body;
                ByteBufSerde.writeString(buf, req.groupId());
                ByteBufSerde.writeString(buf, req.memberId());
                buf.writeInt(req.generationId());
            }
            case ApiKeys.OFFSET_COMMIT -> {
                OffsetCommitRequest req = (OffsetCommitRequest) body;
                ByteBufSerde.writeString(buf, req.groupId());
                ByteBufSerde.writeString(buf, req.topicName());
                buf.writeInt(req.partitionId());
                buf.writeLong(req.offset());
                ByteBufSerde.writeString(buf, req.memberId());
                buf.writeInt(req.generationId());
            }
            case ApiKeys.OFFSET_FETCH -> {
                OffsetFetchRequest req = (OffsetFetchRequest) body;
                ByteBufSerde.writeString(buf, req.groupId());
                ByteBufSerde.writeString(buf, req.topicName());
                buf.writeInt(req.partitionId());
            }
            case ApiKeys.REGISTER_BROKER -> {
                RegisterBrokerRequest req = (RegisterBrokerRequest) body;
                buf.writeInt(req.brokerId());
                ByteBufSerde.writeString(buf, req.host());
                buf.writeInt(req.port());
                buf.writeLong(req.controllerEpoch());
            }
            case ApiKeys.BROKER_HEARTBEAT -> {
                BrokerHeartbeatRequest req = (BrokerHeartbeatRequest) body;
                buf.writeInt(req.brokerId());
                buf.writeLong(req.brokerEpoch());
                buf.writeLong(req.controllerEpoch());
            }
            case ApiKeys.REPLICA_FETCH -> writeReplicaFetchRequest(buf, (ReplicaFetchRequest) body);
            case ApiKeys.MARK_BROKER_OFFLINE -> buf.writeInt(((MarkBrokerOfflineRequest) body).brokerId());
            case ApiKeys.UPDATE_REPLICA_LEO -> {
                UpdateReplicaLeoRequest req = (UpdateReplicaLeoRequest) body;
                ByteBufSerde.writeString(buf, req.topicName());
                buf.writeInt(req.partitionId());
                buf.writeInt(req.replicaBrokerId());
                buf.writeLong(req.brokerEpoch());
                buf.writeLong(req.controllerEpoch());
                buf.writeLong(req.logEndOffset());
            }
            case ApiKeys.MAINTENANCE_REPORT ->
                    ByteBufSerde.writeString(buf, ((MaintenanceReportRequest) body).adminToken());
            case ApiKeys.MAINTENANCE_RUN ->
                    ByteBufSerde.writeString(buf, ((MaintenanceRunRequest) body).adminToken());
            case ApiKeys.MAINTENANCE_METRICS ->
                    ByteBufSerde.writeString(buf, ((MaintenanceMetricsRequest) body).adminToken());
            case ApiKeys.RUNTIME_METRICS ->
                    ByteBufSerde.writeString(buf, ((RuntimeMetricsRequest) body).adminToken());
            case ApiKeys.DISCOVER_CONTROLLER -> {
                // Empty request body.
            }
            case ApiKeys.LEADER_READY -> {
                LeaderReadyRequest req = (LeaderReadyRequest) body;
                ByteBufSerde.writeString(buf, req.topicName());
                buf.writeInt(req.partitionId());
                buf.writeInt(req.brokerId());
                buf.writeLong(req.brokerEpoch());
                buf.writeLong(req.controllerEpoch());
                buf.writeLong(req.leaderEpoch());
                buf.writeLong(req.logEndOffset());
            }
            case ApiKeys.OPS_STATUS -> {
                OpsStatusRequest req = (OpsStatusRequest) body;
                ByteBufSerde.writeString(buf, req.credential());
                buf.writeInt(req.brokerLimit());
                buf.writeInt(req.partitionLimit());
            }
            case ApiKeys.GROUP_STATUS -> {
                GroupStatusRequest req = (GroupStatusRequest) body;
                ByteBufSerde.writeString(buf, req.credential());
                ByteBufSerde.writeString(buf, req.groupId());
                writeOptionalString(buf, req.topicFilter());
                buf.writeInt(req.memberLimit());
                buf.writeInt(req.partitionLimit());
            }
            case ApiKeys.LIST_TOPICS -> {
                ListTopicsRequest req = (ListTopicsRequest) body;
                ByteBufSerde.writeString(buf, req.credential());
                buf.writeInt(req.limit());
            }
            case ApiKeys.DESCRIBE_TOPIC -> {
                DescribeTopicRequest req = (DescribeTopicRequest) body;
                ByteBufSerde.writeString(buf, req.credential());
                ByteBufSerde.writeString(buf, req.topicName());
                buf.writeInt(req.partitionLimit());
                writeBoolean(buf, req.includeDeleted());
            }
            case ApiKeys.DELETE_TOPIC -> {
                DeleteTopicRequest req = (DeleteTopicRequest) body;
                ByteBufSerde.writeString(buf, req.credential());
                ByteBufSerde.writeString(buf, req.topicName());
            }
            case ApiKeys.INCREASE_PARTITIONS -> {
                IncreasePartitionsRequest req = (IncreasePartitionsRequest) body;
                ByteBufSerde.writeString(buf, req.credential());
                ByteBufSerde.writeString(buf, req.topicName());
                buf.writeInt(req.targetPartitionCount());
            }
            case ApiKeys.FAILOVER_AUDIT -> {
                FailoverAuditRequest req = (FailoverAuditRequest) body;
                ByteBufSerde.writeString(buf, req.credential());
                buf.writeInt(req.limit());
            }
            case ApiKeys.AUTH_STATUS ->
                    ByteBufSerde.writeString(buf, ((AuthStatusRequest) body).credential());
            case ApiKeys.CONFIGURE_TOPIC_RELIABILITY ->
                    writeConfigureTopicReliabilityRequest(
                            buf, (ConfigureTopicReliabilityRequest) body);
            case ApiKeys.MARK_BROKER_OFFLINE_ADMIN -> {
                MarkBrokerOfflineAdminRequest req = (MarkBrokerOfflineAdminRequest) body;
                ByteBufSerde.writeString(buf, req.credential());
                buf.writeInt(req.brokerId());
            }
            case ApiKeys.CREATE_TOPIC_ADMIN -> {
                CreateTopicAdminRequest req = (CreateTopicAdminRequest) body;
                ByteBufSerde.writeString(buf, req.credential());
                ByteBufSerde.writeString(buf, req.topicName());
                buf.writeInt(req.numPartitions());
                buf.writeInt(req.replicationFactor());
            }
            case ApiKeys.PRODUCE_V2 -> writeProduceV2Request(buf, (ProduceV2Request) body);
            case ApiKeys.REPLICA_FETCH_V2 -> {
                ReplicaFetchV2Request req = (ReplicaFetchV2Request) body;
                ByteBufSerde.writeString(buf, req.clusterCredential());
                ByteBufSerde.writeString(buf, req.topicName());
                buf.writeInt(req.partitionId());
                buf.writeInt(req.replicaBrokerId());
                buf.writeLong(req.brokerEpoch());
                buf.writeLong(req.controllerEpoch());
                buf.writeLong(req.leaderEpoch());
                buf.writeLong(req.fetchOffset());
                buf.writeInt(req.maxBytes());
            }
            case ApiKeys.TOPIC_DELETE_ACK -> {
                TopicDeleteAckRequest req = (TopicDeleteAckRequest) body;
                ByteBufSerde.writeString(buf, req.clusterCredential());
                ByteBufSerde.writeString(buf, req.topicName());
                buf.writeLong(req.tombstoneVersion());
                buf.writeInt(req.brokerId());
                buf.writeLong(req.brokerEpoch());
                buf.writeLong(req.controllerEpoch());
            }
            case ApiKeys.FETCH_V2 -> {
                FetchV2Request req = (FetchV2Request) body;
                ByteBufSerde.writeString(buf, req.topicName());
                buf.writeInt(req.partitionId());
                buf.writeLong(req.leaderEpoch());
                buf.writeLong(req.fetchOffset());
                buf.writeInt(req.maxBytes());
            }
            case ApiKeys.METADATA_V2 -> {
                MetadataV2Request req = (MetadataV2Request) body;
                writeOptionalString(buf, req.topicFilter());
                writeOptionalString(buf, req.topicStartAfter());
                writeOptionalString(buf, req.tombstoneStartAfter());
                buf.writeInt(req.topicLimit());
                buf.writeInt(req.partitionLimit());
            }
            case ApiKeys.RUNTIME_METRICS_V2 -> {
                RuntimeMetricsV2Request req = (RuntimeMetricsV2Request) body;
                ByteBufSerde.writeString(buf, req.credential());
                buf.writeInt(req.counterLimit());
                buf.writeInt(req.latencyLimit());
                buf.writeInt(req.lagLimit());
            }
            case ApiKeys.UPDATE_REPLICA_STATE_V2 -> {
                UpdateReplicaStateV2Request req = (UpdateReplicaStateV2Request) body;
                ByteBufSerde.writeString(buf, req.clusterCredential());
                ByteBufSerde.writeString(buf, req.topicName());
                buf.writeInt(req.partitionId());
                buf.writeInt(req.replicaBrokerId());
                buf.writeLong(req.brokerEpoch());
                buf.writeLong(req.controllerEpoch());
                buf.writeLong(req.leaderEpoch());
                buf.writeLong(req.physicalLogEndOffset());
                buf.writeLong(req.visibleLogEndOffset());
                buf.writeLong(req.nextVisibleAtEpochMs());
            }
            default -> throw new IllegalArgumentException("Unknown apiKey: " + apiKey);
        }
    }

    private static void writeResponseBody(ErrorCode errorCode, ByteBuf buf, Object body) {
        if (errorCode != ErrorCode.NONE) {
            return;
        }
        if (body == null) {
            throw new IllegalArgumentException("Successful response requires a body object");
        }
        if (body instanceof CreateTopicResponse res) {
            ByteBufSerde.writeString(buf, res.topicName());
        } else if (body instanceof MetadataResponse res) {
            writeMetadataResponse(buf, res);
        } else if (body instanceof ProduceResponse res) {
            ByteBufSerde.writeString(buf, res.topicName());
            buf.writeInt(res.partitionId());
            buf.writeLong(res.baseOffset());
        } else if (body instanceof FetchResponse res) {
            writeFetchResponse(buf, res);
        } else if (body instanceof JoinGroupResponse res) {
            writeJoinGroupResponse(buf, res);
        } else if (body instanceof SyncGroupResponse res) {
            writeByteArrayWithShortPrefix(buf, res.assignment());
        } else if (body instanceof OffsetFetchResponse res) {
            buf.writeLong(res.offset());
        } else if (body instanceof RegisterBrokerResponse res) {
            buf.writeLong(res.brokerEpoch());
            buf.writeLong(res.leaseTimeoutMs());
        } else if (body instanceof BrokerHeartbeatResponse) {
            // Successful Broker heartbeat responses have no body fields.
        } else if (body instanceof ReplicaFetchResponse res) {
            writeReplicaFetchResponse(buf, res);
        } else if (body instanceof MaintenanceReportResponse res) {
            writeMaintenanceReportResponse(buf, res);
        } else if (body instanceof MaintenanceMetricsResponse res) {
            writeMaintenanceMetricsResponse(buf, res);
        } else if (body instanceof RuntimeMetricsResponse res) {
            writeRuntimeMetricsResponse(buf, res);
        } else if (body instanceof DiscoverControllerResponse res) {
            writeControllerMetadata(buf, res.controller());
            buf.writeLong(res.metadataVersion());
        } else if (body instanceof LeaderReadyResponse) {
            // Successful Leader ready responses have no body fields.
        } else if (body instanceof OpsStatusResponse res) {
            writeOpsStatusResponse(buf, res);
        } else if (body instanceof GroupStatusResponse res) {
            writeGroupStatusResponse(buf, res);
        } else if (body instanceof ListTopicsResponse res) {
            writeBoundedList(
                    buf,
                    res.topicNames(),
                    "topic names",
                    (target, value) -> ByteBufSerde.writeString(target, value));
        } else if (body instanceof DescribeTopicResponse res) {
            writeDescribeTopicResponse(buf, res);
        } else if (body instanceof DeleteTopicResponse res) {
            writeDeleteTopicResponse(buf, res);
        } else if (body instanceof IncreasePartitionsResponse res) {
            ByteBufSerde.writeString(buf, res.topicName());
            buf.writeInt(res.previousPartitionCount());
            buf.writeInt(res.newPartitionCount());
            buf.writeLong(res.metadataVersion());
        } else if (body instanceof FailoverAuditResponse res) {
            writeFailoverAuditResponse(buf, res);
        } else if (body instanceof AuthStatusResponse res) {
            buf.writeInt(res.targetBrokerId());
            writeBoundedList(
                    buf,
                    res.scopes(),
                    "auth scopes",
                    (target, value) -> ByteBufSerde.writeString(target, value));
        } else if (body instanceof ConfigureTopicReliabilityResponse res) {
            ByteBufSerde.writeString(buf, res.sourceTopicName());
            writeOptionalString(buf, res.retryTopicName());
            writeOptionalString(buf, res.deadLetterTopicName());
            buf.writeLong(res.metadataVersion());
        } else if (body instanceof MarkBrokerOfflineAdminResponse res) {
            buf.writeInt(res.brokerId());
            buf.writeLong(res.metadataVersion());
        } else if (body instanceof CreateTopicAdminResponse res) {
            ByteBufSerde.writeString(buf, res.topicName());
            buf.writeLong(res.metadataVersion());
        } else if (body instanceof ProduceV2Response res) {
            ByteBufSerde.writeString(buf, res.topicName());
            buf.writeInt(res.partitionId());
            buf.writeLong(res.baseOffset());
        } else if (body instanceof ReplicaFetchV2Response res) {
            writeReplicaFetchV2Response(buf, res);
        } else if (body instanceof FetchV2Response res) {
            writeFetchV2Response(buf, res);
        } else if (body instanceof MetadataV2Response res) {
            writeMetadataV2Response(buf, res);
        } else if (body instanceof RuntimeMetricsV2Response res) {
            writeRuntimeMetricsV2Response(buf, res);
        } else if (body instanceof HeartbeatResponse
                || body instanceof OffsetCommitResponse
                || body instanceof MarkBrokerOfflineResponse
                || body instanceof UpdateReplicaLeoResponse
                || body instanceof TopicDeleteAckResponse
                || body instanceof UpdateReplicaStateV2Response) {
            // Successful acknowledgement responses have no body fields.
        } else {
            throw new IllegalArgumentException(
                    "Unknown response body type: " + body.getClass().getName());
        }
    }

    private static ByteBuf wrapFrame(ByteBuf payload) {
        return wrapFrame(payload, 0);
    }

    private static ByteBuf wrapFrame(ByteBuf payload, int trailingBytes) {
        final int length;
        try {
            length = Math.addExact(payload.readableBytes(), trailingBytes);
        } catch (ArithmeticException overflow) {
            payload.release();
            throw new IllegalArgumentException("Frame length overflow", overflow);
        }
        if (trailingBytes < 0 || length > MqFrame.MAX_FRAME_BYTES) {
            payload.release();
            throw new IllegalArgumentException("Frame is too large: " + length);
        }
        ByteBuf frame = Unpooled.buffer(2 + 1 + 4 + payload.readableBytes());
        frame.writeShort(MqFrame.MAGIC);
        frame.writeByte(MqFrame.VERSION);
        frame.writeInt(length);
        frame.writeBytes(payload);
        payload.release();
        return frame;
    }

    private static ProduceRequest decodeProduceRequest(ByteBuf buf) {
        String topic = ByteBufSerde.readString(buf);
        requireReadable(buf, Integer.BYTES * 2 + Long.BYTES, "produce request");
        int partitionId = buf.readInt();
        long leaderEpoch = buf.readLong();
        int count = readItemCount(buf, "produce records");
        List<ProduceRecord> records = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            records.add(new ProduceRecord(ByteBufSerde.readBytes(buf), ByteBufSerde.readBytes(buf)));
        }
        return new ProduceRequest(topic, partitionId, leaderEpoch, records);
    }

    private static void writeProduceRequest(ByteBuf buf, ProduceRequest req) {
        ByteBufSerde.writeString(buf, req.topicName());
        buf.writeInt(req.partitionId());
        buf.writeLong(req.leaderEpoch());
        writeItemCount(buf, req.records().size(), "produce records");
        for (ProduceRecord record : req.records()) {
            ByteBufSerde.writeBytes(buf, record.key());
            ByteBufSerde.writeBytes(buf, record.value());
        }
    }

    private static ProduceV2Request decodeProduceV2Request(ByteBuf buf) {
        String topic = readV4String(buf, "topic name");
        requireReadable(buf, Integer.BYTES + Long.BYTES, "produce v2 request");
        int partitionId = buf.readInt();
        long leaderEpoch = buf.readLong();
        V4BoundedList<ScheduledProduceRecord> records = readBoundedList(
                buf,
                "scheduled produce records",
                ProtocolSerdes::readScheduledProduceRecord);
        return new ProduceV2Request(topic, partitionId, leaderEpoch, records);
    }

    private static void writeProduceV2Request(ByteBuf buf, ProduceV2Request req) {
        ByteBufSerde.writeString(buf, req.topicName());
        buf.writeInt(req.partitionId());
        buf.writeLong(req.leaderEpoch());
        writeBoundedList(
                buf,
                req.records(),
                "scheduled produce records",
                ProtocolSerdes::writeScheduledProduceRecord);
    }

    private static ScheduledProduceRecord readScheduledProduceRecord(ByteBuf buf) {
        byte[] key = ByteBufSerde.readBytes(buf);
        byte[] value = ByteBufSerde.readBytes(buf);
        requireReadable(buf, Long.BYTES, "scheduled produce timestamp");
        return new ScheduledProduceRecord(key, value, buf.readLong());
    }

    private static void writeScheduledProduceRecord(ByteBuf buf, ScheduledProduceRecord record) {
        ByteBufSerde.writeBytes(buf, record.key());
        ByteBufSerde.writeBytes(buf, record.value());
        buf.writeLong(record.deliverAtEpochMs());
    }

    private static ConfigureTopicReliabilityRequest decodeConfigureTopicReliabilityRequest(
            ByteBuf buf) {
        String credential = readV4Credential(buf);
        String sourceTopicName = readV4String(buf, "source topic name");
        boolean retryTopicSupplied = readBoolean(buf, "retryTopicSupplied");
        String retryTopicName =
                retryTopicSupplied ? readV4String(buf, "retry topic name") : null;
        boolean clearRetryTopic = readBoolean(buf, "clearRetryTopic");
        boolean deadLetterTopicSupplied = readBoolean(buf, "deadLetterTopicSupplied");
        String deadLetterTopicName =
                deadLetterTopicSupplied ? readV4String(buf, "dead-letter topic name") : null;
        boolean clearDeadLetterTopic = readBoolean(buf, "clearDeadLetterTopic");
        return new ConfigureTopicReliabilityRequest(
                credential,
                sourceTopicName,
                retryTopicSupplied,
                retryTopicName,
                clearRetryTopic,
                deadLetterTopicSupplied,
                deadLetterTopicName,
                clearDeadLetterTopic);
    }

    private static void writeConfigureTopicReliabilityRequest(
            ByteBuf buf, ConfigureTopicReliabilityRequest req) {
        ByteBufSerde.writeString(buf, req.credential());
        ByteBufSerde.writeString(buf, req.sourceTopicName());
        writeBoolean(buf, req.retryTopicSupplied());
        if (req.retryTopicSupplied()) {
            ByteBufSerde.writeString(buf, req.retryTopicName());
        }
        writeBoolean(buf, req.clearRetryTopic());
        writeBoolean(buf, req.deadLetterTopicSupplied());
        if (req.deadLetterTopicSupplied()) {
            ByteBufSerde.writeString(buf, req.deadLetterTopicName());
        }
        writeBoolean(buf, req.clearDeadLetterTopic());
    }

    private static MetadataResponse decodeMetadataResponse(ByteBuf buf) {
        int brokerCount = readItemCount(buf, "brokers");
        List<BrokerMetadata> brokers = new ArrayList<>();
        for (int i = 0; i < brokerCount; i++) {
            requireReadable(buf, Integer.BYTES, "broker id");
            int brokerId = buf.readInt();
            String host = ByteBufSerde.readString(buf);
            requireReadable(buf, Integer.BYTES, "broker port");
            int port = buf.readInt();
            brokers.add(new BrokerMetadata(brokerId, host, port));
        }
        int topicCount = readItemCount(buf, "topics");
        List<TopicMetadata> topics = new ArrayList<>();
        for (int i = 0; i < topicCount; i++) {
            String topicName = ByteBufSerde.readString(buf);
            int partitionCount = readItemCount(buf, "partitions");
            List<PartitionMetadata> partitions = new ArrayList<>();
            for (int j = 0; j < partitionCount; j++) {
                requireReadable(buf, Integer.BYTES * 2, "partition metadata");
                int partitionId = buf.readInt();
                int leaderBrokerId = buf.readInt();
                List<Integer> replicaBrokerIds = readBrokerIdList(buf, "replica brokers");
                List<Integer> isrBrokerIds = readBrokerIdList(buf, "isr brokers");
                Map<Integer, Long> replicaLogEndOffsets = readReplicaLogEndOffsets(buf);
                requireReadable(buf, 1 + Long.BYTES * 2 + 1, "partition epoch metadata");
                boolean frozen = buf.readBoolean();
                long leaderEpoch = buf.readLong();
                long leaderEpochStartOffset = buf.readLong();
                boolean leaderReady = buf.readBoolean();
                partitions.add(new PartitionMetadata(
                        partitionId,
                        leaderBrokerId,
                        replicaBrokerIds,
                        isrBrokerIds,
                        replicaLogEndOffsets,
                        frozen,
                        leaderEpoch,
                        leaderEpochStartOffset,
                        leaderReady));
            }
            topics.add(new TopicMetadata(topicName, partitions));
        }
        requireReadable(buf, 1, "controller metadata presence");
        ControllerMetadata controller = buf.readBoolean() ? readControllerMetadata(buf) : null;
        requireReadable(buf, Long.BYTES, "metadata version");
        return new MetadataResponse(brokers, topics, controller, buf.readLong());
    }

    private static void writeMetadataResponse(ByteBuf buf, MetadataResponse res) {
        writeItemCount(buf, res.brokers().size(), "brokers");
        for (BrokerMetadata broker : res.brokers()) {
            buf.writeInt(broker.brokerId());
            ByteBufSerde.writeString(buf, broker.host());
            buf.writeInt(broker.port());
        }
        writeItemCount(buf, res.topics().size(), "topics");
        for (TopicMetadata topic : res.topics()) {
            ByteBufSerde.writeString(buf, topic.topicName());
            writeItemCount(buf, topic.partitions().size(), "partitions");
            for (PartitionMetadata partition : topic.partitions()) {
                buf.writeInt(partition.partitionId());
                buf.writeInt(partition.leaderBrokerId());
                writeBrokerIdList(buf, partition.replicaBrokerIds(), "replica brokers");
                writeBrokerIdList(buf, partition.isrBrokerIds(), "isr brokers");
                writeReplicaLogEndOffsets(buf, partition.replicaLogEndOffsets());
                buf.writeBoolean(partition.frozen());
                buf.writeLong(partition.leaderEpoch());
                buf.writeLong(partition.leaderEpochStartOffset());
                buf.writeBoolean(partition.leaderReady());
            }
        }
        buf.writeBoolean(res.controller() != null);
        if (res.controller() != null) {
            writeControllerMetadata(buf, res.controller());
        }
        buf.writeLong(res.metadataVersion());
    }

    private static ControllerMetadata readControllerMetadata(ByteBuf buf) {
        requireReadable(buf, Integer.BYTES, "controller broker id");
        int brokerId = buf.readInt();
        String host = ByteBufSerde.readString(buf);
        requireReadable(buf, Integer.BYTES + Long.BYTES, "controller metadata");
        return new ControllerMetadata(brokerId, host, buf.readInt(), buf.readLong());
    }

    private static ControllerMetadata readV4ControllerMetadata(ByteBuf buf) {
        requireReadable(buf, Integer.BYTES, "controller broker id");
        int brokerId = buf.readInt();
        String host = readV4String(buf, "controller host");
        requireReadable(buf, Integer.BYTES + Long.BYTES, "controller metadata");
        return new ControllerMetadata(brokerId, host, buf.readInt(), buf.readLong());
    }

    private static void writeControllerMetadata(ByteBuf buf, ControllerMetadata controller) {
        buf.writeInt(controller.brokerId());
        ByteBufSerde.writeString(buf, controller.host());
        buf.writeInt(controller.port());
        buf.writeLong(controller.controllerEpoch());
    }

    private static MaintenanceReportResponse decodeMaintenanceReportResponse(ByteBuf buf) {
        requireReadable(buf, 1, "maintenance report dry-run flag");
        boolean dryRun = buf.readBoolean();
        String status = ByteBufSerde.readString(buf);
        int count = readItemCount(buf, "maintenance segment reports");
        List<MaintenanceSegmentInfo> segments = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            String partition = ByteBufSerde.readString(buf);
            String action = ByteBufSerde.readString(buf);
            requireReadable(buf, Long.BYTES * 4, "maintenance segment report");
            segments.add(new MaintenanceSegmentInfo(
                    partition,
                    action,
                    buf.readLong(),
                    buf.readLong(),
                    buf.readLong(),
                    buf.readLong()));
        }
        requireReadable(buf, Long.BYTES * 6, "maintenance report counters");
        return new MaintenanceReportResponse(
                dryRun,
                status,
                segments,
                buf.readLong(),
                buf.readLong(),
                buf.readLong(),
                buf.readLong(),
                buf.readLong(),
                buf.readLong());
    }

    private static void writeMaintenanceReportResponse(ByteBuf buf, MaintenanceReportResponse res) {
        buf.writeBoolean(res.dryRun());
        ByteBufSerde.writeString(buf, res.status());
        writeItemCount(buf, res.segmentReports().size(), "maintenance segment reports");
        for (MaintenanceSegmentInfo segment : res.segmentReports()) {
            ByteBufSerde.writeString(buf, segment.partition());
            ByteBufSerde.writeString(buf, segment.action());
            buf.writeLong(segment.baseOffset());
            buf.writeLong(segment.firstOffset());
            buf.writeLong(segment.lastOffset());
            buf.writeLong(segment.sizeBytes());
        }
        buf.writeLong(res.skippedDueToReaders());
        buf.writeLong(res.cleanupFailures());
        buf.writeLong(res.deletedBytes());
        buf.writeLong(res.deletedSegments());
        buf.writeLong(res.compactedBytes());
        buf.writeLong(res.compactedSegments());
    }

    private static MaintenanceMetricsResponse decodeMaintenanceMetricsResponse(ByteBuf buf) {
        requireReadable(buf, Long.BYTES * 8, "maintenance metrics counters");
        long runs = buf.readLong();
        long dryRuns = buf.readLong();
        long deletedBytes = buf.readLong();
        long deletedSegments = buf.readLong();
        long compactedBytes = buf.readLong();
        long compactedSegments = buf.readLong();
        long skippedDueToReaders = buf.readLong();
        long cleanupFailures = buf.readLong();
        String lastRunStatus = ByteBufSerde.readString(buf);
        return new MaintenanceMetricsResponse(
                runs,
                dryRuns,
                deletedBytes,
                deletedSegments,
                compactedBytes,
                compactedSegments,
                skippedDueToReaders,
                cleanupFailures,
                lastRunStatus);
    }

    private static void writeMaintenanceMetricsResponse(ByteBuf buf, MaintenanceMetricsResponse res) {
        buf.writeLong(res.runs());
        buf.writeLong(res.dryRuns());
        buf.writeLong(res.deletedBytes());
        buf.writeLong(res.deletedSegments());
        buf.writeLong(res.compactedBytes());
        buf.writeLong(res.compactedSegments());
        buf.writeLong(res.skippedDueToReaders());
        buf.writeLong(res.cleanupFailures());
        ByteBufSerde.writeString(buf, res.lastRunStatus());
    }

    private static RuntimeMetricsResponse decodeRuntimeMetricsResponse(ByteBuf buf) {
        int requestCount = readItemCount(buf, "runtime request metric counts");
        List<RuntimeMetricCount> requests = new ArrayList<>(requestCount);
        for (int i = 0; i < requestCount; i++) {
            requireReadable(buf, Short.BYTES + Long.BYTES, "runtime request metric count");
            requests.add(new RuntimeMetricCount(buf.readShort(), buf.readLong()));
        }

        int responseCount = readItemCount(buf, "runtime response error counts");
        List<RuntimeErrorCount> responses = new ArrayList<>(responseCount);
        for (int i = 0; i < responseCount; i++) {
            requireReadable(buf, Short.BYTES + Long.BYTES, "runtime response error count");
            responses.add(new RuntimeErrorCount(buf.readShort(), buf.readLong()));
        }

        int latencyCount = readItemCount(buf, "runtime latency summaries");
        List<RuntimeLatencySummary> latencies = new ArrayList<>(latencyCount);
        for (int i = 0; i < latencyCount; i++) {
            requireReadable(buf, Short.BYTES + Long.BYTES * 2 + Double.BYTES, "runtime latency summary");
            latencies.add(new RuntimeLatencySummary(
                    buf.readShort(), buf.readLong(), buf.readLong(), buf.readDouble()));
        }

        int throttledCount = readItemCount(buf, "runtime throttled metric counts");
        List<RuntimeMetricCount> throttled = new ArrayList<>(throttledCount);
        for (int i = 0; i < throttledCount; i++) {
            requireReadable(buf, Short.BYTES + Long.BYTES, "runtime throttled metric count");
            throttled.add(new RuntimeMetricCount(buf.readShort(), buf.readLong()));
        }
        return new RuntimeMetricsResponse(requests, responses, latencies, throttled);
    }

    private static void writeRuntimeMetricsResponse(ByteBuf buf, RuntimeMetricsResponse res) {
        writeItemCount(buf, res.requestsByApiKey().size(), "runtime request metric counts");
        for (RuntimeMetricCount count : res.requestsByApiKey()) {
            buf.writeShort(count.apiKey());
            buf.writeLong(count.count());
        }

        writeItemCount(buf, res.responsesByErrorCode().size(), "runtime response error counts");
        for (RuntimeErrorCount count : res.responsesByErrorCode()) {
            buf.writeShort(count.errorCode());
            buf.writeLong(count.count());
        }

        writeItemCount(buf, res.latencyByApiKey().size(), "runtime latency summaries");
        for (RuntimeLatencySummary summary : res.latencyByApiKey()) {
            buf.writeShort(summary.apiKey());
            buf.writeLong(summary.count());
            buf.writeLong(summary.totalLatencyMs());
            buf.writeDouble(summary.averageLatencyMs());
        }

        writeItemCount(buf, res.throttledByApiKey().size(), "runtime throttled metric counts");
        for (RuntimeMetricCount count : res.throttledByApiKey()) {
            buf.writeShort(count.apiKey());
            buf.writeLong(count.count());
        }
    }

    private static ReplicaFetchResponse decodeReplicaFetchResponse(ByteBuf buf) {
        String topic = ByteBufSerde.readString(buf);
        requireReadable(buf, Integer.BYTES + Long.BYTES * 3, "replica fetch response");
        int partitionId = buf.readInt();
        long highWatermark = buf.readLong();
        long leaderEpoch = buf.readLong();
        long leaderLogEndOffset = buf.readLong();
        int count = readItemCount(buf, "replica fetched records");
        List<FetchedRecord> records = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            requireReadable(buf, Long.BYTES, "replica fetched record offset");
            records.add(new FetchedRecord(buf.readLong(), ByteBufSerde.readBytes(buf), ByteBufSerde.readBytes(buf)));
        }
        return new ReplicaFetchResponse(
                topic, partitionId, highWatermark, leaderEpoch, leaderLogEndOffset, records);
    }

    private static void writeReplicaFetchRequest(ByteBuf buf, ReplicaFetchRequest req) {
        ByteBufSerde.writeString(buf, req.topicName());
        buf.writeInt(req.partitionId());
        buf.writeInt(req.replicaBrokerId());
        buf.writeLong(req.leaderEpoch());
        buf.writeLong(req.fetchOffset());
        buf.writeInt(req.maxBytes());
    }

    private static void writeReplicaFetchResponse(ByteBuf buf, ReplicaFetchResponse res) {
        ByteBufSerde.writeString(buf, res.topicName());
        buf.writeInt(res.partitionId());
        buf.writeLong(res.highWatermark());
        buf.writeLong(res.leaderEpoch());
        buf.writeLong(res.leaderLogEndOffset());
        writeItemCount(buf, res.records().size(), "replica fetched records");
        for (FetchedRecord record : res.records()) {
            buf.writeLong(record.offset());
            ByteBufSerde.writeBytes(buf, record.key());
            ByteBufSerde.writeBytes(buf, record.value());
        }
    }

    private static List<Integer> readBrokerIdList(ByteBuf buf, String fieldName) {
        int count = readItemCount(buf, fieldName);
        List<Integer> brokerIds = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            requireReadable(buf, Integer.BYTES, fieldName);
            brokerIds.add(buf.readInt());
        }
        return brokerIds;
    }

    private static void writeBrokerIdList(ByteBuf buf, List<Integer> brokerIds, String fieldName) {
        writeItemCount(buf, brokerIds.size(), fieldName);
        for (int brokerId : brokerIds) {
            buf.writeInt(brokerId);
        }
    }

    private static Map<Integer, Long> readReplicaLogEndOffsets(ByteBuf buf) {
        int count = readItemCount(buf, "replica LEOs");
        Map<Integer, Long> replicaLogEndOffsets = new HashMap<>(count);
        for (int i = 0; i < count; i++) {
            requireReadable(buf, Integer.BYTES + Long.BYTES, "replica LEO");
            int brokerId = buf.readInt();
            long logEndOffset = buf.readLong();
            if (logEndOffset < 0) {
                throw new IllegalArgumentException("Negative replica LEO");
            }
            replicaLogEndOffsets.put(brokerId, logEndOffset);
        }
        return replicaLogEndOffsets;
    }

    private static void writeReplicaLogEndOffsets(ByteBuf buf, Map<Integer, Long> replicaLogEndOffsets) {
        writeItemCount(buf, replicaLogEndOffsets.size(), "replica LEOs");
        replicaLogEndOffsets.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .forEach(entry -> {
                    if (entry.getValue() < 0) {
                        throw new IllegalArgumentException("Negative replica LEO");
                    }
                    buf.writeInt(entry.getKey());
                    buf.writeLong(entry.getValue());
                });
    }

    private static FetchResponse decodeFetchResponse(ByteBuf buf) {
        String topic = ByteBufSerde.readString(buf);
        requireReadable(buf, Integer.BYTES + Long.BYTES, "fetch response");
        int partitionId = buf.readInt();
        long highWatermark = buf.readLong();
        int count = readItemCount(buf, "fetched records");
        List<FetchedRecord> records = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            requireReadable(buf, Long.BYTES, "fetched record offset");
            records.add(new FetchedRecord(buf.readLong(), ByteBufSerde.readBytes(buf), ByteBufSerde.readBytes(buf)));
        }
        return new FetchResponse(topic, partitionId, highWatermark, records);
    }

    private static void writeFetchResponse(ByteBuf buf, FetchResponse res) {
        ByteBufSerde.writeString(buf, res.topicName());
        buf.writeInt(res.partitionId());
        buf.writeLong(res.highWatermark());
        writeItemCount(buf, res.records().size(), "fetched records");
        for (FetchedRecord record : res.records()) {
            buf.writeLong(record.offset());
            ByteBufSerde.writeBytes(buf, record.key());
            ByteBufSerde.writeBytes(buf, record.value());
        }
    }

    /**
     * Current HEARTBEAT body is {@code groupId, memberId, generationId}.
     * Legacy bodies omit {@code generationId}; decode defaults it to 0 so old frames
     * are not rejected as malformed. Coordinator still requires a matching generation
     * after a real join.
     */
    private static HeartbeatRequest decodeHeartbeatRequest(ByteBuf buf) {
        String groupId = ByteBufSerde.readString(buf);
        String memberId = ByteBufSerde.readString(buf);
        int generationId = buf.isReadable() ? buf.readInt() : 0;
        return new HeartbeatRequest(groupId, memberId, generationId);
    }

    /**
     * Current OFFSET_COMMIT body is {@code groupId, topic, partition, offset, memberId, generationId}.
     * Legacy bodies stop after {@code offset}; decode defaults {@code memberId=""} and
     * {@code generationId=0}. A body that includes {@code memberId} but omits generation
     * also defaults generation to 0.
     */
    private static OffsetCommitRequest decodeOffsetCommitRequest(ByteBuf buf) {
        String groupId = ByteBufSerde.readString(buf);
        String topicName = ByteBufSerde.readString(buf);
        int partitionId = buf.readInt();
        long offset = buf.readLong();
        String memberId = "";
        int generationId = 0;
        if (buf.isReadable()) {
            memberId = ByteBufSerde.readString(buf);
            if (buf.isReadable()) {
                generationId = buf.readInt();
            }
        }
        return new OffsetCommitRequest(groupId, topicName, partitionId, offset, memberId, generationId);
    }

    private static JoinGroupResponse decodeJoinGroupResponse(ByteBuf buf) {
        requireReadable(buf, Integer.BYTES, "join group generation");
        int generationId = buf.readInt();
        String memberId = ByteBufSerde.readString(buf);
        String leaderMemberId = ByteBufSerde.readString(buf);
        int memberCount = readItemCount(buf, "group members");
        List<String> members = new ArrayList<>();
        for (int i = 0; i < memberCount; i++) {
            members.add(ByteBufSerde.readString(buf));
        }
        return new JoinGroupResponse(generationId, memberId, leaderMemberId, members);
    }

    private static void writeJoinGroupResponse(ByteBuf buf, JoinGroupResponse res) {
        buf.writeInt(res.generationId());
        ByteBufSerde.writeString(buf, res.memberId());
        ByteBufSerde.writeString(buf, res.leaderMemberId());
        writeItemCount(buf, res.members().size(), "group members");
        for (String member : res.members()) {
            ByteBufSerde.writeString(buf, member);
        }
    }

    private static OpsStatusResponse decodeOpsStatusResponse(ByteBuf buf) {
        int targetBrokerId = buf.readInt();
        long startedAtEpochMs = buf.readLong();
        long snapshotAtEpochMs = buf.readLong();
        long metadataVersion = buf.readLong();
        int metadataFormatVersion = buf.readInt();
        String role = readV4String(buf, "broker role");
        long brokerEpoch = buf.readLong();
        long controllerEpoch = buf.readLong();
        String transportMode = readV4String(buf, "transport mode");
        String maintenanceState = readV4String(buf, "maintenance state");
        V4BoundedList<OpsBrokerInfo> brokers =
                readBoundedList(buf, "ops brokers", ProtocolSerdes::readOpsBrokerInfo);
        V4BoundedList<OpsPartitionInfo> partitions =
                readBoundedList(buf, "ops partitions", ProtocolSerdes::readOpsPartitionInfo);
        return new OpsStatusResponse(
                targetBrokerId,
                startedAtEpochMs,
                snapshotAtEpochMs,
                metadataVersion,
                metadataFormatVersion,
                role,
                brokerEpoch,
                controllerEpoch,
                transportMode,
                maintenanceState,
                brokers,
                partitions);
    }

    private static void writeOpsStatusResponse(ByteBuf buf, OpsStatusResponse res) {
        buf.writeInt(res.targetBrokerId());
        buf.writeLong(res.startedAtEpochMs());
        buf.writeLong(res.snapshotAtEpochMs());
        buf.writeLong(res.metadataVersion());
        buf.writeInt(res.metadataFormatVersion());
        ByteBufSerde.writeString(buf, res.role());
        buf.writeLong(res.brokerEpoch());
        buf.writeLong(res.controllerEpoch());
        ByteBufSerde.writeString(buf, res.transportMode());
        ByteBufSerde.writeString(buf, res.maintenanceState());
        writeBoundedList(buf, res.brokers(), "ops brokers", ProtocolSerdes::writeOpsBrokerInfo);
        writeBoundedList(
                buf,
                res.partitions(),
                "ops partitions",
                ProtocolSerdes::writeOpsPartitionInfo);
    }

    private static OpsBrokerInfo readOpsBrokerInfo(ByteBuf buf) {
        return new OpsBrokerInfo(
                buf.readInt(),
                readV4String(buf, "broker host"),
                buf.readInt(),
                readV4String(buf, "broker state"),
                buf.readLong(),
                buf.readLong());
    }

    private static void writeOpsBrokerInfo(ByteBuf buf, OpsBrokerInfo broker) {
        buf.writeInt(broker.brokerId());
        ByteBufSerde.writeString(buf, broker.host());
        buf.writeInt(broker.port());
        ByteBufSerde.writeString(buf, broker.state());
        buf.writeLong(broker.brokerEpoch());
        buf.writeLong(broker.leaseDeadlineEpochMs());
    }

    private static OpsPartitionInfo readOpsPartitionInfo(ByteBuf buf) {
        return new OpsPartitionInfo(
                readV4String(buf, "partition topic name"),
                buf.readInt(),
                buf.readInt(),
                buf.readLong(),
                buf.readLong(),
                buf.readLong(),
                buf.readLong(),
                readV4String(buf, "partition readiness"),
                readBoolean(buf, "partition frozen"),
                readBoundedIntList(buf, "replica ids"),
                readBoundedIntList(buf, "isr ids"));
    }

    private static void writeOpsPartitionInfo(ByteBuf buf, OpsPartitionInfo partition) {
        ByteBufSerde.writeString(buf, partition.topicName());
        buf.writeInt(partition.partitionId());
        buf.writeInt(partition.leaderId());
        buf.writeLong(partition.leaderEpoch());
        buf.writeLong(partition.physicalHighWatermark());
        buf.writeLong(partition.visibleHighWatermark());
        buf.writeLong(partition.nextVisibleAtEpochMs());
        ByteBufSerde.writeString(buf, partition.readiness());
        writeBoolean(buf, partition.frozen());
        writeBoundedIntList(buf, partition.replicaIds(), "replica ids");
        writeBoundedIntList(buf, partition.isrIds(), "isr ids");
    }

    private static GroupStatusResponse decodeGroupStatusResponse(ByteBuf buf) {
        return new GroupStatusResponse(
                readV4String(buf, "group id"),
                readOptionalString(buf, "group topic"),
                buf.readInt(),
                readBoundedList(buf, "group members", ProtocolSerdes::readGroupMemberInfo),
                readBoundedList(buf, "partition lag rows", ProtocolSerdes::readPartitionLagInfo));
    }

    private static void writeGroupStatusResponse(ByteBuf buf, GroupStatusResponse res) {
        ByteBufSerde.writeString(buf, res.groupId());
        writeOptionalString(buf, res.topicName());
        buf.writeInt(res.generationId());
        writeBoundedList(
                buf,
                res.members(),
                "group members",
                ProtocolSerdes::writeGroupMemberInfo);
        writeBoundedList(
                buf,
                res.partitionLags(),
                "partition lag rows",
                ProtocolSerdes::writePartitionLagInfo);
    }

    private static GroupMemberInfo readGroupMemberInfo(ByteBuf buf) {
        return new GroupMemberInfo(
                readV4String(buf, "member id"),
                readBoolean(buf, "member active"),
                buf.readLong(),
                readBoundedList(
                        buf,
                        "member assignments",
                        ProtocolSerdes::readGroupAssignmentInfo));
    }

    private static void writeGroupMemberInfo(ByteBuf buf, GroupMemberInfo member) {
        ByteBufSerde.writeString(buf, member.memberId());
        writeBoolean(buf, member.active());
        buf.writeLong(member.lastHeartbeatEpochMs());
        writeBoundedList(
                buf,
                member.assignments(),
                "member assignments",
                ProtocolSerdes::writeGroupAssignmentInfo);
    }

    private static GroupAssignmentInfo readGroupAssignmentInfo(ByteBuf buf) {
        return new GroupAssignmentInfo(
                readV4String(buf, "assignment topic name"), buf.readInt());
    }

    private static void writeGroupAssignmentInfo(ByteBuf buf, GroupAssignmentInfo assignment) {
        ByteBufSerde.writeString(buf, assignment.topicName());
        buf.writeInt(assignment.partitionId());
    }

    private static PartitionLagInfo readPartitionLagInfo(ByteBuf buf) {
        return new PartitionLagInfo(
                readV4String(buf, "lag topic name"),
                buf.readInt(),
                buf.readLong(),
                readV4String(buf, "commit state"),
                buf.readLong(),
                buf.readLong(),
                buf.readLong(),
                buf.readLong(),
                buf.readLong(),
                readV4String(buf, "lag readiness"),
                readBoolean(buf, "partition lag frozen"));
    }

    private static void writePartitionLagInfo(ByteBuf buf, PartitionLagInfo lag) {
        ByteBufSerde.writeString(buf, lag.topicName());
        buf.writeInt(lag.partitionId());
        buf.writeLong(lag.committedOffset());
        ByteBufSerde.writeString(buf, lag.commitState());
        buf.writeLong(lag.physicalHighWatermark());
        buf.writeLong(lag.physicalLag());
        buf.writeLong(lag.visibleHighWatermark());
        buf.writeLong(lag.visibleLag());
        buf.writeLong(lag.nextVisibleAtEpochMs());
        ByteBufSerde.writeString(buf, lag.readiness());
        writeBoolean(buf, lag.frozen());
    }

    private static <T> V4BoundedList<T> readBoundedList(
            ByteBuf buf, String fieldName, Function<ByteBuf, T> itemReader) {
        requireReadable(buf, Integer.BYTES + Byte.BYTES + Integer.BYTES, fieldName);
        int total = buf.readInt();
        boolean truncated = readBoolean(buf, fieldName + " truncated");
        int count = readItemCount(buf, fieldName);
        validateBoundedListEnvelope(total, truncated, count);
        List<T> items = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            items.add(itemReader.apply(buf));
        }
        return new V4BoundedList<>(total, truncated, items);
    }

    private static void validateBoundedListEnvelope(
            int total, boolean truncated, int count) {
        if (total < 0) {
            throw new IllegalArgumentException("total must be non-negative");
        }
        if (total < count) {
            throw new IllegalArgumentException(
                    "total cannot be smaller than encoded item count");
        }
        if (truncated && total <= count) {
            throw new IllegalArgumentException(
                    "truncated list total must exceed encoded item count");
        }
        if (!truncated && total != count) {
            throw new IllegalArgumentException(
                    "non-truncated list total must equal encoded item count");
        }
    }

    private static <T> void writeBoundedList(
            ByteBuf buf,
            V4BoundedList<T> list,
            String fieldName,
            BiConsumer<ByteBuf, T> itemWriter) {
        buf.writeInt(list.total());
        writeBoolean(buf, list.truncated());
        writeItemCount(buf, list.items().size(), fieldName);
        for (T item : list.items()) {
            itemWriter.accept(buf, item);
        }
    }

    private static V4BoundedList<Integer> readBoundedIntList(
            ByteBuf buf, String fieldName) {
        return readBoundedList(buf, fieldName, target -> {
            requireReadable(target, Integer.BYTES, fieldName + " item");
            return target.readInt();
        });
    }

    private static void writeBoundedIntList(
            ByteBuf buf, V4BoundedList<Integer> list, String fieldName) {
        writeBoundedList(buf, list, fieldName, (target, value) -> target.writeInt(value));
    }

    private static DescribeTopicResponse decodeDescribeTopicResponse(ByteBuf buf) {
        String topicName = readV4String(buf, "topic name");
        long metadataVersion = buf.readLong();
        boolean active = readBoolean(buf, "topic active");
        String retryTopic = readOptionalString(buf, "retry topic");
        String deadLetterTopic = readOptionalString(buf, "dead-letter topic");
        V4BoundedList<TopicPartitionInfo> partitions = readBoundedList(
                buf,
                "topic partitions",
                ProtocolSerdes::readTopicPartitionInfo);
        TopicTombstoneInfo tombstone = readBoolean(buf, "tombstone present")
                ? readTopicTombstoneInfo(buf)
                : null;
        return new DescribeTopicResponse(
                topicName,
                metadataVersion,
                active,
                retryTopic,
                deadLetterTopic,
                partitions,
                tombstone);
    }

    private static void writeDescribeTopicResponse(ByteBuf buf, DescribeTopicResponse res) {
        ByteBufSerde.writeString(buf, res.topicName());
        buf.writeLong(res.metadataVersion());
        writeBoolean(buf, res.active());
        writeOptionalString(buf, res.retryTopic());
        writeOptionalString(buf, res.deadLetterTopic());
        writeBoundedList(
                buf,
                res.partitions(),
                "topic partitions",
                ProtocolSerdes::writeTopicPartitionInfo);
        writeBoolean(buf, res.tombstone() != null);
        if (res.tombstone() != null) {
            writeTopicTombstoneInfo(buf, res.tombstone());
        }
    }

    private static DeleteTopicResponse decodeDeleteTopicResponse(ByteBuf buf) {
        return new DeleteTopicResponse(
                readV4String(buf, "topic name"),
                readV4String(buf, "tombstone state"),
                buf.readLong(),
                buf.readLong(),
                readBoundedIntList(buf, "required broker ids"),
                readBoundedIntList(buf, "acknowledged broker ids"),
                readBoundedIntList(buf, "pending broker ids"));
    }

    private static void writeDeleteTopicResponse(ByteBuf buf, DeleteTopicResponse res) {
        ByteBufSerde.writeString(buf, res.topicName());
        ByteBufSerde.writeString(buf, res.tombstoneState());
        buf.writeLong(res.tombstoneVersion());
        buf.writeLong(res.deletedAtEpochMs());
        writeBoundedIntList(buf, res.requiredBrokerIds(), "required broker ids");
        writeBoundedIntList(
                buf,
                res.acknowledgedBrokerIds(),
                "acknowledged broker ids");
        writeBoundedIntList(buf, res.pendingBrokerIds(), "pending broker ids");
    }

    private static TopicPartitionInfo readTopicPartitionInfo(ByteBuf buf) {
        return new TopicPartitionInfo(
                buf.readInt(),
                buf.readInt(),
                buf.readLong(),
                readBoundedIntList(buf, "topic partition replica ids"),
                readBoundedIntList(buf, "topic partition isr ids"),
                readV4String(buf, "topic partition readiness"),
                readBoolean(buf, "topic partition frozen"));
    }

    private static void writeTopicPartitionInfo(ByteBuf buf, TopicPartitionInfo partition) {
        buf.writeInt(partition.partitionId());
        buf.writeInt(partition.leaderId());
        buf.writeLong(partition.leaderEpoch());
        writeBoundedIntList(
                buf,
                partition.replicaIds(),
                "topic partition replica ids");
        writeBoundedIntList(buf, partition.isrIds(), "topic partition isr ids");
        ByteBufSerde.writeString(buf, partition.readiness());
        writeBoolean(buf, partition.frozen());
    }

    private static TopicTombstoneInfo readTopicTombstoneInfo(ByteBuf buf) {
        return new TopicTombstoneInfo(
                readV4String(buf, "tombstone state"),
                buf.readLong(),
                buf.readLong(),
                readBoundedIntList(buf, "tombstone required broker ids"),
                readBoundedIntList(buf, "tombstone acknowledged broker ids"),
                readBoundedIntList(buf, "tombstone pending broker ids"));
    }

    private static void writeTopicTombstoneInfo(ByteBuf buf, TopicTombstoneInfo tombstone) {
        ByteBufSerde.writeString(buf, tombstone.state());
        buf.writeLong(tombstone.tombstoneVersion());
        buf.writeLong(tombstone.deletedAtEpochMs());
        writeBoundedIntList(
                buf,
                tombstone.requiredBrokerIds(),
                "tombstone required broker ids");
        writeBoundedIntList(
                buf,
                tombstone.acknowledgedBrokerIds(),
                "tombstone acknowledged broker ids");
        writeBoundedIntList(
                buf,
                tombstone.pendingBrokerIds(),
                "tombstone pending broker ids");
    }

    private static TopicTombstoneSummary readTopicTombstoneSummary(ByteBuf buf) {
        return new TopicTombstoneSummary(
                readV4String(buf, "tombstone topic name"),
                readTopicTombstoneInfo(buf));
    }

    private static void writeTopicTombstoneSummary(ByteBuf buf, TopicTombstoneSummary summary) {
        ByteBufSerde.writeString(buf, summary.topicName());
        writeTopicTombstoneInfo(buf, summary.tombstone());
    }

    private static FailoverAuditResponse decodeFailoverAuditResponse(ByteBuf buf) {
        return new FailoverAuditResponse(
                buf.readInt(),
                buf.readLong(),
                buf.readLong(),
                readBoundedList(buf, "audit events", ProtocolSerdes::readAuditEventInfo));
    }

    private static void writeFailoverAuditResponse(ByteBuf buf, FailoverAuditResponse res) {
        buf.writeInt(res.brokerId());
        buf.writeLong(res.brokerStartedAtEpochMs());
        buf.writeLong(res.overwrittenCount());
        writeBoundedList(
                buf,
                res.events(),
                "audit events",
                ProtocolSerdes::writeAuditEventInfo);
    }

    private static AuditEventInfo readAuditEventInfo(ByteBuf buf) {
        return new AuditEventInfo(
                buf.readLong(),
                buf.readLong(),
                readV4String(buf, "audit event type"),
                readOptionalInt(buf, "audit broker id"),
                readOptionalString(buf, "audit topic name"),
                readOptionalInt(buf, "audit partition id"),
                readV4String(buf, "audit old value"),
                readV4String(buf, "audit new value"),
                readV4String(buf, "audit detail"));
    }

    private static void writeAuditEventInfo(ByteBuf buf, AuditEventInfo event) {
        buf.writeLong(event.sequence());
        buf.writeLong(event.timestampEpochMs());
        ByteBufSerde.writeString(buf, event.eventType());
        writeOptionalInt(buf, event.brokerId());
        writeOptionalString(buf, event.topicName());
        writeOptionalInt(buf, event.partitionId());
        ByteBufSerde.writeString(buf, event.oldValue());
        ByteBufSerde.writeString(buf, event.newValue());
        ByteBufSerde.writeString(buf, event.detail());
    }

    private static ReplicaFetchV2Response decodeReplicaFetchV2Response(ByteBuf buf) {
        return new ReplicaFetchV2Response(
                readV4String(buf, "topic name"),
                buf.readInt(),
                buf.readLong(),
                buf.readLong(),
                buf.readLong(),
                buf.readLong(),
                buf.readLong(),
                readBoundedList(
                        buf,
                        "scheduled replica records",
                        ProtocolSerdes::readScheduledReplicaRecord));
    }

    private static void writeReplicaFetchV2Response(ByteBuf buf, ReplicaFetchV2Response res) {
        ByteBufSerde.writeString(buf, res.topicName());
        buf.writeInt(res.partitionId());
        buf.writeLong(res.physicalHighWatermark());
        buf.writeLong(res.visibleHighWatermark());
        buf.writeLong(res.nextVisibleAtEpochMs());
        buf.writeLong(res.leaderEpoch());
        buf.writeLong(res.leaderLogEndOffset());
        writeBoundedList(
                buf,
                res.records(),
                "scheduled replica records",
                ProtocolSerdes::writeScheduledReplicaRecord);
    }

    private static ScheduledReplicaRecord readScheduledReplicaRecord(ByteBuf buf) {
        requireReadable(buf, Long.BYTES, "scheduled replica record offset");
        long offset = buf.readLong();
        byte[] key = ByteBufSerde.readBytes(buf);
        byte[] value = ByteBufSerde.readBytes(buf);
        requireReadable(buf, Long.BYTES, "scheduled replica record timestamp");
        return new ScheduledReplicaRecord(offset, key, value, buf.readLong());
    }

    private static void writeScheduledReplicaRecord(
            ByteBuf buf, ScheduledReplicaRecord record) {
        buf.writeLong(record.offset());
        ByteBufSerde.writeBytes(buf, record.key());
        ByteBufSerde.writeBytes(buf, record.value());
        buf.writeLong(record.deliverAtEpochMs());
    }

    private static FetchV2Response decodeFetchV2Response(ByteBuf buf) {
        return new FetchV2Response(
                readV4String(buf, "topic name"),
                buf.readInt(),
                buf.readLong(),
                buf.readLong(),
                buf.readLong(),
                readBoundedList(buf, "fetch v2 records", ProtocolSerdes::readFetchedRecord));
    }

    private static void writeFetchV2Response(ByteBuf buf, FetchV2Response res) {
        ByteBufSerde.writeString(buf, res.topicName());
        buf.writeInt(res.partitionId());
        buf.writeLong(res.physicalHighWatermark());
        buf.writeLong(res.visibleHighWatermark());
        buf.writeLong(res.nextVisibleAtEpochMs());
        writeBoundedList(
                buf,
                res.records(),
                "fetch v2 records",
                ProtocolSerdes::writeFetchedRecord);
    }

    private static FetchedRecord readFetchedRecord(ByteBuf buf) {
        requireReadable(buf, Long.BYTES, "fetched record offset");
        return new FetchedRecord(
                buf.readLong(), ByteBufSerde.readBytes(buf), ByteBufSerde.readBytes(buf));
    }

    private static void writeFetchedRecord(ByteBuf buf, FetchedRecord record) {
        buf.writeLong(record.offset());
        ByteBufSerde.writeBytes(buf, record.key());
        ByteBufSerde.writeBytes(buf, record.value());
    }

    private static MetadataV2Response decodeMetadataV2Response(ByteBuf buf) {
        V4BoundedList<OpsBrokerInfo> brokers =
                readBoundedList(buf, "metadata v2 brokers", ProtocolSerdes::readOpsBrokerInfo);
        V4BoundedList<TopicSummary> topics =
                readBoundedList(buf, "metadata v2 topics", ProtocolSerdes::readTopicSummary);
        V4BoundedList<TopicTombstoneSummary> tombstones =
                readBoundedList(
                        buf,
                        "metadata v2 tombstones",
                        ProtocolSerdes::readTopicTombstoneSummary);
        ControllerMetadata controller = readBoolean(buf, "controller present")
                ? readV4ControllerMetadata(buf)
                : null;
        return new MetadataV2Response(
                brokers, topics, tombstones, controller, buf.readLong(), buf.readInt());
    }

    private static void writeMetadataV2Response(ByteBuf buf, MetadataV2Response res) {
        writeBoundedList(
                buf,
                res.brokers(),
                "metadata v2 brokers",
                ProtocolSerdes::writeOpsBrokerInfo);
        writeBoundedList(
                buf,
                res.topics(),
                "metadata v2 topics",
                ProtocolSerdes::writeTopicSummary);
        writeBoundedList(
                buf,
                res.tombstones(),
                "metadata v2 tombstones",
                ProtocolSerdes::writeTopicTombstoneSummary);
        writeBoolean(buf, res.controller() != null);
        if (res.controller() != null) {
            writeControllerMetadata(buf, res.controller());
        }
        buf.writeLong(res.metadataVersion());
        buf.writeInt(res.metadataFormatVersion());
    }

    private static TopicSummary readTopicSummary(ByteBuf buf) {
        return new TopicSummary(
                readV4String(buf, "topic name"),
                buf.readInt(),
                buf.readInt(),
                readOptionalString(buf, "topic retry link"),
                readOptionalString(buf, "topic dead-letter link"));
    }

    private static void writeTopicSummary(ByteBuf buf, TopicSummary topic) {
        ByteBufSerde.writeString(buf, topic.topicName());
        buf.writeInt(topic.partitionCount());
        buf.writeInt(topic.replicationFactor());
        writeOptionalString(buf, topic.retryTopic());
        writeOptionalString(buf, topic.deadLetterTopic());
    }

    private static RuntimeMetricsV2Response decodeRuntimeMetricsV2Response(ByteBuf buf) {
        long startedAtEpochMs = buf.readLong();
        long snapshotAtEpochMs = buf.readLong();
        long uptimeMs = buf.readLong();
        V4BoundedList<RuntimeMetricCount> requests = readBoundedList(
                buf,
                "runtime v2 request metrics",
                ProtocolSerdes::readRuntimeMetricCount);
        V4BoundedList<RuntimeErrorCount> responses = readBoundedList(
                buf,
                "runtime v2 response metrics",
                ProtocolSerdes::readRuntimeErrorCount);
        V4BoundedList<RuntimeLatencyV2Summary> latencies = readBoundedList(
                buf,
                "runtime v2 latency metrics",
                ProtocolSerdes::readRuntimeLatencyV2Summary);
        V4BoundedList<RuntimeMetricCount> throttled = readBoundedList(
                buf,
                "runtime v2 throttled metrics",
                ProtocolSerdes::readRuntimeMetricCount);
        long produceBytes = buf.readLong();
        long fetchBytes = buf.readLong();
        long auditOverwrittenCount = buf.readLong();
        V4BoundedList<PartitionLagInfo> lagMetrics = readBoundedList(
                buf,
                "runtime v2 lag metrics",
                ProtocolSerdes::readPartitionLagInfo);
        return new RuntimeMetricsV2Response(
                startedAtEpochMs,
                snapshotAtEpochMs,
                uptimeMs,
                requests,
                responses,
                latencies,
                throttled,
                produceBytes,
                fetchBytes,
                auditOverwrittenCount,
                lagMetrics);
    }

    private static void writeRuntimeMetricsV2Response(
            ByteBuf buf, RuntimeMetricsV2Response res) {
        buf.writeLong(res.startedAtEpochMs());
        buf.writeLong(res.snapshotAtEpochMs());
        buf.writeLong(res.uptimeMs());
        writeBoundedList(
                buf,
                res.requestsByApiKey(),
                "runtime v2 request metrics",
                ProtocolSerdes::writeRuntimeMetricCount);
        writeBoundedList(
                buf,
                res.responsesByErrorCode(),
                "runtime v2 response metrics",
                ProtocolSerdes::writeRuntimeErrorCount);
        writeBoundedList(
                buf,
                res.latencyByApiKey(),
                "runtime v2 latency metrics",
                ProtocolSerdes::writeRuntimeLatencyV2Summary);
        writeBoundedList(
                buf,
                res.throttledByApiKey(),
                "runtime v2 throttled metrics",
                ProtocolSerdes::writeRuntimeMetricCount);
        buf.writeLong(res.produceBytes());
        buf.writeLong(res.fetchBytes());
        buf.writeLong(res.auditOverwrittenCount());
        writeBoundedList(
                buf,
                res.lagMetrics(),
                "runtime v2 lag metrics",
                ProtocolSerdes::writePartitionLagInfo);
    }

    private static RuntimeMetricCount readRuntimeMetricCount(ByteBuf buf) {
        requireReadable(buf, Short.BYTES + Long.BYTES, "runtime metric");
        return new RuntimeMetricCount(buf.readShort(), buf.readLong());
    }

    private static void writeRuntimeMetricCount(ByteBuf buf, RuntimeMetricCount metric) {
        buf.writeShort(metric.apiKey());
        buf.writeLong(metric.count());
    }

    private static RuntimeErrorCount readRuntimeErrorCount(ByteBuf buf) {
        requireReadable(buf, Short.BYTES + Long.BYTES, "runtime error metric");
        return new RuntimeErrorCount(buf.readShort(), buf.readLong());
    }

    private static void writeRuntimeErrorCount(ByteBuf buf, RuntimeErrorCount metric) {
        buf.writeShort(metric.errorCode());
        buf.writeLong(metric.count());
    }

    private static RuntimeLatencyV2Summary readRuntimeLatencyV2Summary(ByteBuf buf) {
        requireReadable(
                buf,
                Short.BYTES + Long.BYTES + Long.BYTES + Double.BYTES,
                "runtime latency v2 summary");
        return new RuntimeLatencyV2Summary(
                buf.readShort(), buf.readLong(), buf.readLong(), buf.readDouble());
    }

    private static void writeRuntimeLatencyV2Summary(
            ByteBuf buf, RuntimeLatencyV2Summary summary) {
        buf.writeShort(summary.apiKey());
        buf.writeLong(summary.count());
        buf.writeLong(summary.totalNanos());
        buf.writeDouble(summary.maxMillis());
    }

    private static void writeByteArrayWithShortPrefix(ByteBuf buf, byte[] data) {
        if (data == null || data.length == 0) {
            buf.writeShort(0);
            return;
        }
        if (data.length > MAX_SHORT_BYTES) {
            throw new IllegalArgumentException("Short-prefixed byte array is too large: " + data.length);
        }
        buf.writeShort(data.length);
        buf.writeBytes(data);
    }

    private static byte[] readByteArrayWithShortPrefix(ByteBuf buf) {
        requireReadable(buf, Short.BYTES, "short-prefixed byte array length");
        int len = buf.readUnsignedShort();
        if (len == 0) {
            return new byte[0];
        }
        requireReadable(buf, len, "short-prefixed byte array");
        byte[] bytes = new byte[len];
        buf.readBytes(bytes);
        return bytes;
    }

    // 集合长度先做上下界校验，避免畸形帧触发超大内存分配或越界读取。
    private static int readItemCount(ByteBuf buf, String fieldName) {
        requireReadable(buf, Integer.BYTES, fieldName + " count");
        int count = buf.readInt();
        if (count < 0 || count > MAX_COLLECTION_ITEMS) {
            throw new IllegalArgumentException("Invalid " + fieldName + " count: " + count);
        }
        return count;
    }

    private static void writeItemCount(ByteBuf buf, int count, String fieldName) {
        if (count < 0 || count > MAX_COLLECTION_ITEMS) {
            throw new IllegalArgumentException("Invalid " + fieldName + " count: " + count);
        }
        buf.writeInt(count);
    }

    private static boolean readBoolean(ByteBuf buf, String fieldName) {
        requireReadable(buf, Byte.BYTES, fieldName);
        byte value = buf.readByte();
        if (value == 0) {
            return false;
        }
        if (value == 1) {
            return true;
        }
        throw new IllegalArgumentException("Invalid " + fieldName + " boolean: " + value);
    }

    private static void writeBoolean(ByteBuf buf, boolean value) {
        buf.writeByte(value ? 1 : 0);
    }

    private static String readOptionalString(ByteBuf buf, String fieldName) {
        if (!readBoolean(buf, fieldName + " present")) {
            return null;
        }
        String value = readV4String(buf, fieldName);
        if (value.isEmpty()) {
            throw new IllegalArgumentException(fieldName + " must be non-empty when present");
        }
        return value;
    }

    private static void writeOptionalString(ByteBuf buf, String value) {
        writeBoolean(buf, value != null);
        if (value != null) {
            if (value.isEmpty()) {
                throw new IllegalArgumentException("optional string must be absent or non-empty");
            }
            ByteBufSerde.writeString(buf, value);
        }
    }

    private static String readCredentialPrefix(ByteBuf payload) {
        return readV4String(payload, "credential", 1, MAX_CREDENTIAL_BYTES);
    }

    private static String readV4Credential(ByteBuf payload) {
        return readV4String(payload, "credential", 1, MAX_CREDENTIAL_BYTES);
    }

    private static String readV4String(ByteBuf payload, String fieldName) {
        return readV4String(payload, fieldName, 0, MAX_SHORT_BYTES);
    }

    private static String readV4String(
            ByteBuf payload, String fieldName, int minLength, int maxLength) {
        requireReadable(payload, Short.BYTES, fieldName + " length");
        int length = payload.readUnsignedShort();
        if (length < minLength || length > maxLength) {
            throw new IllegalArgumentException(
                    fieldName + " must encode to " + minLength + ".." + maxLength + " bytes");
        }
        requireReadable(payload, length, fieldName);
        byte[] bytes = new byte[length];
        payload.readBytes(bytes);
        try {
            return StandardCharsets.UTF_8
                    .newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(bytes))
                    .toString();
        } catch (CharacterCodingException e) {
            throw new IllegalArgumentException(fieldName + " must be valid UTF-8", e);
        }
    }

    private static Integer readOptionalInt(ByteBuf buf, String fieldName) {
        if (!readBoolean(buf, fieldName + " present")) {
            return null;
        }
        requireReadable(buf, Integer.BYTES, fieldName);
        return buf.readInt();
    }

    private static void writeOptionalInt(ByteBuf buf, Integer value) {
        writeBoolean(buf, value != null);
        if (value != null) {
            buf.writeInt(value);
        }
    }

    private static boolean hasCredentialPrefix(short apiKey) {
        return switch (apiKey) {
            case ApiKeys.OPS_STATUS,
                    ApiKeys.GROUP_STATUS,
                    ApiKeys.LIST_TOPICS,
                    ApiKeys.DESCRIBE_TOPIC,
                    ApiKeys.DELETE_TOPIC,
                    ApiKeys.INCREASE_PARTITIONS,
                    ApiKeys.FAILOVER_AUDIT,
                    ApiKeys.AUTH_STATUS,
                    ApiKeys.CONFIGURE_TOPIC_RELIABILITY,
                    ApiKeys.MARK_BROKER_OFFLINE_ADMIN,
                    ApiKeys.CREATE_TOPIC_ADMIN,
                    ApiKeys.REPLICA_FETCH_V2,
                    ApiKeys.TOPIC_DELETE_ACK,
                    ApiKeys.RUNTIME_METRICS_V2,
                    ApiKeys.UPDATE_REPLICA_STATE_V2 -> true;
            default -> false;
        };
    }

    private static void requireFullyConsumed(ByteBuf buf, String fieldName) {
        if (buf.isReadable()) {
            throw new IllegalArgumentException(
                    "Trailing bytes after " + fieldName + ": " + buf.readableBytes());
        }
    }

    private static void requireReadable(ByteBuf buf, int length, String fieldName) {
        if (buf.readableBytes() < length) {
            throw new IllegalArgumentException("Malformed " + fieldName + ": need " + length
                    + " bytes, only " + buf.readableBytes() + " available");
        }
    }
}
