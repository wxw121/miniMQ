package com.minimq.protocol;

import com.minimq.common.error.ErrorCode;
import com.minimq.protocol.codec.ByteBufSerde;
import com.minimq.protocol.message.MqRequestHeader;
import com.minimq.protocol.message.MqResponseHeader;
import com.minimq.protocol.message.ProtocolMessages;
import com.minimq.protocol.message.ProtocolMessages.*;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.util.ArrayList;
import java.util.List;

/**
 * MiniMQ 协议序列化入口，负责请求/响应 body 与完整网络帧之间的双向转换。
 */
public final class ProtocolSerdes {
    private static final int MAX_COLLECTION_ITEMS = 10_000;
    private static final int MAX_SHORT_BYTES = 0xFFFF;

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
        return switch (apiKey) {
            case ApiKeys.CREATE_TOPIC -> new CreateTopicRequest(
                    ByteBufSerde.readString(payload), payload.readInt(), payload.readInt());
            case ApiKeys.METADATA -> new MetadataRequest(ByteBufSerde.readString(payload));
            case ApiKeys.PRODUCE -> decodeProduceRequest(payload);
            case ApiKeys.FETCH -> new FetchRequest(
                    ByteBufSerde.readString(payload),
                    payload.readInt(),
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
            case ApiKeys.HEARTBEAT -> new HeartbeatRequest(
                    ByteBufSerde.readString(payload), ByteBufSerde.readString(payload));
            case ApiKeys.OFFSET_COMMIT -> new OffsetCommitRequest(
                    ByteBufSerde.readString(payload),
                    ByteBufSerde.readString(payload),
                    payload.readInt(),
                    payload.readLong());
            case ApiKeys.OFFSET_FETCH -> new OffsetFetchRequest(
                    ByteBufSerde.readString(payload),
                    ByteBufSerde.readString(payload),
                    payload.readInt());
            case ApiKeys.REGISTER_BROKER -> new RegisterBrokerRequest(
                    payload.readInt(), ByteBufSerde.readString(payload), payload.readInt());
            case ApiKeys.REPLICA_FETCH -> new ReplicaFetchRequest(
                    ByteBufSerde.readString(payload),
                    payload.readInt(),
                    payload.readInt(),
                    payload.readLong(),
                    payload.readInt());
            case ApiKeys.MARK_BROKER_OFFLINE -> new MarkBrokerOfflineRequest(payload.readInt());
            default -> throw new IllegalArgumentException("Unknown apiKey: " + apiKey);
        };
    }

    public static MqResponseHeader decodeResponseHeader(ByteBuf payload) {
        requireReadable(payload, Integer.BYTES + Short.BYTES, "response header");
        int correlationId = payload.readInt();
        ErrorCode errorCode = ErrorCode.fromCode(payload.readShort());
        return new MqResponseHeader(correlationId, errorCode);
    }

    public static Object decodeResponseBody(short apiKey, ByteBuf payload) {
        return switch (apiKey) {
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
            case ApiKeys.REGISTER_BROKER -> new RegisterBrokerResponse();
            case ApiKeys.REPLICA_FETCH -> decodeReplicaFetchResponse(payload);
            case ApiKeys.MARK_BROKER_OFFLINE -> new MarkBrokerOfflineResponse();
            default -> throw new IllegalArgumentException("Unknown apiKey: " + apiKey);
        };
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
            }
            case ApiKeys.OFFSET_COMMIT -> {
                OffsetCommitRequest req = (OffsetCommitRequest) body;
                ByteBufSerde.writeString(buf, req.groupId());
                ByteBufSerde.writeString(buf, req.topicName());
                buf.writeInt(req.partitionId());
                buf.writeLong(req.offset());
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
            }
            case ApiKeys.REPLICA_FETCH -> writeReplicaFetchRequest(buf, (ReplicaFetchRequest) body);
            case ApiKeys.MARK_BROKER_OFFLINE -> buf.writeInt(((MarkBrokerOfflineRequest) body).brokerId());
            default -> throw new IllegalArgumentException("Unknown apiKey: " + apiKey);
        }
    }

    private static void writeResponseBody(ErrorCode errorCode, ByteBuf buf, Object body) {
        if (errorCode != ErrorCode.NONE || body == null) {
            return;
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
        } else if (body instanceof ReplicaFetchResponse res) {
            writeReplicaFetchResponse(buf, res);
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
        requireReadable(buf, Integer.BYTES * 2, "produce request");
        int partitionId = buf.readInt();
        int count = readItemCount(buf, "produce records");
        List<ProduceRecord> records = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            records.add(new ProduceRecord(ByteBufSerde.readBytes(buf), ByteBufSerde.readBytes(buf)));
        }
        return new ProduceRequest(topic, partitionId, records);
    }

    private static void writeProduceRequest(ByteBuf buf, ProduceRequest req) {
        ByteBufSerde.writeString(buf, req.topicName());
        buf.writeInt(req.partitionId());
        writeItemCount(buf, req.records().size(), "produce records");
        for (ProduceRecord record : req.records()) {
            ByteBufSerde.writeBytes(buf, record.key());
            ByteBufSerde.writeBytes(buf, record.value());
        }
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
                partitions.add(new PartitionMetadata(partitionId, leaderBrokerId, replicaBrokerIds, isrBrokerIds));
            }
            topics.add(new TopicMetadata(topicName, partitions));
        }
        return new MetadataResponse(brokers, topics);
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
            }
        }
    }

    private static ReplicaFetchResponse decodeReplicaFetchResponse(ByteBuf buf) {
        return decodeFetchLikeResponse(buf, ReplicaFetchResponse::new);
    }

    private static void writeReplicaFetchRequest(ByteBuf buf, ReplicaFetchRequest req) {
        ByteBufSerde.writeString(buf, req.topicName());
        buf.writeInt(req.partitionId());
        buf.writeInt(req.replicaBrokerId());
        buf.writeLong(req.fetchOffset());
        buf.writeInt(req.maxBytes());
    }

    private static void writeReplicaFetchResponse(ByteBuf buf, ReplicaFetchResponse res) {
        writeFetchLikeResponse(buf, res.topicName(), res.partitionId(), res.highWatermark(), res.records());
    }

    private static ReplicaFetchResponse decodeFetchLikeResponse(
            ByteBuf buf,
            FetchLikeResponseFactory<ReplicaFetchResponse> factory) {
        String topic = ByteBufSerde.readString(buf);
        requireReadable(buf, Integer.BYTES + Long.BYTES, "replica fetch response");
        int partitionId = buf.readInt();
        long highWatermark = buf.readLong();
        int count = readItemCount(buf, "replica fetched records");
        List<FetchedRecord> records = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            requireReadable(buf, Long.BYTES, "replica fetched record offset");
            records.add(new FetchedRecord(buf.readLong(), ByteBufSerde.readBytes(buf), ByteBufSerde.readBytes(buf)));
        }
        return factory.create(topic, partitionId, highWatermark, records);
    }

    private static void writeFetchLikeResponse(
            ByteBuf buf,
            String topicName,
            int partitionId,
            long highWatermark,
            List<FetchedRecord> records) {
        ByteBufSerde.writeString(buf, topicName);
        buf.writeInt(partitionId);
        buf.writeLong(highWatermark);
        writeItemCount(buf, records.size(), "replica fetched records");
        for (FetchedRecord record : records) {
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

    @FunctionalInterface
    private interface FetchLikeResponseFactory<T> {
        T create(String topicName, int partitionId, long highWatermark, List<FetchedRecord> records);
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

    private static void requireReadable(ByteBuf buf, int length, String fieldName) {
        if (buf.readableBytes() < length) {
            throw new IllegalArgumentException("Malformed " + fieldName + ": need " + length
                    + " bytes, only " + buf.readableBytes() + " available");
        }
    }
}
