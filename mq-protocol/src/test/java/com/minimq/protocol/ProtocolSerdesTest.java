package com.minimq.protocol;

import com.minimq.protocol.message.MqRequestHeader;
import com.minimq.protocol.message.MqResponseHeader;
import com.minimq.protocol.message.ProtocolMessages.*;
import com.minimq.protocol.message.ProtocolMessages.CreateTopicRequest;
import com.minimq.protocol.message.ProtocolMessages.ProduceRequest;
import com.minimq.protocol.message.ProtocolMessages.ProduceRecord;
import com.minimq.common.error.ErrorCode;
import com.minimq.protocol.codec.ByteBufSerde;
import com.minimq.protocol.codec.MqDecoder;
import com.minimq.protocol.codec.MqEncoder;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.DecoderException;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ProtocolSerdesTest {

    @Test
    void v3GoldenFramesRemainStable() {
        assertGoldenRequest(
                ApiKeys.CREATE_TOPIC,
                new CreateTopicRequest("orders", 3, 2),
                "4d51010000001e0000000000070006636c69656e7400066f72646572730000000300000002");
        assertGoldenRequest(
                ApiKeys.METADATA,
                new MetadataRequest("orders"),
                "4d5101000000160001000000070006636c69656e7400066f7264657273");
        assertGoldenRequest(
                ApiKeys.PRODUCE,
                new ProduceRequest(
                        "orders",
                        1,
                        4L,
                        List.of(new ProduceRecord(bytes("k"), bytes("v")))),
                "4d5101000000300002000000070006636c69656e7400066f726465727300000001000000000000000400000001000000016b0000000176");
        assertGoldenRequest(
                ApiKeys.FETCH,
                new FetchRequest("orders", 2, 4L, 10L, 4096),
                "4d51010000002e0003000000070006636c69656e7400066f7264657273000000020000000000000004000000000000000a00001000");
        assertGoldenRequest(
                ApiKeys.REPLICA_FETCH,
                new ReplicaFetchRequest("orders", 0, 1, 4L, 5L, 4096),
                "4d510100000032000a000000070006636c69656e7400066f726465727300000000000000010000000000000004000000000000000500001000");
        assertGoldenRequest(
                ApiKeys.MARK_BROKER_OFFLINE,
                new MarkBrokerOfflineRequest(2),
                "4d510100000012000b000000070006636c69656e7400000002");
        assertGoldenRequest(
                ApiKeys.UPDATE_REPLICA_LEO,
                new UpdateReplicaLeoRequest("orders", 0, 1, 7L, 3L, 9L),
                "4d510100000036000c000000070006636c69656e7400066f72646572730000000000000001000000000000000700000000000000030000000000000009");
        assertGoldenRequest(
                ApiKeys.RUNTIME_METRICS,
                new RuntimeMetricsRequest("runtime-token"),
                "4d51010000001d0013000000070006636c69656e74000d72756e74696d652d746f6b656e");

        assertGoldenResponse(
                ApiKeys.CREATE_TOPIC,
                new CreateTopicResponse("orders"),
                "4d51010000000e00000009000000066f7264657273");
        assertGoldenResponse(
                ApiKeys.METADATA,
                new MetadataResponse(List.of(), List.of(), null, 0L),
                "4d5101000000170000000900000000000000000000000000000000000000");
        assertGoldenResponse(
                ApiKeys.PRODUCE,
                new ProduceResponse("orders", 1, 5L),
                "4d51010000001a00000009000000066f7264657273000000010000000000000005");
        assertGoldenResponse(
                ApiKeys.FETCH,
                new FetchResponse(
                        "orders",
                        2,
                        10L,
                        List.of(new FetchedRecord(9L, bytes("k"), bytes("v")))),
                "4d51010000003000000009000000066f726465727300000002000000000000000a000000010000000000000009000000016b0000000176");
        assertGoldenResponse(
                ApiKeys.REPLICA_FETCH,
                new ReplicaFetchResponse(
                        "orders",
                        0,
                        10L,
                        4L,
                        11L,
                        List.of(new FetchedRecord(9L, bytes("k"), bytes("v")))),
                "4d51010000004000000009000000066f726465727300000000000000000000000a0000000000000004000000000000000b000000010000000000000009000000016b0000000176");
        assertGoldenResponse(
                ApiKeys.MARK_BROKER_OFFLINE,
                new MarkBrokerOfflineResponse(),
                "4d510100000006000000090000");
        assertGoldenResponse(
                ApiKeys.UPDATE_REPLICA_LEO,
                new UpdateReplicaLeoResponse(),
                "4d510100000006000000090000");
        assertGoldenResponse(
                ApiKeys.RUNTIME_METRICS,
                new RuntimeMetricsResponse(List.of(), List.of(), List.of(), List.of()),
                "4d51010000001600000009000000000000000000000000000000000000");
    }

    @Test
    void createTopicRoundTrip() {
        MqRequestHeader header = new MqRequestHeader(ApiKeys.CREATE_TOPIC, 42, "client-1");
        CreateTopicRequest body = new CreateTopicRequest("orders", 3, 2);
        var encoded = ProtocolSerdes.encodeRequest(header, body);
        encoded.readShort();
        encoded.readByte();
        int len = encoded.readInt();
        var payload = encoded.readRetainedSlice(len);
        var decodedHeader = ProtocolSerdes.decodeRequestHeader(payload);
        var decodedBody = (CreateTopicRequest) ProtocolSerdes.decodeRequestBody(decodedHeader.apiKey(), payload);
        assertEquals("orders", decodedBody.topicName());
        assertEquals(3, decodedBody.numPartitions());
        assertEquals(2, decodedBody.replicationFactor());
        encoded.release();
        payload.release();
    }

    @Test
    void produceRoundTrip() {
        MqRequestHeader header = new MqRequestHeader(ApiKeys.PRODUCE, 1, "p");
        ProduceRequest body = new ProduceRequest("orders", 0,
                List.of(new ProduceRecord("k".getBytes(), "v".getBytes())));
        var encoded = ProtocolSerdes.encodeRequest(header, body);
        encoded.readShort();
        encoded.readByte();
        int len = encoded.readInt();
        var payload = encoded.readRetainedSlice(len);
        var decodedHeader = ProtocolSerdes.decodeRequestHeader(payload);
        var decodedBody = (ProduceRequest) ProtocolSerdes.decodeRequestBody(decodedHeader.apiKey(), payload);
        assertEquals("orders", decodedBody.topicName());
        assertEquals(1, decodedBody.records().size());
        encoded.release();
        payload.release();
    }

    @Test
    void rejectsTruncatedStringPayload() {
        var payload = Unpooled.buffer();
        payload.writeShort(ApiKeys.CREATE_TOPIC);
        payload.writeInt(1);
        payload.writeShort(5);
        payload.writeByte('c');

        try {
            assertThrows(IllegalArgumentException.class, () -> ProtocolSerdes.decodeRequestHeader(payload));
        } finally {
            payload.release();
        }
    }

    @Test
    void decoderRejectsOversizedFrameLength() {
        var frame = Unpooled.buffer();
        frame.writeShort(MqFrame.MAGIC);
        frame.writeByte(MqFrame.VERSION);
        frame.writeInt(MqFrame.MAX_FRAME_BYTES + 1);

        EmbeddedChannel channel = new EmbeddedChannel(new MqDecoder());
        DecoderException ex = assertThrows(DecoderException.class, () -> channel.writeInbound(frame));
        assertInstanceOf(IllegalArgumentException.class, ex.getCause());
    }

    @Test
    void allRequestBodiesRoundTrip() {
        List<RequestCase> cases = List.of(
                new RequestCase(ApiKeys.CREATE_TOPIC, new CreateTopicRequest("orders", 3)),
                new RequestCase(ApiKeys.METADATA, new MetadataRequest("orders")),
                new RequestCase(ApiKeys.PRODUCE, new ProduceRequest("orders", 1, 4L,
                        List.of(new ProduceRecord(bytes("k"), bytes("v"))))),
                new RequestCase(ApiKeys.FETCH, new FetchRequest("orders", 2, 4L, 10L, 4096)),
                new RequestCase(ApiKeys.JOIN_GROUP, new JoinGroupRequest("g1", "m1", "orders")),
                new RequestCase(ApiKeys.SYNC_GROUP, new SyncGroupRequest("g1", 2, "m1", bytes("assignment"))),
                new RequestCase(ApiKeys.HEARTBEAT, new HeartbeatRequest("g1", "m1")),
                new RequestCase(ApiKeys.OFFSET_COMMIT, new OffsetCommitRequest("g1", "orders", 1, 42L)),
                new RequestCase(ApiKeys.OFFSET_FETCH, new OffsetFetchRequest("g1", "orders", 1)),
                new RequestCase(ApiKeys.REPLICA_FETCH, new ReplicaFetchRequest("orders", 0, 1, 4L, 5L, 4096)),
                new RequestCase(ApiKeys.MARK_BROKER_OFFLINE, new MarkBrokerOfflineRequest(2)),
                new RequestCase(ApiKeys.UPDATE_REPLICA_LEO,
                        new UpdateReplicaLeoRequest("orders", 0, 1, 7L, 3L, 9L)),
                new RequestCase(ApiKeys.MAINTENANCE_REPORT, new MaintenanceReportRequest("report-token")),
                new RequestCase(ApiKeys.MAINTENANCE_RUN, new MaintenanceRunRequest("run-token")),
                new RequestCase(ApiKeys.MAINTENANCE_METRICS, new MaintenanceMetricsRequest("metrics-token")),
                new RequestCase(ApiKeys.RUNTIME_METRICS, new RuntimeMetricsRequest("runtime-token")),
                new RequestCase(ApiKeys.BROKER_HEARTBEAT, new BrokerHeartbeatRequest(1, 7L, 3L)),
                new RequestCase(ApiKeys.REGISTER_BROKER, new RegisterBrokerRequest(1, "localhost", 9093, 3L)),
                new RequestCase(ApiKeys.DISCOVER_CONTROLLER, new DiscoverControllerRequest()),
                new RequestCase(ApiKeys.LEADER_READY,
                        new LeaderReadyRequest("orders", 0, 1, 7L, 3L, 4L, 12L)));

        for (RequestCase requestCase : cases) {
            var frame = ProtocolSerdes.encodeRequest(
                    new MqRequestHeader(requestCase.apiKey(), 7, "client"), requestCase.body());
            var payload = unwrapRetainedPayload(frame);
            try {
                MqDecoder.DecodedRequest decoded = MqDecoder.decodeRequest(payload);
                assertEquals(requestCase.apiKey(), decoded.header().apiKey());
                assertEquals(7, decoded.header().correlationId());
                assertBodyEquals(requestCase.body(), decoded.body());
            } finally {
                payload.release();
                frame.release();
            }
        }
    }

    @Test
    void allResponseBodiesRoundTrip() {
        List<ResponseCase> cases = List.of(
                new ResponseCase(ApiKeys.CREATE_TOPIC, new CreateTopicResponse("orders")),
                new ResponseCase(ApiKeys.METADATA, new MetadataResponse(
                        List.of(new BrokerMetadata(1, "localhost", 9093)),
                        List.of(new TopicMetadata("orders",
                                List.of(
                                        new PartitionMetadata(
                                                0, 1, List.of(1, 2), List.of(), Map.of(1, 12L, 2, 9L),
                                                true, 4L, 9L, false),
                                        new PartitionMetadata(1, 1)))),
                        new ControllerMetadata(1, "localhost", 9093, 3L),
                        8L)),
                new ResponseCase(ApiKeys.PRODUCE, new ProduceResponse("orders", 0, 11L)),
                new ResponseCase(ApiKeys.FETCH, new FetchResponse("orders", 0, 12L,
                        List.of(new FetchedRecord(11L, bytes("k"), bytes("v"))))),
                new ResponseCase(ApiKeys.JOIN_GROUP, new JoinGroupResponse(1, "m1", "m1", List.of("m1", "m2"))),
                new ResponseCase(ApiKeys.SYNC_GROUP, new SyncGroupResponse(bytes("assignment"))),
                new ResponseCase(ApiKeys.HEARTBEAT, new HeartbeatResponse()),
                new ResponseCase(ApiKeys.OFFSET_COMMIT, new OffsetCommitResponse()),
                new ResponseCase(ApiKeys.OFFSET_FETCH, new OffsetFetchResponse(42L)),
                new ResponseCase(ApiKeys.REGISTER_BROKER, new RegisterBrokerResponse(7L, 5_000L)),
                new ResponseCase(ApiKeys.BROKER_HEARTBEAT, new BrokerHeartbeatResponse()),
                new ResponseCase(ApiKeys.REPLICA_FETCH, new ReplicaFetchResponse("orders", 0, 12L, 4L, 13L,
                        List.of(new FetchedRecord(11L, bytes("k"), bytes("v"))))),
                new ResponseCase(ApiKeys.MARK_BROKER_OFFLINE, new MarkBrokerOfflineResponse()),
                new ResponseCase(ApiKeys.UPDATE_REPLICA_LEO, new UpdateReplicaLeoResponse()),
                new ResponseCase(ApiKeys.MAINTENANCE_REPORT, new MaintenanceReportResponse(
                        true,
                        "SUCCESS",
                        List.of(new MaintenanceSegmentInfo(
                                "orders-0", "RETENTION_DELETE", 0L, 0L, 9L, 1024L)),
                        0L,
                        0L,
                        1024L,
                        1L,
                        0L,
                        0L)),
                new ResponseCase(ApiKeys.MAINTENANCE_RUN, new MaintenanceReportResponse(
                        false,
                        "NOOP",
                        List.of(),
                        0L,
                        0L,
                        0L,
                        0L,
                        0L,
                        0L)),
                new ResponseCase(ApiKeys.MAINTENANCE_METRICS, new MaintenanceMetricsResponse(
                        2L,
                        1L,
                        1024L,
                        1L,
                        2048L,
                        2L,
                        3L,
                        4L,
                        "SUCCESS")),
                new ResponseCase(ApiKeys.RUNTIME_METRICS, new RuntimeMetricsResponse(
                        List.of(new RuntimeMetricCount(ApiKeys.PRODUCE, 2L)),
                        List.of(new RuntimeErrorCount(ErrorCode.NONE.code, 2L)),
                        List.of(new RuntimeLatencySummary(ApiKeys.PRODUCE, 2L, 20L, 10.0)),
                        List.of(new RuntimeMetricCount(ApiKeys.FETCH, 1L)))),
                new ResponseCase(ApiKeys.DISCOVER_CONTROLLER, new DiscoverControllerResponse(
                        new ControllerMetadata(1, "localhost", 9093, 3L), 8L)),
                new ResponseCase(ApiKeys.LEADER_READY, new LeaderReadyResponse()));

        for (ResponseCase responseCase : cases) {
            var frame = ProtocolSerdes.encodeResponse(new MqResponseHeader(9, ErrorCode.NONE), responseCase.body());
            var payload = unwrapRetainedPayload(frame);
            try {
                MqDecoder.DecodedResponse decoded = MqDecoder.decodeResponse(payload, responseCase.apiKey());
                assertEquals(9, decoded.header().correlationId());
                assertEquals(ErrorCode.NONE, decoded.header().errorCode());
                assertBodyEquals(responseCase.body(), decoded.body());
            } finally {
                payload.release();
                frame.release();
            }
        }
    }

    @Test
    void errorResponseSkipsBody() {
        var frame = ProtocolSerdes.encodeResponse(
                new MqResponseHeader(10, ErrorCode.INVALID_REQUEST), new CreateTopicResponse("ignored"));
        var payload = unwrapRetainedPayload(frame);
        try {
            MqDecoder.DecodedResponse decoded = MqDecoder.decodeResponse(payload, ApiKeys.CREATE_TOPIC);
            assertEquals(ErrorCode.INVALID_REQUEST, decoded.header().errorCode());
            assertNull(decoded.body());
        } finally {
            payload.release();
            frame.release();
        }
    }

    @Test
    void maintenanceAuthorizationErrorsRoundTripWithoutBodies() {
        for (ErrorCode errorCode : List.of(ErrorCode.UNAUTHORIZED, ErrorCode.MAINTENANCE_BUSY)) {
            var frame = ProtocolSerdes.encodeResponse(new MqResponseHeader(17, errorCode), null);
            var payload = unwrapRetainedPayload(frame);
            try {
                MqDecoder.DecodedResponse decoded =
                        MqDecoder.decodeResponse(payload, ApiKeys.MAINTENANCE_RUN);
                assertEquals(17, decoded.header().correlationId());
                assertEquals(errorCode, decoded.header().errorCode());
                assertNull(decoded.body());
            } finally {
                payload.release();
                frame.release();
            }
        }
    }

    @Test
    void brokerLeaseErrorsRoundTripWithoutBodies() {
        for (ErrorCode errorCode : List.of(ErrorCode.BROKER_FENCED, ErrorCode.BROKER_ID_IN_USE)) {
            var frame = ProtocolSerdes.encodeResponse(new MqResponseHeader(21, errorCode), null);
            var payload = unwrapRetainedPayload(frame);
            try {
                var decoded = MqDecoder.decodeResponse(payload, ApiKeys.BROKER_HEARTBEAT);
                assertEquals(21, decoded.header().correlationId());
                assertEquals(errorCode, decoded.header().errorCode());
                assertNull(decoded.body());
            } finally {
                payload.release();
                frame.release();
            }
        }
    }

    @Test
    void rejectsTruncatedLengthPrefixes() {
        var stringPayload = Unpooled.buffer();
        try {
            stringPayload.writeShort(ApiKeys.CREATE_TOPIC);
            stringPayload.writeInt(1);
            stringPayload.writeByte(0);
            assertThrows(
                    IllegalArgumentException.class,
                    () -> ProtocolSerdes.decodeRequestHeader(stringPayload));
        } finally {
            stringPayload.release();
        }

        var bytesPayload = requestPayload(ApiKeys.PRODUCE_V2);
        try {
            writeString(bytesPayload, "orders");
            bytesPayload.writeInt(0);
            bytesPayload.writeLong(1L);
            bytesPayload.writeInt(1);
            bytesPayload.writeByte(0);
            bytesPayload.writeInt(1);
            bytesPayload.writeByte(0);
            MqRequestHeader header = ProtocolSerdes.decodeRequestHeader(bytesPayload);
            assertThrows(
                    IllegalArgumentException.class,
                    () -> ProtocolSerdes.decodeRequestBody(header.apiKey(), bytesPayload));
        } finally {
            bytesPayload.release();
        }
    }

    @Test
    void rejectsMalformedUtf8InV4RequestString() {
        ByteBuf payload = requestPayload(ApiKeys.CREATE_TOPIC_ADMIN);
        try {
            writeString(payload, "operator");
            payload.writeShort(1);
            payload.writeByte(0xC3);
            payload.writeInt(1);
            payload.writeInt(1);
            MqRequestHeader header = ProtocolSerdes.decodeRequestHeader(payload);

            assertThrows(
                    IllegalArgumentException.class,
                    () -> ProtocolSerdes.decodeRequestBody(header.apiKey(), payload));
        } finally {
            payload.release();
        }
    }

    @Test
    void rejectsMalformedUtf8InV4FullDecodeCredential() {
        ByteBuf payload = requestPayload(ApiKeys.AUTH_STATUS);
        try {
            payload.writeShort(1);
            payload.writeByte(0xC3);
            MqRequestHeader header = ProtocolSerdes.decodeRequestHeader(payload);

            assertThrows(
                    IllegalArgumentException.class,
                    () -> ProtocolSerdes.decodeRequestBody(header.apiKey(), payload));
        } finally {
            payload.release();
        }
    }

    @Test
    void rejectsMalformedUtf8InV4ResponseItemString() {
        ByteBuf payload = responsePayload();
        try {
            payload.writeInt(1);
            payload.writeByte(0);
            payload.writeInt(1);
            payload.writeShort(1);
            payload.writeByte(0xC3);

            assertThrows(
                    IllegalArgumentException.class,
                    () -> MqDecoder.decodeResponse(payload, ApiKeys.LIST_TOPICS));
        } finally {
            payload.release();
        }
    }

    @Test
    void v4FetchMaxBytesUsesSharedFrameBoundInRecords() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new ReplicaFetchV2Request(
                        "cluster", "orders", 0, 1, 1L, 1L, 1L, 0L, 0));
        assertDoesNotThrow(
                () -> new ReplicaFetchV2Request(
                        "cluster",
                        "orders",
                        0,
                        1,
                        1L,
                        1L,
                        1L,
                        0L,
                        MqFrame.MAX_FRAME_BYTES));
        assertThrows(
                IllegalArgumentException.class,
                () -> new ReplicaFetchV2Request(
                        "cluster",
                        "orders",
                        0,
                        1,
                        1L,
                        1L,
                        1L,
                        0L,
                        MqFrame.MAX_FRAME_BYTES + 1));

        assertThrows(
                IllegalArgumentException.class,
                () -> new FetchV2Request("orders", 0, 1L, 0L, 0));
        assertDoesNotThrow(
                () -> new FetchV2Request(
                        "orders", 0, 1L, 0L, MqFrame.MAX_FRAME_BYTES));
        assertThrows(
                IllegalArgumentException.class,
                () -> new FetchV2Request(
                        "orders", 0, 1L, 0L, MqFrame.MAX_FRAME_BYTES + 1));
    }

    @Test
    void v4FetchMaxBytesUsesSharedFrameBoundOnDecode() {
        for (short apiKey : List.of(ApiKeys.REPLICA_FETCH_V2, ApiKeys.FETCH_V2)) {
            assertV4FetchMaxBytesDecodeRejected(apiKey, 0);
            assertV4FetchMaxBytesDecodeAccepted(apiKey, MqFrame.MAX_FRAME_BYTES);
            assertV4FetchMaxBytesDecodeRejected(
                    apiKey, MqFrame.MAX_FRAME_BYTES + 1);
        }
    }

    @Test
    void rejectsMalformedV4BoundedListEnvelope() {
        var countExceedsTotalPayload = responsePayload();
        try {
            countExceedsTotalPayload.writeInt(0);
            countExceedsTotalPayload.writeByte(0);
            countExceedsTotalPayload.writeInt(1);
            IllegalArgumentException exception = assertThrows(
                    IllegalArgumentException.class,
                    () -> MqDecoder.decodeResponse(
                            countExceedsTotalPayload, ApiKeys.LIST_TOPICS));
            assertEquals(
                    "total cannot be smaller than encoded item count",
                    exception.getMessage());
        } finally {
            countExceedsTotalPayload.release();
        }

        var inconsistentPayload = responsePayload();
        try {
            inconsistentPayload.writeInt(1);
            inconsistentPayload.writeByte(0);
            inconsistentPayload.writeInt(0);
            assertThrows(
                    IllegalArgumentException.class,
                    () -> MqDecoder.decodeResponse(inconsistentPayload, ApiKeys.LIST_TOPICS));
        } finally {
            inconsistentPayload.release();
        }

        var oversizedPayload = responsePayload();
        try {
            oversizedPayload.writeInt(10_001);
            oversizedPayload.writeByte(0);
            oversizedPayload.writeInt(10_001);
            assertThrows(
                    IllegalArgumentException.class,
                    () -> MqDecoder.decodeResponse(oversizedPayload, ApiKeys.LIST_TOPICS));
        } finally {
            oversizedPayload.release();
        }
    }

    @Test
    void rejectsByteArrayLengthsBelowNullSentinel() {
        var payload = Unpooled.buffer();
        try {
            payload.writeInt(-2);
            assertThrows(IllegalArgumentException.class, () -> ByteBufSerde.readBytes(payload));
        } finally {
            payload.release();
        }
    }

    @Test
    void rejectsUnknownSuccessfulResponseBodyType() {
        assertThrows(
                IllegalArgumentException.class,
                () -> ProtocolSerdes.encodeResponse(
                        new MqResponseHeader(1, ErrorCode.NONE),
                        new Object()));
    }

    @Test
    void rejectsNullSuccessfulResponseBody() {
        assertThrows(
                IllegalArgumentException.class,
                () -> ProtocolSerdes.encodeResponse(
                        new MqResponseHeader(1, ErrorCode.NONE),
                        null));
    }

    @Test
    void rejectsEmptyPresentOptionalStringOnEncode() {
        assertThrows(
                IllegalArgumentException.class,
                () -> ProtocolSerdes.encodeResponse(
                        new MqResponseHeader(1, ErrorCode.NONE),
                        new ConfigureTopicReliabilityResponse("orders", "", null, 2L)));
    }

    @Test
    void allV4ResponseBodiesRoundTrip() {
        V4BoundedList<Integer> replicaIds = bounded(1, 2);
        V4BoundedList<Integer> isrIds = bounded(1, 2);
        V4BoundedList<PartitionLagInfo> lagRows = bounded(new PartitionLagInfo(
                "orders",
                0,
                8L,
                "COMMITTED",
                12L,
                4L,
                10L,
                2L,
                1_000L,
                "READY",
                false));

        List<ResponseCase> cases = List.of(
                new ResponseCase(ApiKeys.OPS_STATUS, new OpsStatusResponse(
                        1,
                        100L,
                        200L,
                        9L,
                        4,
                        "CONTROLLER",
                        7L,
                        3L,
                        "NIO",
                        "IDLE",
                        bounded(new OpsBrokerInfo(1, "localhost", 9092, "ACTIVE", 7L, 5_000L)),
                        bounded(new OpsPartitionInfo(
                                "orders",
                                0,
                                1,
                                4L,
                                12L,
                                10L,
                                1_000L,
                                "READY",
                                false,
                                replicaIds,
                                isrIds)))),
                new ResponseCase(ApiKeys.GROUP_STATUS, new GroupStatusResponse(
                        "group-1",
                        "orders",
                        2,
                        bounded(new GroupMemberInfo(
                                "member-1",
                                true,
                                150L,
                                bounded(new GroupAssignmentInfo("orders", 0)))),
                        lagRows)),
                new ResponseCase(ApiKeys.LIST_TOPICS,
                        new ListTopicsResponse(bounded("orders", "orders-retry"))),
                new ResponseCase(ApiKeys.DESCRIBE_TOPIC, new DescribeTopicResponse(
                        "orders",
                        9L,
                        true,
                        "orders-retry",
                        "orders-dlq",
                        bounded(new TopicPartitionInfo(
                                0, 1, 4L, replicaIds, isrIds, "READY", false)),
                        new TopicTombstoneInfo(
                                "DELETING",
                                9L,
                                200L,
                                bounded(1, 2),
                                bounded(1),
                                bounded(2)))),
                new ResponseCase(ApiKeys.DELETE_TOPIC, new DeleteTopicResponse(
                        "orders",
                        "DELETING",
                        9L,
                        200L,
                        bounded(1, 2),
                        bounded(1),
                        bounded(2))),
                new ResponseCase(ApiKeys.INCREASE_PARTITIONS,
                        new IncreasePartitionsResponse("orders", 3, 6, 10L)),
                new ResponseCase(ApiKeys.FAILOVER_AUDIT, new FailoverAuditResponse(
                        1,
                        100L,
                        2L,
                        bounded(new AuditEventInfo(
                                3L,
                                200L,
                                "LEADER_CHANGED",
                                1,
                                "orders",
                                0,
                                "1",
                                "2",
                                "leader failover")))),
                new ResponseCase(ApiKeys.AUTH_STATUS,
                        new AuthStatusResponse(1, bounded("STATUS", "TOPIC_ADMIN"))),
                new ResponseCase(ApiKeys.CONFIGURE_TOPIC_RELIABILITY,
                        new ConfigureTopicReliabilityResponse(
                                "orders", "orders-retry", "orders-dlq", 11L)),
                new ResponseCase(ApiKeys.MARK_BROKER_OFFLINE_ADMIN,
                        new MarkBrokerOfflineAdminResponse(2, 12L)),
                new ResponseCase(ApiKeys.CREATE_TOPIC_ADMIN,
                        new CreateTopicAdminResponse("orders", 13L)),
                new ResponseCase(ApiKeys.PRODUCE_V2,
                        new ProduceV2Response("orders", 0, 12L)),
                new ResponseCase(ApiKeys.REPLICA_FETCH_V2, new ReplicaFetchV2Response(
                        "orders",
                        0,
                        12L,
                        10L,
                        1_000L,
                        4L,
                        13L,
                        bounded(new ScheduledReplicaRecord(
                                11L, bytes("k"), bytes("v"), 1_000L)))),
                new ResponseCase(ApiKeys.TOPIC_DELETE_ACK, new TopicDeleteAckResponse()),
                new ResponseCase(ApiKeys.FETCH_V2, new FetchV2Response(
                        "orders",
                        0,
                        12L,
                        10L,
                        1_000L,
                        bounded(new FetchedRecord(9L, bytes("k"), bytes("v"))))),
                new ResponseCase(ApiKeys.METADATA_V2, new MetadataV2Response(
                        bounded(new OpsBrokerInfo(
                                1, "localhost", 9092, "ACTIVE", 7L, 5_000L)),
                        bounded(new TopicSummary(
                                "orders", 3, 2, "orders-retry", "orders-dlq")),
                        bounded(new TopicTombstoneSummary(
                                "orders-deleted",
                                new TopicTombstoneInfo(
                                        "DELETING",
                                        14L,
                                        1_000L,
                                        bounded(1, 2),
                                        bounded(1),
                                        bounded(2)))),
                        new ControllerMetadata(1, "localhost", 9092, 3L),
                        13L,
                        4)),
                new ResponseCase(ApiKeys.RUNTIME_METRICS_V2, new RuntimeMetricsV2Response(
                        100L,
                        200L,
                        100L,
                        bounded(new RuntimeMetricCount(ApiKeys.PRODUCE_V2, 2L)),
                        bounded(new RuntimeErrorCount(ErrorCode.FORBIDDEN.code, 1L)),
                        bounded(new RuntimeLatencyV2Summary(
                                ApiKeys.PRODUCE_V2, 2L, 20_000_000L, 12.5)),
                        bounded(new RuntimeMetricCount(ApiKeys.PRODUCE_V2, 1L)),
                        128L,
                        64L,
                        2L,
                        lagRows)),
                new ResponseCase(
                        ApiKeys.UPDATE_REPLICA_STATE_V2,
                        new UpdateReplicaStateV2Response()));

        for (ResponseCase responseCase : cases) {
            var frame = ProtocolSerdes.encodeResponse(
                    new MqResponseHeader(47, ErrorCode.NONE), responseCase.body());
            var payload = unwrapRetainedPayload(frame);
            try {
                MqDecoder.DecodedResponse decoded =
                        MqDecoder.decodeResponse(payload, responseCase.apiKey());
                assertEquals(47, decoded.header().correlationId());
                assertEquals(ErrorCode.NONE, decoded.header().errorCode());
                assertBodyEquals(responseCase.body(), decoded.body());
                assertEquals(0, payload.readableBytes());
            } finally {
                payload.release();
                frame.release();
            }
            assertV4ResponseRejectsTruncationAndTrailingBytes(responseCase);
        }
    }

    @Test
    void allV4RequestBodiesRoundTrip() {
        List<RequestCase> cases = List.of(
                new RequestCase(ApiKeys.OPS_STATUS, new OpsStatusRequest("operator", 100, 200)),
                new RequestCase(ApiKeys.GROUP_STATUS,
                        new GroupStatusRequest("operator", "group-1", "orders", 100, 200)),
                new RequestCase(ApiKeys.LIST_TOPICS, new ListTopicsRequest("operator", 100)),
                new RequestCase(ApiKeys.DESCRIBE_TOPIC,
                        new DescribeTopicRequest("operator", "orders", 200, true)),
                new RequestCase(ApiKeys.DELETE_TOPIC, new DeleteTopicRequest("operator", "orders")),
                new RequestCase(ApiKeys.INCREASE_PARTITIONS,
                        new IncreasePartitionsRequest("operator", "orders", 6)),
                new RequestCase(ApiKeys.FAILOVER_AUDIT, new FailoverAuditRequest("operator", 50)),
                new RequestCase(ApiKeys.AUTH_STATUS, new AuthStatusRequest("operator")),
                new RequestCase(ApiKeys.CONFIGURE_TOPIC_RELIABILITY,
                        new ConfigureTopicReliabilityRequest(
                                "operator",
                                "orders",
                                true,
                                "orders-retry",
                                false,
                                false,
                                null,
                                true)),
                new RequestCase(ApiKeys.MARK_BROKER_OFFLINE_ADMIN,
                        new MarkBrokerOfflineAdminRequest("operator", 2)),
                new RequestCase(ApiKeys.CREATE_TOPIC_ADMIN,
                        new CreateTopicAdminRequest("operator", "orders", 3, 2)),
                new RequestCase(ApiKeys.PRODUCE_V2,
                        new ProduceV2Request(
                                "orders",
                                0,
                                4L,
                                bounded(new ScheduledProduceRecord(bytes("k"), bytes("v"), 123L)))),
                new RequestCase(ApiKeys.REPLICA_FETCH_V2,
                        new ReplicaFetchV2Request(
                                "cluster",
                                "orders",
                                0,
                                2,
                                7L,
                                3L,
                                4L,
                                5L,
                                4096)),
                new RequestCase(ApiKeys.TOPIC_DELETE_ACK,
                        new TopicDeleteAckRequest("cluster", "orders", 9L, 2, 7L, 3L)),
                new RequestCase(ApiKeys.FETCH_V2,
                        new FetchV2Request("orders", 0, 4L, 5L, 4096)),
                new RequestCase(ApiKeys.METADATA_V2,
                        new MetadataV2Request(null, 100, 200)),
                new RequestCase(ApiKeys.METADATA_V2,
                        new MetadataV2Request(null, "orders", "orders-deleted", 100, 200)),
                new RequestCase(ApiKeys.RUNTIME_METRICS_V2,
                        new RuntimeMetricsV2Request("operator", 100, 100, 100)),
                new RequestCase(ApiKeys.UPDATE_REPLICA_STATE_V2,
                        new UpdateReplicaStateV2Request(
                                "cluster",
                                "orders",
                                0,
                                2,
                                7L,
                                3L,
                                4L,
                                12L,
                                10L,
                                1_000L)));

        for (RequestCase requestCase : cases) {
            var frame = ProtocolSerdes.encodeRequest(
                    new MqRequestHeader(requestCase.apiKey(), 31, "v4-client"), requestCase.body());
            var payload = unwrapRetainedPayload(frame);
            try {
                MqDecoder.DecodedRequest decoded = MqDecoder.decodeRequest(payload);
                assertEquals(requestCase.apiKey(), decoded.header().apiKey());
                assertEquals(31, decoded.header().correlationId());
                assertBodyEquals(requestCase.body(), decoded.body());
                assertEquals(0, payload.readableBytes());
            } finally {
                payload.release();
                frame.release();
            }
            assertV4RequestRejectsTruncationAndTrailingBytes(requestCase);
        }
    }

    @Test
    void produceV2UsesBoundedListEnvelope() {
        ProduceV2Request body = new ProduceV2Request(
                "orders",
                0,
                4L,
                bounded(new ScheduledProduceRecord(bytes("k"), bytes("v"), 123L)));

        var encoded = ProtocolSerdes.encodeRequest(
                new MqRequestHeader(ApiKeys.PRODUCE_V2, 31, "v4-client"), body);
        var payload = unwrapRetainedPayload(encoded);
        try {
            MqRequestHeader header = ProtocolSerdes.decodeRequestHeader(payload);
            assertEquals(ApiKeys.PRODUCE_V2, header.apiKey());

            ByteBuf bodyView = payload.duplicate();
            assertEquals("orders", ByteBufSerde.readString(bodyView));
            assertEquals(0, bodyView.readInt());
            assertEquals(4L, bodyView.readLong());
            assertEquals(1, bodyView.readInt());
            assertEquals(0, bodyView.readByte());
            assertEquals(1, bodyView.readInt());

            MqDecoder.DecodedRequest decoded = new MqDecoder.DecodedRequest(
                    header,
                    ProtocolSerdes.decodeRequestBody(header.apiKey(), payload));
            assertBodyEquals(body, decoded.body());
        } finally {
            payload.release();
            encoded.release();
        }

        byte[] manualPayload = java.util.HexFormat.of().parseHex(
                "001f0000001f000976342d636c69656e74"
                        + "00066f7264657273000000000000000000000004"
                        + "000000010000000001000000016b0000000176000000000000007b");
        var rawPayload = Unpooled.wrappedBuffer(manualPayload);
        try {
            MqDecoder.DecodedRequest decoded = MqDecoder.decodeRequest(rawPayload);
            assertEquals(ApiKeys.PRODUCE_V2, decoded.header().apiKey());
            assertEquals(31, decoded.header().correlationId());
            assertBodyEquals(body, decoded.body());
        } finally {
            rawPayload.release();
        }
    }

    @Test
    void rejectsV4StatusLimitsBeforeAllocation() {
        assertMalformedOpsStatusLimit(0, 1);
        assertMalformedOpsStatusLimit(1_001, 1);
        assertMalformedOpsStatusLimit(1, 0);
        assertMalformedOpsStatusLimit(1, 1_001);
    }

    @Test
    void v4ErrorsRoundTripWithoutBodies() {
        for (ErrorCode errorCode : List.of(
                ErrorCode.FORBIDDEN, ErrorCode.UNKNOWN_BROKER, ErrorCode.STALE_TOMBSTONE)) {
            var frame = ProtocolSerdes.encodeResponse(new MqResponseHeader(31, errorCode), null);
            var payload = unwrapRetainedPayload(frame);
            try {
                var decoded = MqDecoder.decodeResponse(payload, ApiKeys.OPS_STATUS);
                assertEquals(31, decoded.header().correlationId());
                assertEquals(errorCode, decoded.header().errorCode());
                assertNull(decoded.body());
            } finally {
                payload.release();
                frame.release();
            }
        }
    }

    @Test
    void credentialPeekerReadsPrefixWithoutConsumingBody() {
        for (short apiKey : List.of(
                ApiKeys.OPS_STATUS,
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
                ApiKeys.UPDATE_REPLICA_STATE_V2)) {
            ByteBuf incompleteBody = Unpooled.buffer();
            try {
                writeString(incompleteBody, "credential");
                incompleteBody.writeByte(0x7f);
                int readerIndex = incompleteBody.readerIndex();
                int readableBytes = incompleteBody.readableBytes();

                assertEquals(
                        "credential",
                        ProtocolSerdes.peekRequestCredential(apiKey, incompleteBody));
                assertEquals(readerIndex, incompleteBody.readerIndex());
                assertEquals(readableBytes, incompleteBody.readableBytes());
            } finally {
                incompleteBody.release();
            }
        }

        assertCredentialPeek(
                ApiKeys.AUTH_STATUS,
                new AuthStatusRequest("operator-token"),
                "operator-token");
        assertCredentialPeek(
                ApiKeys.TOPIC_DELETE_ACK,
                new TopicDeleteAckRequest("cluster-token", "orders", 9L, 2, 7L, 3L),
                "cluster-token");
    }

    @Test
    void credentialPeekerRejectsNonCredentialApi() {
        var payload = Unpooled.buffer();
        try {
            assertThrows(
                    IllegalArgumentException.class,
                    () -> ProtocolSerdes.peekRequestCredential(ApiKeys.PRODUCE_V2, payload));
        } finally {
            payload.release();
        }
    }

    @Test
    void credentialPeekerRejectsMalformedUtf8WithoutConsumingBody() {
        var payload = Unpooled.buffer();
        try {
            payload.writeShort(1);
            payload.writeByte(0xC3);
            assertCredentialPeekRejectedWithoutConsuming(payload);
        } finally {
            payload.release();
        }
    }

    @Test
    void credentialPeekerRejectsOverlongCredentialWithoutConsumingBody() {
        var payload = Unpooled.buffer();
        try {
            payload.writeShort(257);
            payload.writeZero(257);
            assertCredentialPeekRejectedWithoutConsuming(payload);
        } finally {
            payload.release();
        }
    }

    @Test
    void credentialPeekerRejectsTruncatedPrefixWithoutConsumingBody() {
        var payload = Unpooled.buffer();
        try {
            payload.writeByte(0);
            assertCredentialPeekRejectedWithoutConsuming(payload);
        } finally {
            payload.release();
        }
    }

    @Test
    void rejectsTrailingRequestAndResponseBytes() {
        var requestPayload = requestPayload(ApiKeys.AUTH_STATUS);
        try {
            writeString(requestPayload, "operator");
            requestPayload.writeByte(1);
            MqRequestHeader header = ProtocolSerdes.decodeRequestHeader(requestPayload);
            assertThrows(
                    IllegalArgumentException.class,
                    () -> ProtocolSerdes.decodeRequestBody(header.apiKey(), requestPayload));
        } finally {
            requestPayload.release();
        }

        var successPayload = responsePayload();
        try {
            writeString(successPayload, "orders");
            successPayload.writeByte(1);
            assertThrows(
                    IllegalArgumentException.class,
                    () -> MqDecoder.decodeResponse(successPayload, ApiKeys.CREATE_TOPIC));
        } finally {
            successPayload.release();
        }

        var errorPayload = Unpooled.buffer();
        try {
            errorPayload.writeInt(1);
            errorPayload.writeShort(ErrorCode.FORBIDDEN.code);
            errorPayload.writeByte(1);
            assertThrows(
                    IllegalArgumentException.class,
                    () -> MqDecoder.decodeResponse(errorPayload, ApiKeys.OPS_STATUS));
        } finally {
            errorPayload.release();
        }
    }

    @Test
    void controlPlaneAndLeaderEpochErrorsRoundTripWithoutBodies() {
        for (ErrorCode errorCode : List.of(
                ErrorCode.CONTROLLER_FENCED,
                ErrorCode.NOT_CONTROLLER,
                ErrorCode.STALE_LEADER_EPOCH,
                ErrorCode.LEADER_NOT_READY,
                ErrorCode.CONTROLLER_NOT_READY)) {
            var frame = ProtocolSerdes.encodeResponse(new MqResponseHeader(22, errorCode), null);
            var payload = unwrapRetainedPayload(frame);
            try {
                var decoded = MqDecoder.decodeResponse(payload, ApiKeys.DISCOVER_CONTROLLER);
                assertEquals(errorCode, decoded.header().errorCode());
                assertNull(decoded.body());
            } finally {
                payload.release();
                frame.release();
            }
        }
    }

    @Test
    void rejectsNegativeEpochsOnEncode() {
        assertThrows(IllegalArgumentException.class, () -> ProtocolSerdes.encodeRequest(
                new MqRequestHeader(ApiKeys.PRODUCE, 1, "client"),
                new ProduceRequest("orders", 0, -1L, List.of())));
        assertThrows(IllegalArgumentException.class, () -> ProtocolSerdes.encodeRequest(
                new MqRequestHeader(ApiKeys.BROKER_HEARTBEAT, 1, "client"),
                new BrokerHeartbeatRequest(1, 2L, -1L)));
        assertThrows(IllegalArgumentException.class, () -> ProtocolSerdes.encodeResponse(
                new MqResponseHeader(1, ErrorCode.NONE),
                new DiscoverControllerResponse(
                        new ControllerMetadata(1, "localhost", 9092, -1L), 0L)));
    }

    @Test
    void fetchPrefixPlusRecordBytesEqualsHeapFrame() {
        byte[] key = bytes("k");
        byte[] value = bytes("value");
        MqResponseHeader header = new MqResponseHeader(7, ErrorCode.NONE);
        FetchResponse response = new FetchResponse(
                "orders", 0, 9, List.of(new FetchedRecord(3, key, value)));
        ByteBuf heap = ProtocolSerdes.encodeResponse(header, response);
        ByteBuf recordBytes = Unpooled.buffer();
        recordBytes.writeLong(3);
        ByteBufSerde.writeBytes(recordBytes, key);
        ByteBufSerde.writeBytes(recordBytes, value);
        ByteBuf prefix = ProtocolSerdes.encodeFetchResponsePrefix(
                header, "orders", 0, 9, 1, recordBytes.readableBytes());
        ByteBuf combined = Unpooled.wrappedUnmodifiableBuffer(prefix, recordBytes);
        try {
            assertEquals(heap, combined);
        } finally {
            heap.release();
            combined.release();
        }
    }

    @Test
    void fetchPrefixRejectsInvalidCountsAndFrameOverflow() {
        MqResponseHeader header = new MqResponseHeader(1, ErrorCode.NONE);
        assertThrows(IllegalArgumentException.class,
                () -> ProtocolSerdes.encodeFetchResponsePrefix(header, "orders", 0, 0, -1, 0));
        assertThrows(IllegalArgumentException.class,
                () -> ProtocolSerdes.encodeFetchResponsePrefix(
                        header, "orders", 0, 0, 0, MqFrame.MAX_FRAME_BYTES));
    }

    @Test
    void rejectsMalformedProduceRecordCounts() {
        assertMalformedRequestCount(ApiKeys.PRODUCE, payload -> {
            writeString(payload, "orders");
            payload.writeInt(0);
        });
    }

    @Test
    void rejectsMalformedMetadataBrokerCounts() {
        assertMalformedResponseCount(ApiKeys.METADATA, payload -> {
        });
    }

    @Test
    void rejectsMalformedMetadataTopicCounts() {
        assertMalformedResponseCount(ApiKeys.METADATA, payload -> {
            payload.writeInt(0);
        });
    }

    @Test
    void rejectsMalformedMetadataPartitionCounts() {
        assertMalformedResponseCount(ApiKeys.METADATA, payload -> {
            payload.writeInt(0);
            payload.writeInt(1);
            writeString(payload, "orders");
        });
    }

    @Test
    void rejectsMalformedFetchRecordCounts() {
        assertMalformedResponseCount(ApiKeys.FETCH, payload -> {
            writeString(payload, "orders");
            payload.writeInt(0);
            payload.writeLong(0L);
        });
    }

    @Test
    void rejectsMalformedJoinGroupMemberCounts() {
        assertMalformedResponseCount(ApiKeys.JOIN_GROUP, payload -> {
            payload.writeInt(1);
            writeString(payload, "member-1");
            writeString(payload, "member-1");
        });
    }

    @Test
    void rejectsOversizedProduceRecordListOnEncode() {
        List<ProduceRecord> records = new ArrayList<>();
        for (int i = 0; i < 10_001; i++) {
            records.add(new ProduceRecord(bytes("k"), bytes("v")));
        }

        assertThrows(IllegalArgumentException.class, () -> ProtocolSerdes.encodeRequest(
                new MqRequestHeader(ApiKeys.PRODUCE, 1, "client"),
                new ProduceRequest("orders", 0, records)));
    }

    @Test
    void encoderWritesByteBufOutbound() {
        EmbeddedChannel channel = new EmbeddedChannel(new MqEncoder());
        var message = Unpooled.wrappedBuffer(bytes("hello"));

        channel.writeOutbound(message);

        ByteBuf outbound = channel.readOutbound();
        try {
            byte[] bytes = new byte[outbound.readableBytes()];
            outbound.readBytes(bytes);
            assertArrayEquals(bytes("hello"), bytes);
        } finally {
            outbound.release();
            channel.finishAndReleaseAll();
        }
    }

    private static ByteBuf unwrapRetainedPayload(ByteBuf frame) {
        frame.readShort();
        frame.readByte();
        int length = frame.readInt();
        return frame.readRetainedSlice(length);
    }

    private static void assertV4RequestRejectsTruncationAndTrailingBytes(
            RequestCase requestCase) {
        ByteBuf frame = ProtocolSerdes.encodeRequest(
                new MqRequestHeader(requestCase.apiKey(), 31, "v4-client"),
                requestCase.body());
        ByteBuf payload = unwrapRetainedPayload(frame);
        try {
            MqRequestHeader header = ProtocolSerdes.decodeRequestHeader(payload);
            int bodyIndex = payload.readerIndex();
            int bodyLength = payload.readableBytes();

            if (bodyLength > 0) {
                ByteBuf truncated = payload.copy(bodyIndex, bodyLength - 1);
                try {
                    assertThrows(
                            IllegalArgumentException.class,
                            () -> ProtocolSerdes.decodeRequestBody(
                                    header.apiKey(), truncated));
                } finally {
                    truncated.release();
                }
            }

            ByteBuf trailing = Unpooled.buffer(bodyLength + 1);
            try {
                trailing.writeBytes(payload, bodyIndex, bodyLength);
                trailing.writeByte(1);
                assertThrows(
                        IllegalArgumentException.class,
                        () -> ProtocolSerdes.decodeRequestBody(header.apiKey(), trailing));
            } finally {
                trailing.release();
            }
        } finally {
            payload.release();
            frame.release();
        }
    }

    private static void assertV4ResponseRejectsTruncationAndTrailingBytes(
            ResponseCase responseCase) {
        ByteBuf frame = ProtocolSerdes.encodeResponse(
                new MqResponseHeader(47, ErrorCode.NONE), responseCase.body());
        ByteBuf payload = unwrapRetainedPayload(frame);
        try {
            ByteBuf bodyView = payload.duplicate();
            ProtocolSerdes.decodeResponseHeader(bodyView);
            if (bodyView.isReadable()) {
                ByteBuf truncated = payload.copy(
                        payload.readerIndex(), payload.readableBytes() - 1);
                try {
                    assertThrows(
                            IllegalArgumentException.class,
                            () -> MqDecoder.decodeResponse(
                                    truncated, responseCase.apiKey()));
                } finally {
                    truncated.release();
                }
            }

            ByteBuf trailing = Unpooled.buffer(payload.readableBytes() + 1);
            try {
                trailing.writeBytes(payload, payload.readerIndex(), payload.readableBytes());
                trailing.writeByte(1);
                assertThrows(
                        IllegalArgumentException.class,
                        () -> MqDecoder.decodeResponse(trailing, responseCase.apiKey()));
            } finally {
                trailing.release();
            }
        } finally {
            payload.release();
            frame.release();
        }
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static void assertMalformedRequestCount(short apiKey, PayloadPrefixWriter prefixWriter) {
        assertMalformedRequestCount(apiKey, prefixWriter, -1);
        assertMalformedRequestCount(apiKey, prefixWriter, 10_001);
    }

    private static void assertMalformedRequestCount(
            short apiKey, PayloadPrefixWriter prefixWriter, int count) {
        var payload = requestPayload(apiKey);
        try {
            prefixWriter.write(payload);
            payload.writeInt(count);
            MqRequestHeader decoded = ProtocolSerdes.decodeRequestHeader(payload);
            assertThrows(IllegalArgumentException.class,
                    () -> ProtocolSerdes.decodeRequestBody(decoded.apiKey(), payload));
        } finally {
            payload.release();
        }
    }

    private static void assertMalformedResponseCount(short apiKey, PayloadPrefixWriter prefixWriter) {
        assertMalformedResponseCount(apiKey, prefixWriter, -1);
        assertMalformedResponseCount(apiKey, prefixWriter, 10_001);
    }

    private static void assertV4FetchMaxBytesDecodeAccepted(short apiKey, int maxBytes) {
        ByteBuf payload = v4FetchRequestPayload(apiKey, maxBytes);
        try {
            MqRequestHeader header = ProtocolSerdes.decodeRequestHeader(payload);
            Object body = assertDoesNotThrow(
                    () -> ProtocolSerdes.decodeRequestBody(header.apiKey(), payload));
            if (body instanceof ReplicaFetchV2Request request) {
                assertEquals(maxBytes, request.maxBytes());
            } else {
                assertEquals(maxBytes, ((FetchV2Request) body).maxBytes());
            }
        } finally {
            payload.release();
        }
    }

    private static void assertV4FetchMaxBytesDecodeRejected(short apiKey, int maxBytes) {
        ByteBuf payload = v4FetchRequestPayload(apiKey, maxBytes);
        try {
            MqRequestHeader header = ProtocolSerdes.decodeRequestHeader(payload);
            assertThrows(
                    IllegalArgumentException.class,
                    () -> ProtocolSerdes.decodeRequestBody(header.apiKey(), payload));
        } finally {
            payload.release();
        }
    }

    private static ByteBuf v4FetchRequestPayload(short apiKey, int maxBytes) {
        ByteBuf payload = requestPayload(apiKey);
        if (apiKey == ApiKeys.REPLICA_FETCH_V2) {
            writeString(payload, "cluster");
            writeString(payload, "orders");
            payload.writeInt(0);
            payload.writeInt(1);
            payload.writeLong(1L);
            payload.writeLong(1L);
            payload.writeLong(1L);
            payload.writeLong(0L);
        } else {
            writeString(payload, "orders");
            payload.writeInt(0);
            payload.writeLong(1L);
            payload.writeLong(0L);
        }
        payload.writeInt(maxBytes);
        return payload;
    }

    private static void assertMalformedOpsStatusLimit(int brokerLimit, int partitionLimit) {
        var payload = requestPayload(ApiKeys.OPS_STATUS);
        try {
            writeString(payload, "operator");
            payload.writeInt(brokerLimit);
            payload.writeInt(partitionLimit);
            MqRequestHeader decoded = ProtocolSerdes.decodeRequestHeader(payload);
            assertThrows(
                    IllegalArgumentException.class,
                    () -> ProtocolSerdes.decodeRequestBody(decoded.apiKey(), payload));
        } finally {
            payload.release();
        }
    }

    private static void assertCredentialPeek(short apiKey, Object body, String expectedCredential) {
        var frame = ProtocolSerdes.encodeRequest(new MqRequestHeader(apiKey, 41, "client"), body);
        var payload = unwrapRetainedPayload(frame);
        try {
            MqRequestHeader header = ProtocolSerdes.decodeRequestHeader(payload);
            int bodyReaderIndex = payload.readerIndex();
            assertEquals(
                    expectedCredential,
                    ProtocolSerdes.peekRequestCredential(header.apiKey(), payload));
            assertEquals(bodyReaderIndex, payload.readerIndex());
            assertBodyEquals(body, ProtocolSerdes.decodeRequestBody(header.apiKey(), payload));
        } finally {
            payload.release();
            frame.release();
        }
    }

    private static void assertCredentialPeekRejectedWithoutConsuming(ByteBuf payload) {
        int readerIndex = payload.readerIndex();
        assertThrows(
                IllegalArgumentException.class,
                () -> ProtocolSerdes.peekRequestCredential(ApiKeys.AUTH_STATUS, payload));
        assertEquals(readerIndex, payload.readerIndex());
    }

    private static void assertMalformedResponseCount(
            short apiKey, PayloadPrefixWriter prefixWriter, int count) {
        var payload = responsePayload();
        try {
            prefixWriter.write(payload);
            payload.writeInt(count);
            assertThrows(IllegalArgumentException.class,
                    () -> MqDecoder.decodeResponse(payload, apiKey));
        } finally {
            payload.release();
        }
    }

    private static ByteBuf requestPayload(short apiKey) {
        var payload = Unpooled.buffer();
        payload.writeShort(apiKey);
        payload.writeInt(1);
        writeString(payload, "client");
        return payload;
    }

    private static ByteBuf responsePayload() {
        var payload = Unpooled.buffer();
        payload.writeInt(1);
        payload.writeShort(ErrorCode.NONE.code);
        return payload;
    }

    private static void writeString(ByteBuf payload, String value) {
        byte[] bytes = bytes(value);
        payload.writeShort(bytes.length);
        payload.writeBytes(bytes);
    }

    private static void assertBodyEquals(Object expected, Object actual) {
        assertEquals(expected.getClass(), actual.getClass());
        if (expected instanceof ProduceRequest e) {
            ProduceRequest a = (ProduceRequest) actual;
            assertEquals(e.topicName(), a.topicName());
            assertEquals(e.partitionId(), a.partitionId());
            assertEquals(e.records().size(), a.records().size());
            assertProduceRecordEquals(e.records().get(0), a.records().get(0));
        } else if (expected instanceof ProduceRecord e) {
            assertProduceRecordEquals(e, (ProduceRecord) actual);
        } else if (expected instanceof SyncGroupRequest e) {
            assertArrayEquals(e.assignment(), ((SyncGroupRequest) actual).assignment());
        } else if (expected instanceof SyncGroupResponse e) {
            assertArrayEquals(e.assignment(), ((SyncGroupResponse) actual).assignment());
        } else if (expected instanceof FetchResponse e) {
            FetchResponse a = (FetchResponse) actual;
            assertEquals(e.topicName(), a.topicName());
            assertEquals(e.partitionId(), a.partitionId());
            assertEquals(e.highWatermark(), a.highWatermark());
            assertFetchedRecordEquals(e.records().get(0), a.records().get(0));
        } else if (expected instanceof ReplicaFetchResponse e) {
            ReplicaFetchResponse a = (ReplicaFetchResponse) actual;
            assertEquals(e.topicName(), a.topicName());
            assertEquals(e.partitionId(), a.partitionId());
            assertEquals(e.highWatermark(), a.highWatermark());
            assertFetchedRecordEquals(e.records().get(0), a.records().get(0));
        } else if (expected instanceof ProduceV2Request e) {
            ProduceV2Request a = (ProduceV2Request) actual;
            assertEquals(e.topicName(), a.topicName());
            assertEquals(e.partitionId(), a.partitionId());
            assertEquals(e.leaderEpoch(), a.leaderEpoch());
            assertEquals(e.records().total(), a.records().total());
            assertEquals(e.records().truncated(), a.records().truncated());
            assertEquals(e.records().items().size(), a.records().items().size());
            assertScheduledProduceRecordEquals(
                    e.records().items().get(0), a.records().items().get(0));
        } else if (expected instanceof ReplicaFetchV2Response e) {
            ReplicaFetchV2Response a = (ReplicaFetchV2Response) actual;
            assertEquals(e.topicName(), a.topicName());
            assertEquals(e.partitionId(), a.partitionId());
            assertEquals(e.physicalHighWatermark(), a.physicalHighWatermark());
            assertEquals(e.visibleHighWatermark(), a.visibleHighWatermark());
            assertEquals(e.nextVisibleAtEpochMs(), a.nextVisibleAtEpochMs());
            assertEquals(e.leaderEpoch(), a.leaderEpoch());
            assertEquals(e.leaderLogEndOffset(), a.leaderLogEndOffset());
            assertEquals(e.records().total(), a.records().total());
            assertEquals(e.records().truncated(), a.records().truncated());
            assertEquals(e.records().items().size(), a.records().items().size());
            assertScheduledReplicaRecordEquals(
                    e.records().items().get(0), a.records().items().get(0));
        } else if (expected instanceof FetchV2Response e) {
            FetchV2Response a = (FetchV2Response) actual;
            assertEquals(e.topicName(), a.topicName());
            assertEquals(e.partitionId(), a.partitionId());
            assertEquals(e.physicalHighWatermark(), a.physicalHighWatermark());
            assertEquals(e.visibleHighWatermark(), a.visibleHighWatermark());
            assertEquals(e.nextVisibleAtEpochMs(), a.nextVisibleAtEpochMs());
            assertEquals(e.records().total(), a.records().total());
            assertEquals(e.records().truncated(), a.records().truncated());
            assertEquals(e.records().items().size(), a.records().items().size());
            assertFetchedRecordEquals(e.records().items().get(0), a.records().items().get(0));
        } else {
            assertEquals(expected, actual);
        }
    }

    private static void assertProduceRecordEquals(ProduceRecord expected, ProduceRecord actual) {
        assertArrayEquals(expected.key(), actual.key());
        assertArrayEquals(expected.value(), actual.value());
    }

    private static void assertFetchedRecordEquals(FetchedRecord expected, FetchedRecord actual) {
        assertEquals(expected.offset(), actual.offset());
        assertArrayEquals(expected.key(), actual.key());
        assertArrayEquals(expected.value(), actual.value());
    }

    private static void assertScheduledProduceRecordEquals(
            ScheduledProduceRecord expected, ScheduledProduceRecord actual) {
        assertArrayEquals(expected.key(), actual.key());
        assertArrayEquals(expected.value(), actual.value());
        assertEquals(expected.deliverAtEpochMs(), actual.deliverAtEpochMs());
    }

    private static void assertScheduledReplicaRecordEquals(
            ScheduledReplicaRecord expected, ScheduledReplicaRecord actual) {
        assertEquals(expected.offset(), actual.offset());
        assertArrayEquals(expected.key(), actual.key());
        assertArrayEquals(expected.value(), actual.value());
        assertEquals(expected.deliverAtEpochMs(), actual.deliverAtEpochMs());
    }

    @SafeVarargs
    private static <T> V4BoundedList<T> bounded(T... items) {
        return new V4BoundedList<>(items.length, false, List.of(items));
    }

    private static void assertGoldenRequest(short apiKey, Object body, String expectedHex) {
        byte[] expectedBytes = java.util.HexFormat.of().parseHex(expectedHex);
        assertEquals(MqFrame.VERSION, expectedBytes[2]);

        ByteBuf encoded = ProtocolSerdes.encodeRequest(
                new MqRequestHeader(apiKey, 7, "client"), body);
        try {
            assertArrayEquals(
                    expectedBytes,
                    io.netty.buffer.ByteBufUtil.getBytes(
                            encoded,
                            encoded.readerIndex(),
                            encoded.readableBytes(),
                            false));
        } finally {
            encoded.release();
        }

        ByteBuf goldenFrame = Unpooled.wrappedBuffer(expectedBytes);
        ByteBuf payload = unwrapRetainedPayload(goldenFrame);
        try {
            MqDecoder.DecodedRequest decoded = MqDecoder.decodeRequest(payload);
            assertEquals(apiKey, decoded.header().apiKey());
            assertEquals(7, decoded.header().correlationId());
            assertBodyEquals(body, decoded.body());
            ByteBuf reencoded = ProtocolSerdes.encodeRequest(decoded.header(), decoded.body());
            try {
                assertArrayEquals(
                        expectedBytes,
                        io.netty.buffer.ByteBufUtil.getBytes(
                                reencoded,
                                reencoded.readerIndex(),
                                reencoded.readableBytes(),
                                false));
            } finally {
                reencoded.release();
            }
        } finally {
            payload.release();
            goldenFrame.release();
        }
    }

    private static void assertGoldenResponse(short apiKey, Object body, String expectedHex) {
        byte[] expectedBytes = java.util.HexFormat.of().parseHex(expectedHex);
        assertEquals(MqFrame.VERSION, expectedBytes[2]);

        ByteBuf encoded = ProtocolSerdes.encodeResponse(
                new MqResponseHeader(9, ErrorCode.NONE), body);
        try {
            assertArrayEquals(
                    expectedBytes,
                    io.netty.buffer.ByteBufUtil.getBytes(
                            encoded,
                            encoded.readerIndex(),
                            encoded.readableBytes(),
                            false));
        } finally {
            encoded.release();
        }

        ByteBuf goldenFrame = Unpooled.wrappedBuffer(expectedBytes);
        ByteBuf payload = unwrapRetainedPayload(goldenFrame);
        try {
            MqDecoder.DecodedResponse decoded = MqDecoder.decodeResponse(payload, apiKey);
            assertEquals(9, decoded.header().correlationId());
            assertEquals(ErrorCode.NONE, decoded.header().errorCode());
            assertBodyEquals(body, decoded.body());
            ByteBuf reencoded = ProtocolSerdes.encodeResponse(decoded.header(), decoded.body());
            try {
                assertArrayEquals(
                        expectedBytes,
                        io.netty.buffer.ByteBufUtil.getBytes(
                                reencoded,
                                reencoded.readerIndex(),
                                reencoded.readableBytes(),
                                false));
            } finally {
                reencoded.release();
            }
        } finally {
            payload.release();
            goldenFrame.release();
        }
    }

    private record RequestCase(short apiKey, Object body) {
    }

    private record ResponseCase(short apiKey, Object body) {
    }

    @FunctionalInterface
    private interface PayloadPrefixWriter {
        void write(ByteBuf payload);
    }
}
