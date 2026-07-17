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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ProtocolSerdesTest {

    @Test
    void createTopicRoundTrip() {
        MqRequestHeader header = new MqRequestHeader(ApiKeys.CREATE_TOPIC, 42, "client-1");
        CreateTopicRequest body = new CreateTopicRequest("orders", 3);
        var encoded = ProtocolSerdes.encodeRequest(header, body);
        encoded.readShort();
        encoded.readByte();
        int len = encoded.readInt();
        var payload = encoded.readRetainedSlice(len);
        var decodedHeader = ProtocolSerdes.decodeRequestHeader(payload);
        var decodedBody = (CreateTopicRequest) ProtocolSerdes.decodeRequestBody(decodedHeader.apiKey(), payload);
        assertEquals("orders", decodedBody.topicName());
        assertEquals(3, decodedBody.numPartitions());
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
                new RequestCase(ApiKeys.PRODUCE, new ProduceRequest("orders", 1,
                        List.of(new ProduceRecord(bytes("k"), bytes("v"))))),
                new RequestCase(ApiKeys.FETCH, new FetchRequest("orders", 2, 10L, 4096)),
                new RequestCase(ApiKeys.JOIN_GROUP, new JoinGroupRequest("g1", "m1", "orders")),
                new RequestCase(ApiKeys.SYNC_GROUP, new SyncGroupRequest("g1", 2, "m1", bytes("assignment"))),
                new RequestCase(ApiKeys.HEARTBEAT, new HeartbeatRequest("g1", "m1")),
                new RequestCase(ApiKeys.OFFSET_COMMIT, new OffsetCommitRequest("g1", "orders", 1, 42L)),
                new RequestCase(ApiKeys.OFFSET_FETCH, new OffsetFetchRequest("g1", "orders", 1)),
                new RequestCase(ApiKeys.REGISTER_BROKER, new RegisterBrokerRequest(1, "localhost", 9093)));

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
                                List.of(new PartitionMetadata(0, 1), new PartitionMetadata(1, 1)))))),
                new ResponseCase(ApiKeys.PRODUCE, new ProduceResponse("orders", 0, 11L)),
                new ResponseCase(ApiKeys.FETCH, new FetchResponse("orders", 0, 12L,
                        List.of(new FetchedRecord(11L, bytes("k"), bytes("v"))))),
                new ResponseCase(ApiKeys.JOIN_GROUP, new JoinGroupResponse(1, "m1", "m1", List.of("m1", "m2"))),
                new ResponseCase(ApiKeys.SYNC_GROUP, new SyncGroupResponse(bytes("assignment"))),
                new ResponseCase(ApiKeys.HEARTBEAT, new HeartbeatResponse()),
                new ResponseCase(ApiKeys.OFFSET_COMMIT, new OffsetCommitResponse()),
                new ResponseCase(ApiKeys.OFFSET_FETCH, new OffsetFetchResponse(42L)),
                new ResponseCase(ApiKeys.REGISTER_BROKER, new RegisterBrokerResponse()));

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

    private record RequestCase(short apiKey, Object body) {
    }

    private record ResponseCase(short apiKey, Object body) {
    }

    @FunctionalInterface
    private interface PayloadPrefixWriter {
        void write(ByteBuf payload);
    }
}
