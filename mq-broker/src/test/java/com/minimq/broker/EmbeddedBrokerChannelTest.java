package com.minimq.broker;

import com.minimq.broker.controller.ClusterController;
import com.minimq.broker.controller.GroupCoordinator;
import com.minimq.broker.handler.MqRequestHandler;
import com.minimq.broker.metadata.MetadataStore;
import com.minimq.common.error.ErrorCode;
import com.minimq.protocol.ApiKeys;
import com.minimq.protocol.MqFrame;
import com.minimq.protocol.ProtocolSerdes;
import com.minimq.protocol.codec.MqDecoder;
import com.minimq.protocol.codec.MqEncoder;
import com.minimq.protocol.message.MqRequestHeader;
import com.minimq.protocol.message.ProtocolMessages.CreateTopicRequest;
import com.minimq.protocol.message.ProtocolMessages.MetadataRequest;
import com.minimq.protocol.message.ProtocolMessages.ProduceRecord;
import com.minimq.protocol.message.ProtocolMessages.ProduceRequest;
import com.minimq.protocol.message.ProtocolMessages.RegisterBrokerRequest;
import com.minimq.storage.LogManager;
import io.netty.buffer.ByteBuf;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class EmbeddedBrokerChannelTest {

    @TempDir
    Path tempDir;

    @Test
    void createTopicOverEmbeddedChannel() {
        MetadataStore store = new MetadataStore(tempDir);
        ClusterController clusterController = new ClusterController(store);
        GroupCoordinator groupCoordinator = new GroupCoordinator(clusterController);
        LogManager logManager = new LogManager(tempDir);
        PartitionManager partitionManager = new PartitionManager(logManager);
        BrokerConfig config = new BrokerConfig(0, "127.0.0.1", 9092, tempDir, true, "127.0.0.1", 9092, false);

        EmbeddedChannel channel = new EmbeddedChannel(
                new MqDecoder(),
                new MqRequestHandler(config, clusterController, groupCoordinator, partitionManager),
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
        BrokerConfig config = new BrokerConfig(0, "127.0.0.1", 9092, tempDir, true, "127.0.0.1", 9092, false);

        EmbeddedChannel channel = new EmbeddedChannel(
                new MqDecoder(),
                new MqRequestHandler(config, clusterController, groupCoordinator, partitionManager),
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

        assertEquals(ErrorCode.INVALID_REQUEST, response.header().errorCode());
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

    private EmbeddedChannel newChannel(ClusterController clusterController, boolean controller) {
        GroupCoordinator groupCoordinator = new GroupCoordinator(clusterController);
        LogManager logManager = new LogManager(tempDir);
        PartitionManager partitionManager = new PartitionManager(logManager);
        BrokerConfig config = new BrokerConfig(
                0, "127.0.0.1", 9092, tempDir, controller, "127.0.0.1", 9092, false);
        return new EmbeddedChannel(
                new MqDecoder(),
                new MqRequestHandler(config, clusterController, groupCoordinator, partitionManager),
                new MqEncoder());
    }

    private static MqDecoder.DecodedResponse send(EmbeddedChannel channel, short apiKey, Object body) {
        MqRequestHeader header = new MqRequestHeader(apiKey, 11, "test");
        channel.writeInbound(ProtocolSerdes.encodeRequest(header, body));
        ByteBuf responseFrame = channel.readOutbound();
        try {
            return MqDecoder.decodeResponse(unwrapPayload(responseFrame), apiKey);
        } finally {
            responseFrame.release();
        }
    }
}
