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
import com.minimq.protocol.message.ProtocolMessages.FetchRequest;
import com.minimq.protocol.message.ProtocolMessages.FetchResponse;
import com.minimq.protocol.message.ProtocolMessages.HeartbeatRequest;
import com.minimq.protocol.message.ProtocolMessages.JoinGroupRequest;
import com.minimq.protocol.message.ProtocolMessages.JoinGroupResponse;
import com.minimq.protocol.message.ProtocolMessages.MetadataRequest;
import com.minimq.protocol.message.ProtocolMessages.OffsetCommitRequest;
import com.minimq.protocol.message.ProtocolMessages.OffsetFetchRequest;
import com.minimq.protocol.message.ProtocolMessages.OffsetFetchResponse;
import com.minimq.protocol.message.ProtocolMessages.ProduceRecord;
import com.minimq.protocol.message.ProtocolMessages.ProduceRequest;
import com.minimq.protocol.message.ProtocolMessages.ProduceResponse;
import com.minimq.protocol.message.ProtocolMessages.RegisterBrokerRequest;
import com.minimq.protocol.message.ProtocolMessages.ReplicaFetchRequest;
import com.minimq.protocol.message.ProtocolMessages.ReplicaFetchResponse;
import com.minimq.protocol.message.ProtocolMessages.SyncGroupRequest;
import com.minimq.protocol.message.ProtocolMessages.SyncGroupResponse;
import com.minimq.protocol.message.ProtocolMessages.UpdateReplicaLeoRequest;
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
    void controllerAcceptsReplicaLeoUpdates() {
        ClusterController clusterController = new ClusterController(new MetadataStore(tempDir));
        clusterController.registerBroker(new RegisterBrokerRequest(0, "127.0.0.1", 9092));
        clusterController.registerBroker(new RegisterBrokerRequest(1, "127.0.0.1", 9093));
        clusterController.createTopic(new CreateTopicRequest("orders", 1, 2));
        EmbeddedChannel channel = newChannel(clusterController, true);

        MqDecoder.DecodedResponse response = send(
                channel,
                ApiKeys.UPDATE_REPLICA_LEO,
                new UpdateReplicaLeoRequest("orders", 0, 1, 9L));

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

    private EmbeddedChannel newChannel(ClusterController clusterController, boolean controller) {
        return newChannel(clusterController, controller, 0);
    }

    private EmbeddedChannel newChannel(ClusterController clusterController, boolean controller, int brokerId) {
        GroupCoordinator groupCoordinator = new GroupCoordinator(clusterController);
        LogManager logManager = new LogManager(tempDir);
        PartitionManager partitionManager = new PartitionManager(logManager);
        BrokerConfig config = new BrokerConfig(
                brokerId, "127.0.0.1", 9092, tempDir, controller, "127.0.0.1", 9092, false);
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
