package com.minimq.broker;

import com.minimq.common.error.ErrorCode;
import com.minimq.protocol.ApiKeys;
import com.minimq.protocol.ProtocolSerdes;
import com.minimq.protocol.codec.MqDecoder;
import com.minimq.protocol.codec.MqEncoder;
import com.minimq.protocol.message.MqRequestHeader;
import com.minimq.protocol.message.MqResponseHeader;
import com.minimq.protocol.message.ProtocolMessages.BrokerHeartbeatRequest;
import com.minimq.protocol.message.ProtocolMessages.BrokerHeartbeatResponse;
import com.minimq.protocol.message.ProtocolMessages.BrokerMetadata;
import com.minimq.protocol.message.ProtocolMessages.ControllerMetadata;
import com.minimq.protocol.message.ProtocolMessages.DiscoverControllerResponse;
import com.minimq.protocol.message.ProtocolMessages.LeaderReadyRequest;
import com.minimq.protocol.message.ProtocolMessages.LeaderReadyResponse;
import com.minimq.protocol.message.ProtocolMessages.MetadataResponse;
import com.minimq.protocol.message.ProtocolMessages.MetadataV2Request;
import com.minimq.protocol.message.ProtocolMessages.MetadataV2Response;
import com.minimq.protocol.message.ProtocolMessages.RegisterBrokerRequest;
import com.minimq.protocol.message.ProtocolMessages.RegisterBrokerResponse;
import com.minimq.protocol.message.ProtocolMessages.TopicDeleteAckRequest;
import com.minimq.protocol.message.ProtocolMessages.TopicDeleteAckResponse;
import com.minimq.protocol.message.ProtocolMessages.UpdateReplicaLeoRequest;
import com.minimq.protocol.message.ProtocolMessages.UpdateReplicaLeoResponse;
import com.minimq.protocol.message.ProtocolMessages.UpdateReplicaStateV2Request;
import com.minimq.protocol.message.ProtocolMessages.UpdateReplicaStateV2Response;
import com.minimq.protocol.message.ProtocolMessages.V4BoundedList;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ControllerClientTest {

    @Test
    void pendingResponseSlotReleasesAcceptedAndRejectsLatePayloadsAfterClose() {
        ControllerClient.PendingResponseSlot slot = new ControllerClient.PendingResponseSlot();
        ByteBuf accepted = Unpooled.buffer(1);
        ByteBuf late = Unpooled.buffer(1);

        assertTrue(slot.offerRetained(accepted));
        assertEquals(2, accepted.refCnt());
        slot.closeAndDrain();
        assertEquals(1, accepted.refCnt());

        assertFalse(slot.offerRetained(late));
        assertEquals(1, late.refCnt());
        accepted.release();
        late.release();
    }

    @Test
    void pendingResponseSlotReleasesPayloadWhenOfferRacesWithClose() throws Exception {
        for (int iteration = 0; iteration < 250; iteration++) {
            ControllerClient.PendingResponseSlot slot = new ControllerClient.PendingResponseSlot();
            ByteBuf payload = Unpooled.buffer(1);
            CountDownLatch start = new CountDownLatch(1);
            Thread offer = new Thread(() -> {
                awaitUninterruptibly(start);
                slot.offerRetained(payload);
            });
            Thread close = new Thread(() -> {
                awaitUninterruptibly(start);
                slot.closeAndDrain();
            });
            offer.start();
            close.start();
            start.countDown();
            offer.join();
            close.join();

            assertEquals(1, payload.refCnt(), "racing cleanup leaked or over-released payload");
            payload.release();
        }
    }

    private static void awaitUninterruptibly(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    @Test
    void lateResponseFromTimedOutRequestDoesNotSatisfyNextRequest() throws Exception {
        int port = freePort();
        EventLoopGroup boss = new NioEventLoopGroup(1);
        EventLoopGroup worker = new NioEventLoopGroup(1);
        AtomicInteger requestCount = new AtomicInteger();
        CountDownLatch lateResponseWritten = new CountDownLatch(1);
        Channel server = new ServerBootstrap()
                .group(boss, worker)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline().addLast(new MqDecoder(), new SimpleChannelInboundHandler<ByteBuf>() {
                            @Override
                            protected void channelRead0(ChannelHandlerContext ctx, ByteBuf payload) {
                                MqRequestHeader header = ProtocolSerdes.decodeRequestHeader(payload);
                                if (requestCount.incrementAndGet() == 1) {
                                    ctx.executor().schedule(
                                            () -> ctx.writeAndFlush(ProtocolSerdes.encodeResponse(
                                                    new MqResponseHeader(
                                                            header.correlationId(), ErrorCode.NONE),
                                                    new BrokerHeartbeatResponse()))
                                                    .addListener(ignored -> lateResponseWritten.countDown()),
                                            75,
                                            TimeUnit.MILLISECONDS);
                                    return;
                                }
                                ctx.writeAndFlush(ProtocolSerdes.encodeResponse(
                                        new MqResponseHeader(header.correlationId(), ErrorCode.NONE),
                                        new MetadataResponse(
                                                List.of(new BrokerMetadata(
                                                        7, "127.0.0.1", 19092)),
                                                List.of(),
                                                null,
                                                42L)));
                            }
                        }, new MqEncoder());
                    }
                })
                .bind("127.0.0.1", port)
                .sync()
                .channel();

        try (ControllerClient client = new ControllerClient()) {
            client.connect("127.0.0.1", port);

            assertThrows(IllegalStateException.class,
                    () -> client.heartbeatBroker(new BrokerHeartbeatRequest(7, 1L), 25L));
            assertTrue(lateResponseWritten.await(1, TimeUnit.SECONDS));

            MetadataResponse metadata = client.fetchMetadata(1_000L);

            assertEquals(42L, metadata.metadataVersion());
            assertEquals(7, metadata.brokers().get(0).brokerId());
            assertEquals(2, requestCount.get());
        } finally {
            server.close().sync();
            boss.shutdownGracefully().sync();
            worker.shutdownGracefully().sync();
        }
    }

    @Test
    void controllerRpcMethodsDecodeTheirMatchingResponses() throws Exception {
        int port = freePort();
        EventLoopGroup boss = new NioEventLoopGroup(1);
        EventLoopGroup worker = new NioEventLoopGroup(1);
        AtomicInteger requestCount = new AtomicInteger();
        Channel server = startControllerServer(port, boss, worker, ErrorCode.NONE, requestCount);

        try (ControllerClient client = new ControllerClient()) {
            client.connect("127.0.0.1", port);

            RegisterBrokerResponse registered = client.registerBroker(
                    new RegisterBrokerRequest(7, "127.0.0.1", 19092, 3L));
            client.heartbeatBroker(new BrokerHeartbeatRequest(7, registered.brokerEpoch()));
            MetadataResponse metadata = client.fetchMetadata();
            MetadataV2Response metadataV2 = client.fetchMetadataV2();
            client.updateReplicaLeo(new UpdateReplicaLeoRequest("orders", 0, 7, 4L));
            client.updateReplicaStateV2(new UpdateReplicaStateV2Request(
                    "cluster-secret", "orders", 0, 7, 9L, 3L, 2L, 4L, 4L, -1L));
            client.acknowledgeTopicDeletion(new TopicDeleteAckRequest(
                    "cluster-secret", "orders", 5L, 7, 9L, 3L));
            client.leaderReady(new LeaderReadyRequest("orders", 0, 7, 9L, 3L, 2L, 4L));
            DiscoverControllerResponse discovered = client.discoverController();

            assertEquals(9L, registered.brokerEpoch());
            assertEquals(3_000L, registered.leaseTimeoutMs());
            assertEquals(42L, metadata.metadataVersion());
            assertEquals(43L, metadataV2.metadataVersion());
            assertEquals(7, discovered.controller().brokerId());
            assertEquals(44L, discovered.metadataVersion());
            assertEquals(9, requestCount.get());
        } finally {
            server.close().sync();
            boss.shutdownGracefully().sync();
            worker.shutdownGracefully().sync();
        }
    }

    @Test
    void fetchMetadataV2SendsPaginationCursorsForLargeLegacySnapshots() throws Exception {
        int port = freePort();
        EventLoopGroup boss = new NioEventLoopGroup(1);
        EventLoopGroup worker = new NioEventLoopGroup(1);
        AtomicInteger requestedTopicLimit = new AtomicInteger();
        AtomicReference<String> topicStartAfter = new AtomicReference<>();
        AtomicReference<String> tombstoneStartAfter = new AtomicReference<>();
        AtomicReference<String> clientId = new AtomicReference<>();
        Channel server = new ServerBootstrap()
                .group(boss, worker)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline().addLast(new MqDecoder(), new SimpleChannelInboundHandler<ByteBuf>() {
                            @Override
                            protected void channelRead0(ChannelHandlerContext ctx, ByteBuf payload) {
                                MqRequestHeader header = ProtocolSerdes.decodeRequestHeader(payload);
                                MetadataV2Request request =
                                        (MetadataV2Request) ProtocolSerdes.decodeRequestBody(
                                                header.apiKey(), payload);
                                requestedTopicLimit.set(request.topicLimit());
                                topicStartAfter.set(request.topicStartAfter());
                                tombstoneStartAfter.set(request.tombstoneStartAfter());
                                clientId.set(header.clientId());
                                ctx.writeAndFlush(ProtocolSerdes.encodeResponse(
                                        new MqResponseHeader(header.correlationId(), ErrorCode.NONE),
                                        new MetadataV2Response(
                                                new V4BoundedList<>(0, false, List.of()),
                                                new V4BoundedList<>(0, false, List.of()),
                                                new V4BoundedList<>(0, false, List.of()),
                                                new ControllerMetadata(7, "127.0.0.1", 19092, 3L),
                                                43L,
                                                4)));
                            }
                        }, new MqEncoder());
                    }
                })
                .bind("127.0.0.1", port)
                .sync()
                .channel();

        try (ControllerClient client = new ControllerClient()) {
            client.connect("127.0.0.1", port);

            client.fetchMetadataV2("topic-09999", "deleted-09999", 1_000L);

            assertEquals("broker-internal", clientId.get());
            assertEquals(10_000, requestedTopicLimit.get());
            assertEquals("topic-09999", topicStartAfter.get());
            assertEquals("deleted-09999", tombstoneStartAfter.get());
        } finally {
            server.close().sync();
            boss.shutdownGracefully().sync();
            worker.shutdownGracefully().sync();
        }
    }

    @Test
    void controllerErrorsAndInvalidBudgetsAreReportedExplicitly() throws Exception {
        int port = freePort();
        EventLoopGroup boss = new NioEventLoopGroup(1);
        EventLoopGroup worker = new NioEventLoopGroup(1);
        AtomicInteger requestCount = new AtomicInteger();
        Channel server = startControllerServer(
                port, boss, worker, ErrorCode.CONTROLLER_FENCED, requestCount);

        try (ControllerClient client = new ControllerClient()) {
            client.connect("127.0.0.1", port, 1_000L);

            ControllerClient.ControllerRequestException failure = assertThrows(
                    ControllerClient.ControllerRequestException.class,
                    () -> client.heartbeatBroker(new BrokerHeartbeatRequest(7, 9L), 1_000L));
            assertEquals(ErrorCode.CONTROLLER_FENCED, failure.errorCode());
            assertThrows(IllegalArgumentException.class,
                    () -> client.heartbeatBroker(new BrokerHeartbeatRequest(7, 9L), 0L));
            assertThrows(IllegalArgumentException.class, () -> client.fetchMetadata(0L));
            assertThrows(IllegalArgumentException.class, () -> client.fetchMetadataV2(-1L));
            assertThrows(IllegalArgumentException.class, () -> client.acknowledgeTopicDeletion(
                    new TopicDeleteAckRequest("cluster-secret", "orders", 5L, 7, 9L, 3L),
                    0L));
            assertThrows(ControllerClient.ControllerRequestException.class,
                    () -> client.heartbeatBroker(new BrokerHeartbeatRequest(7, 9L), 1_000L));
            assertEquals(2, requestCount.get(), "invalid budgets must fail before writing an RPC");
        } finally {
            server.close().sync();
            boss.shutdownGracefully().sync();
            worker.shutdownGracefully().sync();
        }

        ControllerClient invalid = new ControllerClient();
        try {
            assertThrows(IllegalArgumentException.class,
                    () -> invalid.connect("127.0.0.1", port, 0L));
            assertThrows(IllegalArgumentException.class, () -> invalid.close(0L));
        } finally {
            invalid.close(1_000L);
        }

        int unavailablePort = freePort();
        try (ControllerClient unavailable = new ControllerClient()) {
            IllegalStateException connectFailure = assertThrows(
                    IllegalStateException.class,
                    () -> unavailable.connect("127.0.0.1", unavailablePort, 1_000L));
            assertTrue(connectFailure.getMessage().contains("Failed to connect"));
        }
    }

    private static Channel startControllerServer(
            int port,
            EventLoopGroup boss,
            EventLoopGroup worker,
            ErrorCode responseError,
            AtomicInteger requestCount) throws InterruptedException {
        return new ServerBootstrap()
                .group(boss, worker)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline().addLast(new MqDecoder(), new SimpleChannelInboundHandler<ByteBuf>() {
                            @Override
                            protected void channelRead0(ChannelHandlerContext ctx, ByteBuf payload) {
                                MqRequestHeader header = ProtocolSerdes.decodeRequestHeader(payload);
                                requestCount.incrementAndGet();
                                ctx.writeAndFlush(ProtocolSerdes.encodeResponse(
                                        new MqResponseHeader(header.correlationId(), responseError),
                                        responseBody(header.apiKey())));
                            }
                        }, new MqEncoder());
                    }
                })
                .bind("127.0.0.1", port)
                .sync()
                .channel();
    }

    private static Object responseBody(short apiKey) {
        return switch (apiKey) {
            case ApiKeys.REGISTER_BROKER -> new RegisterBrokerResponse(9L, 3_000L);
            case ApiKeys.BROKER_HEARTBEAT -> new BrokerHeartbeatResponse();
            case ApiKeys.METADATA -> new MetadataResponse(List.of(), List.of(), null, 42L);
            case ApiKeys.METADATA_V2 -> new MetadataV2Response(
                    new V4BoundedList<>(0, false, List.of()),
                    new V4BoundedList<>(0, false, List.of()),
                    new V4BoundedList<>(0, false, List.of()),
                    new ControllerMetadata(7, "127.0.0.1", 19092, 3L),
                    43L,
                    4);
            case ApiKeys.UPDATE_REPLICA_LEO -> new UpdateReplicaLeoResponse();
            case ApiKeys.UPDATE_REPLICA_STATE_V2 -> new UpdateReplicaStateV2Response();
            case ApiKeys.TOPIC_DELETE_ACK -> new TopicDeleteAckResponse();
            case ApiKeys.LEADER_READY -> new LeaderReadyResponse();
            case ApiKeys.DISCOVER_CONTROLLER -> new DiscoverControllerResponse(
                    new ControllerMetadata(7, "127.0.0.1", 19092, 3L),
                    44L);
            default -> throw new IllegalArgumentException("Unexpected API key " + apiKey);
        };
    }

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            socket.setReuseAddress(true);
            return socket.getLocalPort();
        }
    }
}
