package com.minimq.client;

import com.minimq.client.exception.MiniMqConnectionException;
import com.minimq.client.exception.MiniMqRequestTimeoutException;
import com.minimq.protocol.ApiKeys;
import com.minimq.protocol.message.ProtocolMessages.HeartbeatRequest;
import com.minimq.protocol.message.ProtocolMessages.JoinGroupRequest;
import com.minimq.protocol.message.ProtocolMessages.MaintenanceMetricsRequest;
import com.minimq.protocol.message.ProtocolMessages.MaintenanceReportRequest;
import com.minimq.protocol.message.ProtocolMessages.MaintenanceRunRequest;
import com.minimq.protocol.message.ProtocolMessages.MetadataRequest;
import com.minimq.protocol.message.ProtocolMessages.OffsetCommitRequest;
import com.minimq.protocol.message.ProtocolMessages.OffsetFetchRequest;
import com.minimq.protocol.message.ProtocolMessages.OpsStatusRequest;
import com.minimq.protocol.message.ProtocolMessages.RegisterBrokerRequest;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NetworkClientTimeoutTest {

    @Test
    void pendingResponseSlotReleasesAcceptedAndRejectsLatePayloadsAfterClose() {
        NetworkClient.PendingResponseSlot slot = new NetworkClient.PendingResponseSlot();
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
            NetworkClient.PendingResponseSlot slot = new NetworkClient.PendingResponseSlot();
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
    void sendThrowsTypedTimeoutExceptionWhenBrokerDoesNotRespond() throws Exception {
        int port = freePort();
        EventLoopGroup boss = new NioEventLoopGroup(1);
        EventLoopGroup worker = new NioEventLoopGroup(1);
        Channel server = new ServerBootstrap()
                .group(boss, worker)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        // Intentionally keep the channel open without writing a response.
                    }
                })
                .bind("127.0.0.1", port)
                .sync()
                .channel();

        try (NetworkClient client = new NetworkClient(
                "timeout-test",
                NetworkClient.RetryPolicy.fixedBackoff(1, 0),
                Duration.ofMillis(75))) {
            client.connect("127.0.0.1", port);

            long started = System.nanoTime();
            MiniMqRequestTimeoutException ex = assertThrows(
                    MiniMqRequestTimeoutException.class,
                    () -> client.send(ApiKeys.METADATA, new MetadataRequest("orders")));
            long elapsedMillis = (System.nanoTime() - started) / 1_000_000;

            assertTrue(ex.getMessage().contains("METADATA"));
            assertTrue(elapsedMillis < 1_000, "configured timeout should be used instead of the old 10 second default");
        } finally {
            server.close().sync();
            boss.shutdownGracefully().sync();
            worker.shutdownGracefully().sync();
        }
    }

    @Test
    void requestTimeoutRejectsInvalidValues() {
        assertThrows(IllegalArgumentException.class,
                () -> new NetworkClient(
                        "bad-timeout",
                        NetworkClient.RetryPolicy.fixedBackoff(1, 0),
                        Duration.ZERO));
        assertThrows(IllegalArgumentException.class,
                () -> new NetworkClient(
                        "huge-timeout",
                        NetworkClient.RetryPolicy.fixedBackoff(1, 0),
                        Duration.ofSeconds(Long.MAX_VALUE)));
        assertThrows(IllegalArgumentException.class,
                () -> NetworkClient.deadlineNanos(Long.MAX_VALUE, Duration.ofNanos(1L)));
    }

    @Test
    void perCallAndCandidateTimeoutsRejectNegativeValues() throws Exception {
        try (NetworkClient client = new NetworkClient("negative-call-timeout")) {
            assertThrows(IllegalArgumentException.class,
                    () -> client.connect("127.0.0.1", 1, Duration.ofMillis(-1)));
            assertThrows(IllegalArgumentException.class,
                    () -> client.abortCandidate(Duration.ofMillis(-1)));
        }
    }

    @Test
    void zeroConnectTimeoutFailsBeforeOpeningAConnection() throws Exception {
        try (NetworkClient client = new NetworkClient("zero-connect-timeout")) {
            assertThrows(MiniMqConnectionException.class,
                    () -> client.connect("127.0.0.1", 1, Duration.ZERO));
            assertEquals(0, client.connectionAttempts());
        }
    }

    @Test
    void abortCandidateClosesAnAlreadyConnectedChannel() throws Exception {
        int port = freePort();
        EventLoopGroup boss = new NioEventLoopGroup(1);
        EventLoopGroup worker = new NioEventLoopGroup(1);
        Channel server = new ServerBootstrap()
                .group(boss, worker)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        // Keep the accepted channel open until the client aborts it.
                    }
                })
                .bind("127.0.0.1", port)
                .sync()
                .channel();
        NetworkClient client = new NetworkClient("abort-connected-channel");
        try {
            client.connect("127.0.0.1", port);
        } finally {
            client.abortCandidate();
            server.close().sync();
            boss.shutdownGracefully().sync();
            worker.shutdownGracefully().sync();
        }
    }

    @Test
    void closeDrainsPendingResponseSlotsAndReleasesTheirPayloads() throws Exception {
        NetworkClient client = new NetworkClient("pending-close-drain");
        ByteBuf payload = Unpooled.buffer(1);
        NetworkClient.PendingResponseSlot slot = new NetworkClient.PendingResponseSlot();
        assertTrue(slot.offerRetained(payload));
        try {
            java.lang.reflect.Field pendingField = NetworkClient.class.getDeclaredField("pending");
            pendingField.setAccessible(true);
            @SuppressWarnings("unchecked")
            Map<Integer, NetworkClient.PendingResponseSlot> pending =
                    (Map<Integer, NetworkClient.PendingResponseSlot>) pendingField.get(client);
            pending.put(42, slot);
        } finally {
            client.close();
        }

        assertEquals(1, payload.refCnt());
        payload.release();
    }

    @Test
    void connectRetriesAndBackoffShareOneRequestTimeoutBudget() throws Exception {
        int port = freePort();
        try (NetworkClient client = new NetworkClient(
                "connect-deadline-test",
                NetworkClient.RetryPolicy.fixedBackoff(3, 500),
                Duration.ofMillis(75))) {
            long started = System.nanoTime();

            assertThrows(
                    MiniMqConnectionException.class,
                    () -> client.connect("127.0.0.1", port));
            long elapsedMillis = (System.nanoTime() - started) / 1_000_000L;

            assertEquals(1, client.connectionAttempts());
            assertTrue(elapsedMillis < 500,
                    "connect retries must not sleep past the shared request deadline");
        }
    }

    @Test
    void connectHonorsAShorterPerCallTimeout() throws Exception {
        int port = freePort();
        try (NetworkClient client = new NetworkClient(
                "per-call-connect-timeout-test",
                NetworkClient.RetryPolicy.fixedBackoff(3, 500),
                Duration.ofSeconds(1))) {
            long started = System.nanoTime();

            assertThrows(
                    MiniMqConnectionException.class,
                    () -> client.connect(
                            "127.0.0.1", port, Duration.ofMillis(75)));
            long elapsedMillis = (System.nanoTime() - started) / 1_000_000L;

            assertEquals(1, client.connectionAttempts());
            assertTrue(elapsedMillis < 500L,
                    "the remaining caller budget must cap connect retries and backoff");
        }
    }

    @Test
    void sendHonorsAShorterPerCallTimeout() throws Exception {
        int port = freePort();
        EventLoopGroup boss = new NioEventLoopGroup(1);
        EventLoopGroup worker = new NioEventLoopGroup(1);
        Channel server = new ServerBootstrap()
                .group(boss, worker)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        // Keep the channel open without responding.
                    }
                })
                .bind("127.0.0.1", port)
                .sync()
                .channel();

        try (NetworkClient client = new NetworkClient(
                "per-call-timeout-test",
                NetworkClient.RetryPolicy.fixedBackoff(1, 0),
                Duration.ofSeconds(1))) {
            client.connect("127.0.0.1", port);
            long started = System.nanoTime();

            assertThrows(
                    MiniMqRequestTimeoutException.class,
                    () -> client.send(
                            ApiKeys.METADATA,
                            new MetadataRequest("orders"),
                            Duration.ofMillis(75)));
            long elapsedMillis = (System.nanoTime() - started) / 1_000_000L;

            assertTrue(elapsedMillis < 500L,
                    "the remaining caller budget must cap the configured request timeout");
        } finally {
            server.close().sync();
            boss.shutdownGracefully().sync();
            worker.shutdownGracefully().sync();
        }
    }

    @Test
    void timeoutMessagesIdentifyProtocolOperations() throws Exception {
        int port = freePort();
        EventLoopGroup boss = new NioEventLoopGroup(1);
        EventLoopGroup worker = new NioEventLoopGroup(1);
        Channel server = new ServerBootstrap()
                .group(boss, worker)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        // Keep the connection open so each request expires at the client timeout.
                    }
                })
                .bind("127.0.0.1", port)
                .sync()
                .channel();

        try (NetworkClient client = new NetworkClient(
                "operation-timeout-test",
                NetworkClient.RetryPolicy.fixedBackoff(1, 0),
                Duration.ofMillis(75))) {
            client.connect("127.0.0.1", port);
            for (TimeoutRequest request : timeoutRequests()) {
                MiniMqRequestTimeoutException ex = assertThrows(
                        MiniMqRequestTimeoutException.class,
                        () -> client.send(request.apiKey(), request.body()));
                assertTrue(ex.getMessage().contains(request.operation()));
            }
        } finally {
            server.close().sync();
            boss.shutdownGracefully().sync();
            worker.shutdownGracefully().sync();
        }
    }

    @Test
    void clientsShareAProcessWideEventLoopGroup() throws Exception {
        NetworkClient first = new NetworkClient("shared-io-a");
        NetworkClient second = new NetworkClient("shared-io-b");
        try {
            assertSame(first.eventLoopGroup(), second.eventLoopGroup());
            first.abortCandidate();
            assertFalse(second.eventLoopGroup().isShutdown());
            assertFalse(second.eventLoopGroup().isTerminated());
        } finally {
            first.close();
            second.close();
        }
    }

    private static List<TimeoutRequest> timeoutRequests() {
        return List.of(
                new TimeoutRequest(ApiKeys.JOIN_GROUP, new JoinGroupRequest("g", "m", "orders"), "JOIN_GROUP"),
                new TimeoutRequest(ApiKeys.HEARTBEAT, new HeartbeatRequest("g", "m"), "HEARTBEAT"),
                new TimeoutRequest(ApiKeys.OFFSET_COMMIT, new OffsetCommitRequest("g", "orders", 0, 1), "OFFSET_COMMIT"),
                new TimeoutRequest(ApiKeys.OFFSET_FETCH, new OffsetFetchRequest("g", "orders", 0), "OFFSET_FETCH"),
                new TimeoutRequest(ApiKeys.REGISTER_BROKER,
                        new RegisterBrokerRequest(1, "127.0.0.1", 19092), "REGISTER_BROKER"),
                new TimeoutRequest(ApiKeys.MAINTENANCE_REPORT,
                        new MaintenanceReportRequest("secret"), "MAINTENANCE_REPORT"),
                new TimeoutRequest(ApiKeys.MAINTENANCE_RUN,
                        new MaintenanceRunRequest("secret"), "MAINTENANCE_RUN"),
                new TimeoutRequest(ApiKeys.MAINTENANCE_METRICS,
                        new MaintenanceMetricsRequest("secret"), "MAINTENANCE_METRICS"),
                new TimeoutRequest(ApiKeys.OPS_STATUS,
                        new OpsStatusRequest("secret", 1, 1), "OPS_STATUS"));
    }

    private record TimeoutRequest(short apiKey, Object body, String operation) {
    }

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}
