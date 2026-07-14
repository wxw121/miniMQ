package com.minimq.client;

import com.minimq.client.exception.MiniMqRequestTimeoutException;
import com.minimq.protocol.ApiKeys;
import com.minimq.protocol.message.ProtocolMessages.HeartbeatRequest;
import com.minimq.protocol.message.ProtocolMessages.JoinGroupRequest;
import com.minimq.protocol.message.ProtocolMessages.MetadataRequest;
import com.minimq.protocol.message.ProtocolMessages.OffsetCommitRequest;
import com.minimq.protocol.message.ProtocolMessages.OffsetFetchRequest;
import com.minimq.protocol.message.ProtocolMessages.RegisterBrokerRequest;
import io.netty.bootstrap.ServerBootstrap;
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

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NetworkClientTimeoutTest {

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

    private static List<TimeoutRequest> timeoutRequests() {
        return List.of(
                new TimeoutRequest(ApiKeys.JOIN_GROUP, new JoinGroupRequest("g", "m", "orders"), "JOIN_GROUP"),
                new TimeoutRequest(ApiKeys.HEARTBEAT, new HeartbeatRequest("g", "m"), "HEARTBEAT"),
                new TimeoutRequest(ApiKeys.OFFSET_COMMIT, new OffsetCommitRequest("g", "orders", 0, 1), "OFFSET_COMMIT"),
                new TimeoutRequest(ApiKeys.OFFSET_FETCH, new OffsetFetchRequest("g", "orders", 0), "OFFSET_FETCH"),
                new TimeoutRequest(ApiKeys.REGISTER_BROKER, new RegisterBrokerRequest(1, "127.0.0.1", 19092), "REGISTER_BROKER"));
    }

    private record TimeoutRequest(short apiKey, Object body, String operation) {
    }

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}
