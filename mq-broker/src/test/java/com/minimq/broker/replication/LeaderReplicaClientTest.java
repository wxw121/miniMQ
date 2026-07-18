package com.minimq.broker.replication;

import com.minimq.common.error.ErrorCode;
import com.minimq.protocol.ApiKeys;
import com.minimq.protocol.ProtocolSerdes;
import com.minimq.protocol.codec.MqDecoder;
import com.minimq.protocol.codec.MqEncoder;
import com.minimq.protocol.message.MqResponseHeader;
import com.minimq.protocol.message.ProtocolMessages.FetchedRecord;
import com.minimq.protocol.message.ProtocolMessages.ReplicaFetchRequest;
import com.minimq.protocol.message.ProtocolMessages.ReplicaFetchResponse;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class LeaderReplicaClientTest {

    @Test
    void replicaFetchEncodesRequestAndReturnsLeaderRecords() throws Exception {
        AtomicReference<ReplicaFetchRequest> captured = new AtomicReference<>();
        try (TestLeader leader = TestLeader.start(ErrorCode.NONE,
                new ReplicaFetchResponse("orders", 0, 7L,
                        List.of(new FetchedRecord(3L, "k".getBytes(), "v".getBytes()))),
                captured);
             LeaderReplicaClient client = new LeaderReplicaClient()) {
            client.connect("127.0.0.1", leader.port());

            ReplicaFetchResponse response = client.replicaFetch("orders", 0, 1, 3L, 1024);

            assertEquals(new ReplicaFetchRequest("orders", 0, 1, 3L, 1024), captured.get());
            assertEquals(7L, response.highWatermark());
            assertEquals(1, response.records().size());
            assertEquals(3L, response.records().get(0).offset());
            assertArrayEquals("k".getBytes(), response.records().get(0).key());
            assertArrayEquals("v".getBytes(), response.records().get(0).value());
        }
    }

    @Test
    void replicaFetchThrowsTypedExceptionForLeaderErrors() throws Exception {
        try (TestLeader leader = TestLeader.start(ErrorCode.NOT_LEADER,
                new ReplicaFetchResponse("orders", 0, 0L, List.of()),
                new AtomicReference<>());
             LeaderReplicaClient client = new LeaderReplicaClient()) {
            client.connect("127.0.0.1", leader.port());

            LeaderReplicaClient.ReplicaFetchException failure = assertThrows(
                    LeaderReplicaClient.ReplicaFetchException.class,
                    () -> client.replicaFetch("orders", 0, 1, 0L, 1024));

            assertEquals(ErrorCode.NOT_LEADER, failure.errorCode());
        }
    }

    static final class TestLeader implements AutoCloseable {
        private final EventLoopGroup boss;
        private final EventLoopGroup worker;
        private final Channel channel;

        private TestLeader(EventLoopGroup boss, EventLoopGroup worker, Channel channel) {
            this.boss = boss;
            this.worker = worker;
            this.channel = channel;
        }

        static TestLeader start(
                ErrorCode errorCode,
                ReplicaFetchResponse response,
                AtomicReference<ReplicaFetchRequest> captured) throws InterruptedException {
            EventLoopGroup boss = new NioEventLoopGroup(1);
            EventLoopGroup worker = new NioEventLoopGroup(1);
            Channel server = new ServerBootstrap()
                    .group(boss, worker)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ch.pipeline().addLast(new MqDecoder(), new SimpleChannelInboundHandler<ByteBuf>() {
                                @Override
                                protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) {
                                    MqDecoder.DecodedRequest decoded = MqDecoder.decodeRequest(msg);
                                    captured.set((ReplicaFetchRequest) decoded.body());
                                    ctx.writeAndFlush(ProtocolSerdes.encodeResponse(
                                            new MqResponseHeader(decoded.header().correlationId(), errorCode),
                                            response));
                                }
                            }, new MqEncoder());
                        }
                    })
                    .bind("127.0.0.1", 0)
                    .sync()
                    .channel();
            return new TestLeader(boss, worker, server);
        }

        int port() {
            return ((java.net.InetSocketAddress) channel.localAddress()).getPort();
        }

        @Override
        public void close() throws InterruptedException {
            channel.close().sync();
            boss.shutdownGracefully().sync();
            worker.shutdownGracefully().sync();
        }
    }
}