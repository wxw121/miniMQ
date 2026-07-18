package com.minimq.broker.replication;

import com.minimq.broker.BrokerConfig;
import com.minimq.broker.PartitionManager;
import com.minimq.broker.controller.ClusterController;
import com.minimq.broker.metadata.MetadataStore;
import com.minimq.common.error.ErrorCode;
import com.minimq.protocol.ProtocolSerdes;
import com.minimq.protocol.codec.MqDecoder;
import com.minimq.protocol.codec.MqEncoder;
import com.minimq.protocol.message.ProtocolMessages.CreateTopicRequest;
import com.minimq.protocol.message.ProtocolMessages.FetchedRecord;
import com.minimq.protocol.message.MqResponseHeader;
import com.minimq.protocol.message.ProtocolMessages.RegisterBrokerRequest;
import com.minimq.protocol.message.ProtocolMessages.ReplicaFetchRequest;
import com.minimq.protocol.message.ProtocolMessages.ReplicaFetchResponse;
import com.minimq.storage.LogManager;
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
import org.junit.jupiter.api.io.TempDir;

import java.util.ArrayList;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ReplicaFetcherTest {

    @TempDir
    Path tempDir;

    @Test
    void followerFetchesLeaderRecordsIntoLocalPartitionLog() throws Exception {
        AtomicReference<ReplicaFetchRequest> captured = new AtomicReference<>();
        try (LeaderReplicaClientTest.TestLeader leader = LeaderReplicaClientTest.TestLeader.start(
                ErrorCode.NONE,
                new ReplicaFetchResponse("orders", 0, 1L,
                        List.of(new FetchedRecord(0L, "k".getBytes(), "v".getBytes()))),
                captured)) {
            ClusterController controller = new ClusterController(new MetadataStore(tempDir.resolve("controller")));
            controller.registerBroker(new RegisterBrokerRequest(0, "127.0.0.1", leader.port()));
            controller.registerBroker(new RegisterBrokerRequest(1, "127.0.0.1", 19093));
            assertEquals(ErrorCode.NONE, controller.createTopic(new CreateTopicRequest("orders", 1, 2)));

            PartitionManager partitionManager = new PartitionManager(new LogManager(tempDir.resolve("follower")));
            ReplicaFetcher fetcher = new ReplicaFetcher(
                    new BrokerConfig(1, "127.0.0.1", 19093, tempDir.resolve("broker-1"),
                            true, "127.0.0.1", 9092, false),
                    controller,
                    partitionManager);
            try {
                fetcher.start();
                waitForHighWatermark(partitionManager, "orders", 0, 1L);

                assertEquals(new ReplicaFetchRequest("orders", 0, 1, 0L, 1024 * 1024), captured.get());
                assertEquals(1L, partitionManager.highWatermark("orders", 0));
            } finally {
                fetcher.close();
                partitionManager.close();
            }
        }
    }

    @Test
    void reusesLeaderConnectionAcrossFetchLoops() throws Exception {
        try (CountingReplicaLeader leader = CountingReplicaLeader.start()) {
            ClusterController controller = new ClusterController(new MetadataStore(tempDir.resolve("controller-pool")));
            controller.registerBroker(new RegisterBrokerRequest(0, "127.0.0.1", leader.port()));
            controller.registerBroker(new RegisterBrokerRequest(1, "127.0.0.1", 19093));
            assertEquals(ErrorCode.NONE, controller.createTopic(new CreateTopicRequest("orders", 2, 2)));
            controller.updateMetadata(metadata -> {
                metadata.topics.get("orders").partitions.forEach(partition -> {
                    partition.leaderBrokerId = 0;
                    partition.replicaBrokerIds = new ArrayList<>(List.of(0, 1));
                    partition.isrBrokerIds = new ArrayList<>(List.of(0, 1));
                });
                return null;
            });

            PartitionManager partitionManager = new PartitionManager(new LogManager(tempDir.resolve("follower-pool")));
            ReplicaFetcher fetcher = new ReplicaFetcher(
                    new BrokerConfig(1, "127.0.0.1", 19093, tempDir.resolve("broker-1-pool"),
                            true, "127.0.0.1", 9092, false),
                    controller,
                    partitionManager);
            try {
                fetcher.start();
                waitForHighWatermark(partitionManager, "orders", 0, 1L);
                waitForHighWatermark(partitionManager, "orders", 1, 1L);

                waitForAcceptedConnections(leader, 1);
                assertEquals(1, leader.acceptedConnections());
            } finally {
                fetcher.close();
                partitionManager.close();
            }
        }
    }

    private static void waitForHighWatermark(
            PartitionManager partitionManager,
            String topicName,
            int partitionId,
            long expectedHighWatermark) throws InterruptedException {
        long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(5);
        while (System.nanoTime() < deadline) {
            if (partitionManager.highWatermark(topicName, partitionId) >= expectedHighWatermark) {
                return;
            }
            Thread.sleep(25);
        }
        assertEquals(expectedHighWatermark, partitionManager.highWatermark(topicName, partitionId));
    }

    private static void waitForAcceptedConnections(
            CountingReplicaLeader leader,
            int expectedAcceptedConnections) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (System.nanoTime() < deadline) {
            if (leader.acceptedConnections() >= expectedAcceptedConnections) {
                return;
            }
            Thread.sleep(25);
        }
        assertEquals(expectedAcceptedConnections, leader.acceptedConnections());
    }

    private static final class CountingReplicaLeader implements AutoCloseable {
        private final EventLoopGroup boss;
        private final EventLoopGroup worker;
        private final Channel channel;
        private final AtomicInteger acceptedConnections;

        private CountingReplicaLeader(
                EventLoopGroup boss,
                EventLoopGroup worker,
                Channel channel,
                AtomicInteger acceptedConnections) {
            this.boss = boss;
            this.worker = worker;
            this.channel = channel;
            this.acceptedConnections = acceptedConnections;
        }

        static CountingReplicaLeader start() throws InterruptedException {
            EventLoopGroup boss = new NioEventLoopGroup(1);
            EventLoopGroup worker = new NioEventLoopGroup(1);
            AtomicInteger acceptedConnections = new AtomicInteger();
            Channel server = new ServerBootstrap()
                    .group(boss, worker)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            acceptedConnections.incrementAndGet();
                            ch.pipeline().addLast(new MqDecoder(), new SimpleChannelInboundHandler<ByteBuf>() {
                                @Override
                                protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) {
                                    MqDecoder.DecodedRequest decoded = MqDecoder.decodeRequest(msg);
                                    ReplicaFetchRequest request = (ReplicaFetchRequest) decoded.body();
                                    List<FetchedRecord> records = request.fetchOffset() == 0L
                                            ? List.of(new FetchedRecord(0L, "k".getBytes(), "v".getBytes()))
                                            : List.of();
                                    ctx.writeAndFlush(ProtocolSerdes.encodeResponse(
                                            new MqResponseHeader(decoded.header().correlationId(), ErrorCode.NONE),
                                            new ReplicaFetchResponse(
                                                    request.topicName(), request.partitionId(), 1L, records)));
                                }
                            }, new MqEncoder());
                        }
                    })
                    .bind("127.0.0.1", 0)
                    .sync()
                    .channel();
            return new CountingReplicaLeader(boss, worker, server, acceptedConnections);
        }

        int port() {
            return ((java.net.InetSocketAddress) channel.localAddress()).getPort();
        }

        int acceptedConnections() {
            return acceptedConnections.get();
        }

        @Override
        public void close() throws InterruptedException {
            channel.close().sync();
            boss.shutdownGracefully().sync();
            worker.shutdownGracefully().sync();
        }
    }
}
