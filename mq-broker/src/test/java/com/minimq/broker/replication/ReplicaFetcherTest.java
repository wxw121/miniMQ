package com.minimq.broker.replication;

import com.minimq.broker.BrokerConfig;
import com.minimq.broker.BrokerAvailabilityGuard;
import com.minimq.broker.PartitionManager;
import com.minimq.broker.controller.ClusterController;
import com.minimq.broker.metadata.MetadataStore;
import com.minimq.broker.support.BrokerTestHarness;
import com.minimq.common.error.ErrorCode;
import com.minimq.protocol.ProtocolSerdes;
import com.minimq.protocol.codec.MqDecoder;
import com.minimq.protocol.codec.MqEncoder;
import com.minimq.protocol.message.ProtocolMessages.CreateTopicRequest;
import com.minimq.protocol.message.MqResponseHeader;
import com.minimq.protocol.message.ProtocolMessages.RegisterBrokerRequest;
import com.minimq.protocol.message.ProtocolMessages.ReplicaFetchV2Request;
import com.minimq.protocol.message.ProtocolMessages.ReplicaFetchV2Response;
import com.minimq.protocol.message.ProtocolMessages.ScheduledReplicaRecord;
import com.minimq.protocol.message.ProtocolMessages.V4BoundedList;
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
import static org.junit.jupiter.api.Assertions.assertTrue;

class ReplicaFetcherTest {

    @TempDir
    Path tempDir;

    @Test
    void followerFetchesLeaderRecordsIntoLocalPartitionLog() throws Exception {
        AtomicReference<ReplicaFetchV2Request> captured = new AtomicReference<>();
        try (CountingReplicaLeader leader = CountingReplicaLeader.start(captured)) {
            ClusterController controller = new ClusterController(new MetadataStore(tempDir.resolve("controller")));
            controller.registerBroker(new RegisterBrokerRequest(0, "127.0.0.1", leader.port()));
            controller.registerBroker(new RegisterBrokerRequest(1, "127.0.0.1", 19093));
            assertEquals(ErrorCode.NONE, controller.createTopic(new CreateTopicRequest("orders", 1, 2)));

            PartitionManager partitionManager = new PartitionManager(new LogManager(tempDir.resolve("follower")));
            ReplicaFetcher fetcher = new ReplicaFetcher(
                    new BrokerConfig(1, "127.0.0.1", 19093, tempDir.resolve("broker-1"),
                            true, "127.0.0.1", 9092, false, BrokerTestHarness.ADMIN_TOKEN),
                    controller,
                    partitionManager);
            try {
                fetcher.start();
                waitForHighWatermark(partitionManager, "orders", 0, 1L);
                waitForReplicaState(controller, "orders", 0, 1, 1L);

                assertEquals(new ReplicaFetchV2Request(
                        BrokerTestHarness.ADMIN_TOKEN,
                        "orders",
                        0,
                        1,
                        1L,
                        0L,
                        0L,
                        0L,
                        1024 * 1024), captured.get());
                assertEquals(1L, partitionManager.highWatermark("orders", 0));
                var progress = controller.snapshot().topics.get("orders").partitions.get(0)
                        .replicaProgress.get(1);
                assertEquals(1L, progress.physicalLogEndOffset);
                assertEquals(1L, progress.visibleLogEndOffset);
                assertEquals(-1L, progress.nextVisibleAtEpochMs);
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
                            true, "127.0.0.1", 9092, false, BrokerTestHarness.ADMIN_TOKEN),
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

    @Test
    void periodicallyReportsLocalReplicaStateWithoutNewRecords() throws Exception {
        try (CountingReplicaLeader leader = CountingReplicaLeader.start()) {
            ClusterController controller =
                    new ClusterController(new MetadataStore(tempDir.resolve("periodic-controller")));
            controller.registerBroker(new RegisterBrokerRequest(0, "127.0.0.1", leader.port()));
            controller.registerBroker(new RegisterBrokerRequest(1, "127.0.0.1", 19093));
            assertEquals(ErrorCode.NONE, controller.createTopic(new CreateTopicRequest("orders", 1, 2)));

            PartitionManager partitionManager =
                    new PartitionManager(new LogManager(tempDir.resolve("periodic-follower")));
            partitionManager.replicateAt(
                    "orders", 0, 0L, "k".getBytes(), "v".getBytes());
            ReplicaFetcher fetcher = new ReplicaFetcher(
                    new BrokerConfig(
                            1,
                            "127.0.0.1",
                            19093,
                            tempDir.resolve("periodic-broker"),
                            true,
                            "127.0.0.1",
                            9092,
                            false,
                            BrokerTestHarness.ADMIN_TOKEN),
                    controller,
                    partitionManager);
            try {
                fetcher.start();
                waitForReplicaState(controller, "orders", 0, 1, 1L);
            } finally {
                fetcher.close();
                partitionManager.close();
            }
        }
    }

    @Test
    void waitsForActiveGuardBeforeConnectingToLeader() throws Exception {
        try (CountingReplicaLeader leader = CountingReplicaLeader.start()) {
            ClusterController controller = new ClusterController(new MetadataStore(tempDir.resolve("guard-controller")));
            controller.registerBroker(new RegisterBrokerRequest(0, "127.0.0.1", leader.port()));
            controller.registerBroker(new RegisterBrokerRequest(1, "127.0.0.1", 19093));
            controller.createTopic(new CreateTopicRequest("orders", 1, 2));
            PartitionManager partitionManager = new PartitionManager(new LogManager(tempDir.resolve("guard-follower")));
            BrokerAvailabilityGuard guard = new BrokerAvailabilityGuard();
            guard.fence();
            ReplicaFetcher fetcher = new ReplicaFetcher(
                    new BrokerConfig(1, "127.0.0.1", 19093, tempDir.resolve("guard-broker"),
                            true, "127.0.0.1", 9092, false, BrokerTestHarness.ADMIN_TOKEN),
                    controller,
                    partitionManager,
                    guard);
            try {
                fetcher.start();
                Thread.sleep(200);
                assertEquals(0, leader.acceptedConnections());

                guard.activate();
                waitForAcceptedConnections(leader, 1);
            } finally {
                fetcher.close();
                partitionManager.close();
            }
        }
    }

    @Test
    void reportsReplicaStateFromOneVisibilitySnapshot() throws Exception {
        ClusterController controller =
                new ClusterController(new MetadataStore(tempDir.resolve("snapshot-controller")));
        controller.registerBroker(new RegisterBrokerRequest(0, "127.0.0.1", 19092));
        assertEquals(ErrorCode.NONE, controller.createTopic(new CreateTopicRequest("orders", 1)));
        BrokerConfig config = new BrokerConfig(
                0,
                "127.0.0.1",
                19092,
                tempDir.resolve("snapshot-broker"),
                true,
                "127.0.0.1",
                19092,
                false,
                BrokerTestHarness.ADMIN_TOKEN);
        SnapshotOnlyPartitionManager partitionManager = new SnapshotOnlyPartitionManager(
                new LogManager(tempDir.resolve("snapshot-log")));
        ReplicaFetcher fetcher = new ReplicaFetcher(config, controller, partitionManager);
        try {
            fetcher.start();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (System.nanoTime() < deadline) {
                var progress = controller.snapshot().topics.get("orders").partitions.get(0)
                        .replicaProgress.get(0);
                if (progress != null && progress.physicalLogEndOffset == 3L) {
                    assertEquals(1L, progress.visibleLogEndOffset);
                    assertEquals(5_000L, progress.nextVisibleAtEpochMs);
                    assertTrue(partitionManager.snapshotCalls.get() >= 1);
                    return;
                }
                Thread.sleep(25L);
            }
            assertEquals(3L, controller.replicaLogEndOffset("orders", 0, 0));
        } finally {
            fetcher.close();
            partitionManager.close();
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

    private static void waitForReplicaState(
            ClusterController controller,
            String topicName,
            int partitionId,
            int brokerId,
            long expectedLeo) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (System.nanoTime() < deadline) {
            var progress = controller.snapshot().topics.get(topicName).partitions.get(partitionId)
                    .replicaProgress.get(brokerId);
            if (progress != null
                    && progress.physicalLogEndOffset == expectedLeo
                    && progress.visibleLogEndOffset == expectedLeo) {
                return;
            }
            Thread.sleep(25);
        }
        var progress = controller.snapshot().topics.get(topicName).partitions.get(partitionId)
                .replicaProgress.get(brokerId);
        assertEquals(expectedLeo, progress == null ? -1L : progress.physicalLogEndOffset);
    }

    private static final class CountingReplicaLeader implements AutoCloseable {
        private final EventLoopGroup boss;
        private final EventLoopGroup worker;
        private final Channel channel;
        private final AtomicInteger acceptedConnections;
        private final AtomicReference<ReplicaFetchV2Request> captured;

        private CountingReplicaLeader(
                EventLoopGroup boss,
                EventLoopGroup worker,
                Channel channel,
                AtomicInteger acceptedConnections,
                AtomicReference<ReplicaFetchV2Request> captured) {
            this.boss = boss;
            this.worker = worker;
            this.channel = channel;
            this.acceptedConnections = acceptedConnections;
            this.captured = captured;
        }

        static CountingReplicaLeader start() throws InterruptedException {
            return start(new AtomicReference<>());
        }

        static CountingReplicaLeader start(AtomicReference<ReplicaFetchV2Request> captured)
                throws InterruptedException {
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
                                    ReplicaFetchV2Request request = (ReplicaFetchV2Request) decoded.body();
                                    captured.set(request);
                                    List<ScheduledReplicaRecord> records = request.fetchOffset() == 0L
                                            ? List.of(new ScheduledReplicaRecord(
                                                    0L, "k".getBytes(), "v".getBytes(), 0L))
                                            : List.of();
                                    ctx.writeAndFlush(ProtocolSerdes.encodeResponse(
                                            new MqResponseHeader(decoded.header().correlationId(), ErrorCode.NONE),
                                            new ReplicaFetchV2Response(
                                                    request.topicName(),
                                                    request.partitionId(),
                                                    1L,
                                                    1L,
                                                    -1L,
                                                    request.leaderEpoch(),
                                                    1L,
                                                    new V4BoundedList<>(records.size(), false, records))));
                                }
                            }, new MqEncoder());
                        }
                    })
                    .bind("127.0.0.1", 0)
                    .sync()
                    .channel();
            return new CountingReplicaLeader(boss, worker, server, acceptedConnections, captured);
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

    private static final class SnapshotOnlyPartitionManager extends PartitionManager {
        private final AtomicInteger snapshotCalls = new AtomicInteger();

        private SnapshotOnlyPartitionManager(LogManager logManager) {
            super(logManager);
        }

        @Override
        public LogManager.VisibilitySnapshot visibilitySnapshot(String topic, int partitionId) {
            snapshotCalls.incrementAndGet();
            return new LogManager.VisibilitySnapshot(3L, 1L, 5_000L);
        }

        @Override
        public long highWatermark(String topic, int partitionId) {
            throw new AssertionError("replica reporting must use visibilitySnapshot");
        }

        @Override
        public long visibleHighWatermark(String topic, int partitionId) {
            throw new AssertionError("replica reporting must use visibilitySnapshot");
        }

        @Override
        public long nextVisibleAtEpochMs(String topic, int partitionId) {
            throw new AssertionError("replica reporting must use visibilitySnapshot");
        }
    }
}
