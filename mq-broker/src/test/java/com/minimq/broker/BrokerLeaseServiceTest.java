package com.minimq.broker;

import com.minimq.broker.controller.ClusterController;
import com.minimq.broker.metadata.MetadataStore;
import com.minimq.common.error.ErrorCode;
import com.minimq.protocol.ProtocolSerdes;
import com.minimq.protocol.codec.MqDecoder;
import com.minimq.protocol.codec.MqEncoder;
import com.minimq.protocol.message.MqResponseHeader;
import com.minimq.protocol.message.ProtocolMessages.BrokerMetadata;
import com.minimq.protocol.message.ProtocolMessages.BrokerHeartbeatRequest;
import com.minimq.protocol.message.ProtocolMessages.MetadataResponse;
import com.minimq.protocol.message.ProtocolMessages.MetadataV2Request;
import com.minimq.protocol.message.ProtocolMessages.MetadataV2Response;
import com.minimq.protocol.message.ProtocolMessages.OpsBrokerInfo;
import com.minimq.protocol.message.ProtocolMessages.RegisterBrokerRequest;
import com.minimq.protocol.message.ProtocolMessages.RegisterBrokerResponse;
import com.minimq.protocol.message.ProtocolMessages.TopicTombstoneInfo;
import com.minimq.protocol.message.ProtocolMessages.TopicTombstoneSummary;
import com.minimq.protocol.message.ProtocolMessages.TopicMetadata;
import com.minimq.protocol.message.ProtocolMessages.V4BoundedList;
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

import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BrokerLeaseServiceTest {

    @TempDir
    Path tempDir;

    @Test
    void controllerClientExposesTypedLeaseFailure() throws Exception {
        try (TestController controller = TestController.start(ErrorCode.BROKER_FENCED);
             ControllerClient client = new ControllerClient()) {
            client.connect("127.0.0.1", controller.port());

            ControllerClient.ControllerRequestException failure = assertThrows(
                    ControllerClient.ControllerRequestException.class,
                    () -> client.heartbeatBroker(new BrokerHeartbeatRequest(1, 2L)));

            assertEquals(ErrorCode.BROKER_FENCED, failure.errorCode());
        }
    }

    @Test
    void renewsFencesRecoversAndClosesResources() throws Exception {
        AtomicLong now = new AtomicLong();
        FakeControllerClient fake = new FakeControllerClient();
        ScheduledThreadPoolExecutor scheduler = new ScheduledThreadPoolExecutor(1);
        BrokerAvailabilityGuard guard = new BrokerAvailabilityGuard();
        BrokerLeaseService service = new BrokerLeaseService(
                leaseConfig(),
                new ClusterController(new MetadataStore(tempDir)),
                guard,
                () -> fake,
                now::get,
                scheduler);

        service.initialize();
        assertEquals(BrokerAvailabilityGuard.State.ACTIVE, guard.state());
        assertEquals(1, fake.registerCalls);
        assertEquals(1, fake.connectCalls);
        assertEquals(1L, service.brokerEpoch());

        now.set(TimeUnit.MILLISECONDS.toNanos(100));
        service.runOnce();
        assertEquals(2, fake.heartbeatCalls);
        assertEquals(200L, fake.heartbeatTimeoutMs);
        assertEquals(BrokerAvailabilityGuard.State.ACTIVE, guard.state());
        assertEquals(1, fake.connectCalls);

        fake.heartbeatFailure = new IllegalStateException("controller unavailable");
        now.set(TimeUnit.MILLISECONDS.toNanos(3_101));
        service.runOnce();
        assertEquals(BrokerAvailabilityGuard.State.FENCED, guard.state());

        fake.registrationFailure =
                new ControllerClient.ControllerRequestException(ErrorCode.BROKER_ID_IN_USE);
        service.runOnce();
        assertEquals(BrokerAvailabilityGuard.State.FENCED, guard.state());

        fake.registrationFailure = null;
        fake.heartbeatFailure = null;
        fake.nextRegistration = new RegisterBrokerResponse(2L, 5_000L);
        service.runOnce();
        assertEquals(BrokerAvailabilityGuard.State.ACTIVE, guard.state());
        assertEquals(2L, service.brokerEpoch());
        assertEquals(3, fake.heartbeatCalls);

        service.close();
        assertEquals(BrokerAvailabilityGuard.State.FENCED, guard.state());
        assertTrue(scheduler.isShutdown());
        assertTrue(fake.closed);

        service.runOnce();
        assertEquals(BrokerAvailabilityGuard.State.FENCED, guard.state());
    }

    @Test
    void successfulSlowHeartbeatStillFencesFromRequestStart() throws Exception {
        AtomicLong now = new AtomicLong();
        FakeControllerClient fake = new FakeControllerClient();
        BrokerAvailabilityGuard guard = new BrokerAvailabilityGuard();
        BrokerLeaseService service = new BrokerLeaseService(
                leaseConfig(),
                new ClusterController(new MetadataStore(tempDir)),
                guard,
                () -> fake,
                now::get,
                new ScheduledThreadPoolExecutor(1));
        service.initialize();
        now.set(TimeUnit.MILLISECONDS.toNanos(100));
        fake.afterHeartbeat = () -> now.set(TimeUnit.MILLISECONDS.toNanos(400));

        service.runOnce();

        assertEquals(BrokerAvailabilityGuard.State.FENCED, guard.state());
        service.close();
    }

    @Test
    void confirmsLeaseAfterSlowMetadataSyncBeforeActivation() throws Exception {
        AtomicLong now = new AtomicLong();
        FakeControllerClient fake = new FakeControllerClient();
        fake.afterFetchMetadata = () -> now.set(TimeUnit.MILLISECONDS.toNanos(400));
        BrokerAvailabilityGuard guard = new BrokerAvailabilityGuard();
        BrokerLeaseService service = new BrokerLeaseService(
                leaseConfig(),
                new ClusterController(new MetadataStore(tempDir)),
                guard,
                () -> fake,
                now::get,
                new ScheduledThreadPoolExecutor(1));

        service.initialize();

        assertEquals(BrokerAvailabilityGuard.State.ACTIVE, guard.state());
        assertEquals(1, fake.registerCalls);
        assertEquals(1, fake.heartbeatCalls);
        assertEquals(300L, fake.heartbeatTimeoutMs);
        service.close();
    }

    @Test
    void legacyInitializationInstallsV2TombstonesBeforeActivation() throws Exception {
        AtomicLong now = new AtomicLong();
        FakeControllerClient fake = new FakeControllerClient();
        fake.nextMetadata = new MetadataResponse(
                List.of(new BrokerMetadata(1, "127.0.0.1", 19093)),
                List.of(),
                null,
                9L);
        TopicTombstoneSummary tombstone = new TopicTombstoneSummary(
                "orders",
                new TopicTombstoneInfo(
                        "DELETING",
                        8L,
                        100L,
                        new V4BoundedList<>(1, false, List.of(1)),
                        new V4BoundedList<>(0, false, List.of()),
                        new V4BoundedList<>(1, false, List.of(1))));
        fake.nextMetadataV2 = new MetadataV2Response(
                new V4BoundedList<>(1, false,
                        List.of(new OpsBrokerInfo(1, "127.0.0.1", 19093, "ACTIVE", 4L, 0L))),
                new V4BoundedList<>(0, false, List.of()),
                new V4BoundedList<>(1, false, List.of(tombstone)),
                null,
                9L,
                4);
        ClusterController controller = new ClusterController(new MetadataStore(tempDir));
        BrokerAvailabilityGuard guard = new BrokerAvailabilityGuard();
        BrokerLeaseService service = new BrokerLeaseService(
                leaseConfig(),
                controller,
                guard,
                () -> fake,
                now::get,
                new ScheduledThreadPoolExecutor(1));

        service.initialize();

        assertEquals(1, fake.metadataFetches);
        assertEquals(1, fake.metadataV2Fetches);
        assertEquals("DELETING", controller.snapshot().topicTombstones.get("orders").state);
        assertEquals(BrokerAvailabilityGuard.State.ACTIVE, guard.state());
        service.close();
    }

    @Test
    void startupBarrierAndStorageCleanupFenceControlActivation() throws Exception {
        AtomicLong now = new AtomicLong();
        FakeControllerClient fake = new FakeControllerClient();
        BrokerAvailabilityGuard guard = new BrokerAvailabilityGuard();
        BrokerLeaseService service = new BrokerLeaseService(
                leaseConfig(),
                new ClusterController(new MetadataStore(tempDir)),
                guard,
                () -> fake,
                now::get,
                new ScheduledThreadPoolExecutor(1));
        service.holdActivation();

        service.initialize();
        assertEquals(BrokerAvailabilityGuard.State.STARTING, guard.state());

        service.activateAfterStartupBarrier();
        assertEquals(BrokerAvailabilityGuard.State.ACTIVE, guard.state());

        guard.fenceStorageCleanup();
        now.set(TimeUnit.MILLISECONDS.toNanos(100));
        service.runOnce();

        assertEquals(BrokerAvailabilityGuard.State.FENCED, guard.state());
        assertTrue(guard.hasStickyStorageFence());
        service.close();
    }

    @Test
    void controllerFenceDuringStartupBarrierPreventsActivation() throws Exception {
        AtomicLong now = new AtomicLong();
        FakeControllerClient fake = new FakeControllerClient();
        BrokerAvailabilityGuard guard = new BrokerAvailabilityGuard();
        BrokerLeaseService service = new BrokerLeaseService(
                leaseConfig(),
                new ClusterController(new MetadataStore(tempDir)),
                guard,
                () -> fake,
                now::get,
                new ScheduledThreadPoolExecutor(1));
        service.holdActivation();
        service.initialize();

        fake.heartbeatFailure =
                new ControllerClient.ControllerRequestException(ErrorCode.BROKER_FENCED);
        now.set(TimeUnit.MILLISECONDS.toNanos(100));
        service.runOnce();

        assertEquals(BrokerAvailabilityGuard.State.FENCED, guard.state());
        assertThrows(IllegalStateException.class, service::activateAfterStartupBarrier);
        service.close();
    }

    @Test
    void failedInitializationClosesPoisonedClientBeforeRetry() throws Exception {
        AtomicLong now = new AtomicLong();
        AtomicInteger clientsCreated = new AtomicInteger();
        FakeControllerClient poisoned = new FakeControllerClient();
        poisoned.connectFailure = new IllegalStateException("controller unavailable");
        FakeControllerClient replacement = new FakeControllerClient();
        BrokerAvailabilityGuard guard = new BrokerAvailabilityGuard();
        BrokerLeaseService service = new BrokerLeaseService(
                leaseConfig(),
                new ClusterController(new MetadataStore(tempDir)),
                guard,
                () -> clientsCreated.getAndIncrement() == 0 ? poisoned : replacement,
                now::get,
                new ScheduledThreadPoolExecutor(1));

        try {
            assertThrows(IllegalStateException.class, service::initialize);
            assertTrue(poisoned.closed);

            service.initialize();

            assertEquals(2, clientsCreated.get());
            assertEquals(BrokerAvailabilityGuard.State.ACTIVE, guard.state());
            assertEquals(1, replacement.registerCalls);
        } finally {
            service.close();
        }
    }

    @Test
    void failedInitializationPreservesFailureWhenClientCloseAlsoFails() throws Exception {
        AtomicLong now = new AtomicLong();
        FakeControllerClient poisoned = new FakeControllerClient();
        RuntimeException connectFailure = new IllegalStateException("controller unavailable");
        RuntimeException closeFailure = new IllegalStateException("controller close failed");
        poisoned.connectFailure = connectFailure;
        poisoned.closeFailure = closeFailure;
        BrokerLeaseService service = new BrokerLeaseService(
                leaseConfig(),
                new ClusterController(new MetadataStore(tempDir)),
                new BrokerAvailabilityGuard(),
                () -> poisoned,
                now::get,
                new ScheduledThreadPoolExecutor(1));

        try {
            RuntimeException thrown = assertThrows(RuntimeException.class, service::initialize);

            assertSame(connectFailure, thrown);
            assertEquals(List.of(closeFailure), List.of(connectFailure.getSuppressed()));
            assertTrue(poisoned.closed);
        } finally {
            poisoned.closeFailure = null;
            service.close();
        }
    }

    @Test
    void reconnectAndHeartbeatShareRemainingSelfFenceBudget() throws Exception {
        AtomicLong now = new AtomicLong();
        FakeControllerClient fake = new FakeControllerClient();
        BrokerAvailabilityGuard guard = new BrokerAvailabilityGuard();
        BrokerLeaseService service = new BrokerLeaseService(
                leaseConfig(),
                new ClusterController(new MetadataStore(tempDir)),
                guard,
                () -> fake,
                now::get,
                new ScheduledThreadPoolExecutor(1));
        service.initialize();

        now.set(TimeUnit.MILLISECONDS.toNanos(100));
        fake.heartbeatFailure = new IllegalStateException("controller unavailable");
        service.runOnce();
        assertEquals(BrokerAvailabilityGuard.State.ACTIVE, guard.state());

        fake.heartbeatFailure = null;
        fake.afterConnect = () -> now.set(TimeUnit.MILLISECONDS.toNanos(290));
        service.runOnce();

        assertEquals(10L, fake.heartbeatTimeoutMs);
        service.close();
    }

    @Test
    void failedHeartbeatBoundsClientCloseBySelfFenceDeadline() throws Exception {
        AtomicLong now = new AtomicLong();
        FakeControllerClient fake = new FakeControllerClient();
        BrokerAvailabilityGuard guard = new BrokerAvailabilityGuard();
        BrokerLeaseService service = new BrokerLeaseService(
                leaseConfig(),
                new ClusterController(new MetadataStore(tempDir)),
                guard,
                () -> fake,
                now::get,
                new ScheduledThreadPoolExecutor(1));
        service.initialize();

        now.set(TimeUnit.MILLISECONDS.toNanos(100));
        fake.heartbeatFailure = new IllegalStateException("controller unavailable");
        fake.afterClose = () -> now.set(TimeUnit.MILLISECONDS.toNanos(300));
        service.runOnce();

        assertEquals(200L, fake.closeTimeoutMs);
        assertEquals(BrokerAvailabilityGuard.State.FENCED, guard.state());
        service.close();
    }

    private BrokerConfig leaseConfig() {
        return BrokerConfig.parse(new String[] {
                "--broker-id", "1",
                "--host", "127.0.0.1",
                "--port", "19093",
                "--data-dir", tempDir.resolve("broker-1").toString(),
                "--broker-heartbeat-interval-ms", "100",
                "--broker-self-fence-timeout-ms", "300",
                "--controller-broker-lease-timeout-ms", "500",
                "--admin-token", "test-token"
        }).withLegacyController(false, "127.0.0.1", 19092);
    }

    private static final class FakeControllerClient extends ControllerClient {
        private int connectCalls;
        private int registerCalls;
        private int heartbeatCalls;
        private long heartbeatTimeoutMs;
        private long closeTimeoutMs = -1L;
        private boolean closed;
        private int metadataFetches;
        private int metadataV2Fetches;
        private MetadataResponse nextMetadata = new MetadataResponse(List.of(), List.of());
        private MetadataV2Response nextMetadataV2 = new MetadataV2Response(
                new V4BoundedList<>(0, false, List.of()),
                new V4BoundedList<>(0, false, List.of()),
                new V4BoundedList<>(0, false, List.of()),
                null,
                0L,
                4);
        private RuntimeException connectFailure;
        private RuntimeException closeFailure;
        private RuntimeException registrationFailure;
        private RuntimeException heartbeatFailure;
        private Runnable afterConnect = () -> {
        };
        private Runnable afterHeartbeat = () -> {
        };
        private Runnable afterFetchMetadata = () -> {
        };
        private Runnable afterClose = () -> {
        };
        private RegisterBrokerResponse nextRegistration = new RegisterBrokerResponse(1L, 5_000L);

        @Override
        public void connect(String host, int port) {
            connectCalls++;
            afterConnect.run();
            if (connectFailure != null) {
                throw connectFailure;
            }
        }

        @Override
        public void connect(String host, int port, long timeoutMs) {
            connectCalls++;
            afterConnect.run();
            if (connectFailure != null) {
                throw connectFailure;
            }
        }

        @Override
        public synchronized RegisterBrokerResponse registerBroker(RegisterBrokerRequest request) {
            registerCalls++;
            if (registrationFailure != null) {
                throw registrationFailure;
            }
            return nextRegistration;
        }

        @Override
        public synchronized void heartbeatBroker(BrokerHeartbeatRequest request) {
            heartbeatBroker(request, TimeUnit.SECONDS.toMillis(5));
        }

        @Override
        public synchronized void heartbeatBroker(BrokerHeartbeatRequest request, long responseTimeoutMs) {
            heartbeatCalls++;
            heartbeatTimeoutMs = responseTimeoutMs;
            afterHeartbeat.run();
            if (heartbeatFailure != null) {
                throw heartbeatFailure;
            }
        }

        @Override
        public synchronized MetadataResponse fetchMetadata() {
            metadataFetches++;
            afterFetchMetadata.run();
            return nextMetadata;
        }

        @Override
        public synchronized MetadataResponse fetchMetadata(long responseTimeoutMs) {
            return fetchMetadata();
        }

        @Override
        public synchronized MetadataResponse fetchMetadata(String topicName, long responseTimeoutMs) {
            MetadataResponse metadata = fetchMetadata();
            if (topicName == null || topicName.isEmpty()) {
                return metadata;
            }
            List<TopicMetadata> topics = metadata.topics().stream()
                    .filter(topic -> topic.topicName().equals(topicName))
                    .toList();
            return new MetadataResponse(metadata.brokers(), topics, metadata.controller(), metadata.metadataVersion());
        }

        @Override
        public synchronized MetadataV2Response fetchMetadataV2() {
            metadataV2Fetches++;
            return nextMetadataV2;
        }

        @Override
        public synchronized MetadataV2Response fetchMetadataV2(long responseTimeoutMs) {
            return fetchMetadataV2();
        }

        @Override
        public synchronized MetadataV2Response fetchMetadataV2(
                String topicStartAfter,
                String tombstoneStartAfter,
                long responseTimeoutMs) {
            return fetchMetadataV2();
        }

        @Override
        public synchronized MetadataV2Response fetchMetadataV2(MetadataV2Request request, long responseTimeoutMs) {
            return fetchMetadataV2();
        }

        @Override
        public void close() {
            closed = true;
        }

        @Override
        public void close(long timeoutMs) {
            closeTimeoutMs = timeoutMs;
            afterClose.run();
            closed = true;
            if (closeFailure != null) {
                throw closeFailure;
            }
        }
    }

    private static final class TestController implements AutoCloseable {
        private final EventLoopGroup boss;
        private final EventLoopGroup worker;
        private final Channel server;

        private TestController(EventLoopGroup boss, EventLoopGroup worker, Channel server) {
            this.boss = boss;
            this.worker = worker;
            this.server = server;
        }

        static TestController start(ErrorCode errorCode) throws InterruptedException {
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
                                    MqDecoder.DecodedRequest request = MqDecoder.decodeRequest(msg);
                                    ctx.writeAndFlush(ProtocolSerdes.encodeResponse(
                                            new MqResponseHeader(request.header().correlationId(), errorCode),
                                            null));
                                }
                            }, new MqEncoder());
                        }
                    })
                    .bind("127.0.0.1", 0)
                    .sync()
                    .channel();
            return new TestController(boss, worker, server);
        }

        int port() {
            return ((InetSocketAddress) server.localAddress()).getPort();
        }

        @Override
        public void close() throws InterruptedException {
            server.close().sync();
            boss.shutdownGracefully().sync();
            worker.shutdownGracefully().sync();
        }
    }
}
