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
import com.minimq.protocol.message.ProtocolMessages.RegisterBrokerRequest;
import com.minimq.protocol.message.ProtocolMessages.RegisterBrokerResponse;
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
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
        assertEquals(1, fake.heartbeatCalls);
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
        fake.nextRegistration = new RegisterBrokerResponse(2L, 5_000L);
        service.runOnce();
        assertEquals(BrokerAvailabilityGuard.State.ACTIVE, guard.state());
        assertEquals(2L, service.brokerEpoch());

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

    private BrokerConfig leaseConfig() {
        return BrokerConfig.parse(new String[] {
                "--broker-id", "1",
                "--host", "127.0.0.1",
                "--port", "19093",
                "--data-dir", tempDir.resolve("broker-1").toString(),
                "--controller-addr", "127.0.0.1:19092",
                "--broker-heartbeat-interval-ms", "100",
                "--broker-self-fence-timeout-ms", "300",
                "--controller-broker-lease-timeout-ms", "500",
                "--admin-token", "test-token"
        });
    }

    private static final class FakeControllerClient extends ControllerClient {
        private int connectCalls;
        private int registerCalls;
        private int heartbeatCalls;
        private boolean closed;
        private RuntimeException registrationFailure;
        private RuntimeException heartbeatFailure;
        private Runnable afterHeartbeat = () -> {
        };
        private RegisterBrokerResponse nextRegistration = new RegisterBrokerResponse(1L, 5_000L);

        @Override
        public void connect(String host, int port) {
            connectCalls++;
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
            heartbeatCalls++;
            afterHeartbeat.run();
            if (heartbeatFailure != null) {
                throw heartbeatFailure;
            }
        }

        @Override
        public synchronized MetadataResponse fetchMetadata() {
            return new MetadataResponse(
                    List.of(new BrokerMetadata(1, "127.0.0.1", 19093)),
                    List.of());
        }

        @Override
        public void close() {
            closed = true;
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
