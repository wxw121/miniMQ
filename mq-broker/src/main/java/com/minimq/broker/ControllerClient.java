package com.minimq.broker;

import com.minimq.common.error.ErrorCode;
import com.minimq.protocol.ApiKeys;
import com.minimq.protocol.ProtocolSerdes;
import com.minimq.protocol.codec.MqDecoder;
import com.minimq.protocol.message.MqResponseHeader;
import com.minimq.protocol.codec.MqEncoder;
import com.minimq.protocol.message.MqRequestHeader;
import com.minimq.protocol.message.ProtocolMessages.MetadataRequest;
import com.minimq.protocol.message.ProtocolMessages.MetadataResponse;
import com.minimq.protocol.message.ProtocolMessages.MetadataV2Request;
import com.minimq.protocol.message.ProtocolMessages.MetadataV2Response;
import com.minimq.protocol.message.ProtocolMessages.BrokerHeartbeatRequest;
import com.minimq.protocol.message.ProtocolMessages.RegisterBrokerRequest;
import com.minimq.protocol.message.ProtocolMessages.RegisterBrokerResponse;
import com.minimq.protocol.message.ProtocolMessages.UpdateReplicaLeoRequest;
import com.minimq.protocol.message.ProtocolMessages.UpdateReplicaStateV2Request;
import com.minimq.protocol.message.ProtocolMessages.TopicDeleteAckRequest;
import com.minimq.protocol.message.ProtocolMessages.DiscoverControllerRequest;
import com.minimq.protocol.message.ProtocolMessages.DiscoverControllerResponse;
import com.minimq.protocol.message.ProtocolMessages.LeaderReadyRequest;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Broker 侧 Controller 客户端，用于向 Controller 注册自身并同步最新集群元数据。
 */
public class ControllerClient implements AutoCloseable {
    private static final AtomicInteger CORR = new AtomicInteger(1);
    private final EventLoopGroup group = new NioEventLoopGroup(1);
    private Channel channel;
    private final Map<Integer, PendingResponseSlot> pending = new ConcurrentHashMap<>();

    public void connect(String host, int port) throws InterruptedException {
        connect(host, port, TimeUnit.SECONDS.toMillis(5));
    }

    public void connect(String host, int port, long timeoutMs) throws InterruptedException {
        if (timeoutMs <= 0) {
            throw new IllegalArgumentException("Controller connect timeout must be positive");
        }
        Bootstrap bootstrap = new Bootstrap()
                .group(group)
                .channel(NioSocketChannel.class)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline().addLast(new MqDecoder(), new SimpleChannelInboundHandler<ByteBuf>() {
                            @Override
                            protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) {
                                int correlationId = msg.getInt(msg.readerIndex());
                                PendingResponseSlot slot = pending.get(correlationId);
                                if (slot != null) {
                                    slot.offerRetained(msg);
                                }
                            }
                        }, new MqEncoder());
                    }
                });
        ChannelFuture connectFuture = bootstrap.connect(host, port);
        boolean completed;
        try {
            completed = connectFuture.await(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException interrupted) {
            abortConnect(connectFuture);
            throw interrupted;
        }
        if (!completed) {
            abortConnect(connectFuture);
            throw new IllegalStateException("Timeout connecting to Controller");
        }
        if (!connectFuture.isSuccess()) {
            Throwable cause = connectFuture.cause();
            abortConnect(connectFuture);
            throw new IllegalStateException("Failed to connect to Controller", cause);
        }
        channel = connectFuture.channel();
    }

    private static void abortConnect(ChannelFuture connectFuture) {
        connectFuture.cancel(false);
        if (connectFuture.channel() != null) {
            connectFuture.channel().close();
        }
    }

    public synchronized RegisterBrokerResponse registerBroker(RegisterBrokerRequest request)
            throws InterruptedException {
        int correlationId = send(ApiKeys.REGISTER_BROKER, request);
        MqDecoder.DecodedResponse response = awaitResponse(correlationId, ApiKeys.REGISTER_BROKER);
        ensureSuccess(response);
        return (RegisterBrokerResponse) response.body();
    }

    public synchronized void heartbeatBroker(BrokerHeartbeatRequest request) throws InterruptedException {
        heartbeatBroker(request, TimeUnit.SECONDS.toMillis(5));
    }

    public synchronized void heartbeatBroker(BrokerHeartbeatRequest request, long responseTimeoutMs)
            throws InterruptedException {
        long timeoutMs = requirePositiveTimeout(responseTimeoutMs);
        int correlationId = send(ApiKeys.BROKER_HEARTBEAT, request);
        ensureSuccess(awaitResponse(
                correlationId,
                ApiKeys.BROKER_HEARTBEAT,
                timeoutMs,
                TimeUnit.MILLISECONDS));
    }

    public synchronized MetadataResponse fetchMetadata() throws InterruptedException {
        return fetchMetadata(TimeUnit.SECONDS.toMillis(5));
    }

    public synchronized MetadataResponse fetchMetadata(long responseTimeoutMs) throws InterruptedException {
        return fetchMetadata("", responseTimeoutMs);
    }

    public synchronized MetadataResponse fetchMetadata(String topicName, long responseTimeoutMs)
            throws InterruptedException {
        long timeoutMs = requirePositiveTimeout(responseTimeoutMs);
        int correlationId = send(ApiKeys.METADATA, new MetadataRequest(topicName == null ? "" : topicName));
        MqDecoder.DecodedResponse decoded = awaitResponse(
                correlationId,
                ApiKeys.METADATA,
                timeoutMs,
                TimeUnit.MILLISECONDS);
        ensureSuccess(decoded);
        return (MetadataResponse) decoded.body();
    }

    public synchronized MetadataV2Response fetchMetadataV2() throws InterruptedException {
        return fetchMetadataV2(TimeUnit.SECONDS.toMillis(5));
    }

    public synchronized MetadataV2Response fetchMetadataV2(long responseTimeoutMs)
            throws InterruptedException {
        return fetchMetadataV2(new MetadataV2Request(null, 10_000, 1), responseTimeoutMs);
    }

    public synchronized MetadataV2Response fetchMetadataV2(
            String topicStartAfter,
            String tombstoneStartAfter,
            long responseTimeoutMs) throws InterruptedException {
        return fetchMetadataV2(
                new MetadataV2Request(null, topicStartAfter, tombstoneStartAfter, 10_000, 10_000),
                responseTimeoutMs);
    }

    public synchronized MetadataV2Response fetchMetadataV2(
            MetadataV2Request request, long responseTimeoutMs) throws InterruptedException {
        long timeoutMs = requirePositiveTimeout(responseTimeoutMs);
        int correlationId = send(ApiKeys.METADATA_V2, request);
        MqDecoder.DecodedResponse decoded = awaitResponse(
                correlationId,
                ApiKeys.METADATA_V2,
                timeoutMs,
                TimeUnit.MILLISECONDS);
        ensureSuccess(decoded);
        return (MetadataV2Response) decoded.body();
    }

    public synchronized void updateReplicaLeo(UpdateReplicaLeoRequest request) throws InterruptedException {
        int correlationId = send(ApiKeys.UPDATE_REPLICA_LEO, request);
        ensureSuccess(awaitResponse(correlationId, ApiKeys.UPDATE_REPLICA_LEO));
    }

    public synchronized void updateReplicaStateV2(UpdateReplicaStateV2Request request)
            throws InterruptedException {
        int correlationId = send(ApiKeys.UPDATE_REPLICA_STATE_V2, request);
        ensureSuccess(awaitResponse(correlationId, ApiKeys.UPDATE_REPLICA_STATE_V2));
    }

    public synchronized void acknowledgeTopicDeletion(TopicDeleteAckRequest request)
            throws InterruptedException {
        acknowledgeTopicDeletion(request, TimeUnit.SECONDS.toMillis(5));
    }

    public synchronized void acknowledgeTopicDeletion(
            TopicDeleteAckRequest request, long responseTimeoutMs) throws InterruptedException {
        long timeoutMs = requirePositiveTimeout(responseTimeoutMs);
        int correlationId = send(ApiKeys.TOPIC_DELETE_ACK, request);
        ensureSuccess(awaitResponse(
                correlationId,
                ApiKeys.TOPIC_DELETE_ACK,
                timeoutMs,
                TimeUnit.MILLISECONDS));
    }

    private int send(short apiKey, Object body) {
        int correlationId = CORR.getAndIncrement();
        PendingResponseSlot slot = new PendingResponseSlot();
        pending.put(correlationId, slot);
        MqRequestHeader header = new MqRequestHeader(apiKey, correlationId, "broker-internal");
        try {
            channel.writeAndFlush(ProtocolSerdes.encodeRequest(header, body));
        } catch (RuntimeException | Error failure) {
            if (pending.remove(correlationId, slot)) {
                slot.closeAndDrain();
            }
            throw failure;
        }
        return correlationId;
    }

    private MqDecoder.DecodedResponse awaitResponse(int correlationId, short apiKey)
            throws InterruptedException {
        return awaitResponse(correlationId, apiKey, 5, TimeUnit.SECONDS);
    }

    public synchronized void leaderReady(LeaderReadyRequest request) throws InterruptedException {
        int correlationId = send(ApiKeys.LEADER_READY, request);
        ensureSuccess(awaitResponse(correlationId, ApiKeys.LEADER_READY));
    }

    public synchronized DiscoverControllerResponse discoverController() throws InterruptedException {
        int correlationId = send(ApiKeys.DISCOVER_CONTROLLER, new DiscoverControllerRequest());
        MqDecoder.DecodedResponse decoded = awaitResponse(correlationId, ApiKeys.DISCOVER_CONTROLLER);
        ensureSuccess(decoded);
        return (DiscoverControllerResponse) decoded.body();
    }

    private MqDecoder.DecodedResponse awaitResponse(
            int correlationId, short apiKey, long timeout, TimeUnit unit)
            throws InterruptedException {
        PendingResponseSlot slot = pending.get(correlationId);
        if (slot == null) {
            throw new IllegalStateException("No pending request for response");
        }
        try {
            ByteBuf payload = slot.poll(timeout, unit);
            if (payload == null) {
                throw new IllegalStateException("Timeout waiting for response");
            }
            try {
                return MqDecoder.decodeResponse(payload, apiKey);
            } finally {
                payload.release();
            }
        } finally {
            pending.remove(correlationId, slot);
            slot.closeAndDrain();
        }
    }

    private void ensureSuccess(MqDecoder.DecodedResponse response) {
        MqResponseHeader header = response.header();
        if (header.errorCode() != ErrorCode.NONE) {
            throw new ControllerRequestException(header.errorCode());
        }
    }

    private static long requirePositiveTimeout(long timeoutMs) {
        if (timeoutMs <= 0L) {
            throw new IllegalArgumentException("Controller response timeout must be positive");
        }
        return timeoutMs;
    }

    public static final class ControllerRequestException extends IllegalStateException {
        private final ErrorCode errorCode;

        public ControllerRequestException(ErrorCode errorCode) {
            super("Controller request failed: " + errorCode);
            this.errorCode = errorCode;
        }

        public ErrorCode errorCode() {
            return errorCode;
        }
    }

    @Override
    public synchronized void close() throws InterruptedException {
        close(TimeUnit.SECONDS.toMillis(5));
    }

    public synchronized void close(long timeoutMs) throws InterruptedException {
        if (timeoutMs <= 0) {
            throw new IllegalArgumentException("Controller close timeout must be positive");
        }
        long timeoutNanos = TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        long startedNanos = System.nanoTime();
        try {
            if (channel != null) {
                channel.close().await(timeoutNanos, TimeUnit.NANOSECONDS);
            }
        } finally {
            clearPendingResponses();
        }
        long remainingNanos = timeoutNanos - (System.nanoTime() - startedNanos);
        var shutdown = group.shutdownGracefully(
                0L,
                Math.max(1L, remainingNanos),
                TimeUnit.NANOSECONDS);
        if (remainingNanos > 0) {
            shutdown.await(remainingNanos, TimeUnit.NANOSECONDS);
        }
    }

    private void clearPendingResponses() {
        for (Map.Entry<Integer, PendingResponseSlot> entry : pending.entrySet()) {
            if (pending.remove(entry.getKey(), entry.getValue())) {
                entry.getValue().closeAndDrain();
            }
        }
        pending.clear();
    }

    static final class PendingResponseSlot {
        private final ArrayBlockingQueue<ByteBuf> queue = new ArrayBlockingQueue<>(1);
        private boolean closed;

        boolean offerRetained(ByteBuf message) {
            ByteBuf retained = message.retain();
            synchronized (this) {
                if (closed || !queue.offer(retained)) {
                    retained.release();
                    return false;
                }
                return true;
            }
        }

        ByteBuf poll(long timeout, TimeUnit unit) throws InterruptedException {
            return queue.poll(timeout, unit);
        }

        synchronized void closeAndDrain() {
            closed = true;
            ByteBuf payload;
            while ((payload = queue.poll()) != null) {
                payload.release();
            }
        }
    }
}
