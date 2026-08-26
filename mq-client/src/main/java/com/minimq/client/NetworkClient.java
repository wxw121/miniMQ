package com.minimq.client;

import com.minimq.client.exception.MiniMqConnectionException;
import com.minimq.client.exception.MiniMqRequestTimeoutException;
import com.minimq.client.exception.MiniMqServerException;
import com.minimq.common.error.ErrorCode;
import com.minimq.protocol.ApiKeys;
import com.minimq.protocol.ProtocolSerdes;
import com.minimq.protocol.codec.MqDecoder;
import com.minimq.protocol.codec.MqEncoder;
import com.minimq.protocol.message.MqRequestHeader;
import com.minimq.protocol.message.MqResponseHeader;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * MiniMQ 网络客户端，维护单条 Netty 连接、请求相关性映射和固定退避连接重试。
 */
public class NetworkClient implements AutoCloseable {
    public static final Duration DEFAULT_REQUEST_TIMEOUT = Duration.ofSeconds(10);
    private static final long CANDIDATE_ABORT_TIMEOUT_MS = 250L;
    private final String clientId;
    private final RetryPolicy retryPolicy;
    private final Duration requestTimeout;
    private final AtomicInteger correlationId = new AtomicInteger(1);
    private final EventLoopGroup group = SharedIoEventLoops.acquire();
    private final AtomicBoolean ioReleased = new AtomicBoolean();
    private final Map<Integer, PendingResponseSlot> pending = new ConcurrentHashMap<>();
    private final AtomicInteger connectionAttempts = new AtomicInteger();
    private Channel channel;

    public NetworkClient(String clientId) {
        this(clientId, RetryPolicy.fixedBackoff(1, 0), DEFAULT_REQUEST_TIMEOUT);
    }

    public NetworkClient(String clientId, RetryPolicy retryPolicy) {
        this(clientId, retryPolicy, DEFAULT_REQUEST_TIMEOUT);
    }

    public NetworkClient(String clientId, RetryPolicy retryPolicy, Duration requestTimeout) {
        this.clientId = clientId;
        this.retryPolicy = Objects.requireNonNull(retryPolicy, "retryPolicy");
        this.requestTimeout = validateRequestTimeout(requestTimeout);
    }

    // 连接失败时按固定退避策略重试，避免 Broker 尚未启动时客户端立即失败。
    public void connect(String host, int port) throws InterruptedException {
        connect(host, port, requestTimeout);
    }

    public void connect(String host, int port, Duration timeout) throws InterruptedException {
        Duration effectiveTimeout = effectiveRequestTimeout(timeout);
        long deadlineNanos = deadlineNanosFromNow(effectiveTimeout);
        Throwable lastFailure = null;
        int attempts = 0;
        for (int attempt = 1; attempt <= retryPolicy.maxAttempts(); attempt++) {
            long remainingNanos = deadlineNanos - System.nanoTime();
            if (remainingNanos <= 0) {
                break;
            }

            attempts++;
            connectionAttempts.incrementAndGet();
            ChannelFuture connectFuture = createBootstrap().connect(host, port);
            boolean completed;
            try {
                completed = connectFuture.await(remainingNanos, TimeUnit.NANOSECONDS);
            } catch (InterruptedException interrupted) {
                abortConnect(connectFuture);
                throw interrupted;
            }
            if (!completed) {
                lastFailure = new MiniMqRequestTimeoutException("CONNECT", effectiveTimeout);
                abortConnect(connectFuture);
                break;
            }
            if (connectFuture.isSuccess()) {
                channel = connectFuture.channel();
                return;
            }

            lastFailure = connectFuture.cause();
            abortConnect(connectFuture);
            if (attempt < retryPolicy.maxAttempts()) {
                long backoffNanos = TimeUnit.MILLISECONDS.toNanos(retryPolicy.backoffMillis());
                remainingNanos = deadlineNanos - System.nanoTime();
                if (backoffNanos >= remainingNanos) {
                    break;
                }
                TimeUnit.NANOSECONDS.sleep(backoffNanos);
            }
        }
        throw new MiniMqConnectionException("Failed to connect to " + host + ":" + port
                + " after " + attempts + " attempts", lastFailure);
    }

    private static void abortConnect(ChannelFuture connectFuture) {
        connectFuture.cancel(false);
        if (connectFuture.channel() != null) {
            connectFuture.channel().close();
        }
    }

    public int connectionAttempts() {
        return connectionAttempts.get();
    }

    // Netty pipeline 顺序保持为解码 -> 响应分发 -> 编码，保证业务层只处理完整 payload。
    private Bootstrap createBootstrap() {
        Bootstrap bootstrap = new Bootstrap()
                .group(group)
                .channel(NioSocketChannel.class)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline().addLast(new MqDecoder(), new SimpleChannelInboundHandler<ByteBuf>() {
                            @Override
                            protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) {
                                int corrId = msg.getInt(msg.readerIndex());
                                PendingResponseSlot slot = pending.get(corrId);
                                if (slot != null) {
                                    slot.offerRetained(msg);
                                }
                            }
                        }, new MqEncoder());
                    }
                });
        return bootstrap;
    }

    // correlationId 与阻塞队列一一对应，用来把异步响应重新匹配回同步 send 调用。
    public MqDecoder.DecodedResponse send(short apiKey, Object body) throws InterruptedException {
        return send(apiKey, body, requestTimeout);
    }

    public MqDecoder.DecodedResponse send(
            short apiKey, Object body, Duration timeout) throws InterruptedException {
        if (channel == null || !channel.isActive()) {
            throw new MiniMqConnectionException("Client is not connected", null);
        }
        Duration effectiveTimeout = effectiveRequestTimeout(timeout);
        int corr = correlationId.getAndIncrement();
        PendingResponseSlot slot = new PendingResponseSlot();
        pending.put(corr, slot);
        ByteBuf payload = null;
        try {
            MqRequestHeader header = new MqRequestHeader(apiKey, corr, clientId);
            channel.writeAndFlush(ProtocolSerdes.encodeRequest(header, body));
            payload = slot.poll(durationToNanos(effectiveTimeout), TimeUnit.NANOSECONDS);
            if (payload == null) {
                throw new MiniMqRequestTimeoutException(ApiKeys.nameOf(apiKey), effectiveTimeout);
            }
            return MqDecoder.decodeResponse(payload, apiKey);
        } finally {
            pending.remove(corr, slot);
            slot.closeAndDrain();
            if (payload != null) {
                payload.release();
            }
        }
    }

    public void ensureSuccess(MqDecoder.DecodedResponse response) {
        ensureSuccess(response, "Request");
    }

    public void ensureSuccess(MqDecoder.DecodedResponse response, String operation) {
        MqResponseHeader header = response.header();
        if (header.errorCode() != ErrorCode.NONE) {
            throw new MiniMqServerException(header.errorCode(), operation);
        }
    }

    @Override
    public void close() throws InterruptedException {
        try {
            if (channel != null) {
                channel.close().sync();
            }
        } finally {
            clearPendingResponses();
            releaseIo();
        }
    }

    void abortCandidate() throws InterruptedException {
        try {
            if (channel != null) {
                channel.close().await(CANDIDATE_ABORT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            }
        } finally {
            clearPendingResponses();
            releaseIo(0L, CANDIDATE_ABORT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        }
    }

    void abortCandidate(Duration timeout) throws InterruptedException {
        Duration cleanupTimeout = Objects.requireNonNull(timeout, "timeout");
        if (cleanupTimeout.isNegative()) {
            throw new IllegalArgumentException("timeout must not be negative");
        }
        long deadlineNanos = deadlineNanosFromNow(cleanupTimeout);
        try {
            if (channel != null) {
                ChannelFuture closeFuture = channel.close();
                long remainingNanos = deadlineNanos - System.nanoTime();
                if (remainingNanos > 0L) {
                    closeFuture.await(remainingNanos, TimeUnit.NANOSECONDS);
                }
            }
        } finally {
            clearPendingResponses();
            long remainingNanos = Math.max(0L, deadlineNanos - System.nanoTime());
            releaseIo(0L, remainingNanos, TimeUnit.NANOSECONDS);
        }
    }

    EventLoopGroup eventLoopGroup() {
        return group;
    }

    private void releaseIo() throws InterruptedException {
        if (!ioReleased.compareAndSet(false, true)) {
            return;
        }
        SharedIoEventLoops.release(group);
    }

    private void releaseIo(long quietPeriod, long timeout, TimeUnit unit) throws InterruptedException {
        if (!ioReleased.compareAndSet(false, true)) {
            return;
        }
        SharedIoEventLoops.release(group, quietPeriod, timeout, unit);
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

    public record RetryPolicy(int maxAttempts, long backoffMillis) {
        public RetryPolicy {
            if (maxAttempts < 1) {
                throw new IllegalArgumentException("maxAttempts must be positive");
            }
            if (backoffMillis < 0) {
                throw new IllegalArgumentException("backoffMillis must be non-negative");
            }
        }

        public static RetryPolicy fixedBackoff(int maxAttempts, long backoffMillis) {
            return new RetryPolicy(maxAttempts, backoffMillis);
        }
    }

    private static Duration validateRequestTimeout(Duration requestTimeout) {
        Duration timeout = Objects.requireNonNull(requestTimeout, "requestTimeout");
        if (timeout.isZero() || timeout.isNegative()) {
            throw new IllegalArgumentException("requestTimeout must be positive");
        }
        durationToNanos(timeout);
        return timeout;
    }

    private Duration effectiveRequestTimeout(Duration timeout) {
        Duration requested = Objects.requireNonNull(timeout, "timeout");
        if (requested.isNegative()) {
            throw new IllegalArgumentException("timeout must not be negative");
        }
        durationToNanos(requested);
        return requested.compareTo(requestTimeout) < 0 ? requested : requestTimeout;
    }

    static long durationToNanos(Duration timeout) {
        try {
            return Objects.requireNonNull(timeout, "timeout").toNanos();
        } catch (ArithmeticException overflow) {
            throw new IllegalArgumentException("timeout is too large", overflow);
        }
    }

    static long deadlineNanosFromNow(Duration timeout) {
        return deadlineNanos(System.nanoTime(), timeout);
    }

    static long deadlineNanos(long startNanos, Duration timeout) {
        long timeoutNanos = durationToNanos(timeout);
        try {
            return Math.addExact(startNanos, timeoutNanos);
        } catch (ArithmeticException overflow) {
            throw new IllegalArgumentException("timeout deadline is too large", overflow);
        }
    }

}
