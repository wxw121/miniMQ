package com.minimq.client;

import com.minimq.common.error.ErrorCode;
import com.minimq.protocol.ProtocolSerdes;
import com.minimq.protocol.codec.MqDecoder;
import com.minimq.protocol.codec.MqEncoder;
import com.minimq.protocol.message.MqRequestHeader;
import com.minimq.protocol.message.MqResponseHeader;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class NetworkClient implements AutoCloseable {
    private final String clientId;
    private final RetryPolicy retryPolicy;
    private final AtomicInteger correlationId = new AtomicInteger(1);
    private final EventLoopGroup group = new NioEventLoopGroup(1);
    private final Map<Integer, ArrayBlockingQueue<ByteBuf>> pending = new ConcurrentHashMap<>();
    private final AtomicInteger connectionAttempts = new AtomicInteger();
    private Channel channel;

    public NetworkClient(String clientId) {
        this(clientId, RetryPolicy.fixedBackoff(1, 0));
    }

    public NetworkClient(String clientId, RetryPolicy retryPolicy) {
        this.clientId = clientId;
        this.retryPolicy = retryPolicy;
    }

    public void connect(String host, int port) throws InterruptedException {
        Throwable lastFailure = null;
        for (int attempt = 1; attempt <= retryPolicy.maxAttempts(); attempt++) {
            connectionAttempts.incrementAndGet();
            ChannelFuture connectFuture = createBootstrap().connect(host, port);
            connectFuture.await();
            if (connectFuture.isSuccess()) {
                channel = connectFuture.channel();
                return;
            }

            lastFailure = connectFuture.cause();
            if (connectFuture.channel() != null) {
                connectFuture.channel().close();
            }
            if (attempt < retryPolicy.maxAttempts()) {
                Thread.sleep(retryPolicy.backoffMillis());
            }
        }
        throw new IllegalStateException("Failed to connect to " + host + ":" + port
                + " after " + retryPolicy.maxAttempts() + " attempts", lastFailure);
    }

    public int connectionAttempts() {
        return connectionAttempts.get();
    }

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
                                ArrayBlockingQueue<ByteBuf> queue = pending.get(corrId);
                                if (queue != null) {
                                    ByteBuf response = msg.retain();
                                    if (!queue.offer(response)) {
                                        response.release();
                                    }
                                }
                            }
                        }, new MqEncoder());
                    }
                });
        return bootstrap;
    }

    public MqDecoder.DecodedResponse send(short apiKey, Object body) throws InterruptedException {
        int corr = correlationId.getAndIncrement();
        ArrayBlockingQueue<ByteBuf> queue = new ArrayBlockingQueue<>(1);
        pending.put(corr, queue);
        ByteBuf payload = null;
        try {
            MqRequestHeader header = new MqRequestHeader(apiKey, corr, clientId);
            channel.writeAndFlush(ProtocolSerdes.encodeRequest(header, body));
            payload = queue.poll(10, TimeUnit.SECONDS);
            if (payload == null) {
                throw new IllegalStateException("Request timeout");
            }
            return MqDecoder.decodeResponse(payload, apiKey);
        } finally {
            pending.remove(corr);
            if (payload != null) {
                payload.release();
            }
        }
    }

    public void ensureSuccess(MqDecoder.DecodedResponse response) {
        MqResponseHeader header = response.header();
        if (header.errorCode() != ErrorCode.NONE) {
            throw new IllegalStateException("Request failed: " + header.errorCode());
        }
    }

    @Override
    public void close() throws InterruptedException {
        if (channel != null) {
            channel.close().sync();
        }
        group.shutdownGracefully().sync();
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
}
