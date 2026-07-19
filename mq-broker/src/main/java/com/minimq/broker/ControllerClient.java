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
import com.minimq.protocol.message.ProtocolMessages.BrokerHeartbeatRequest;
import com.minimq.protocol.message.ProtocolMessages.RegisterBrokerRequest;
import com.minimq.protocol.message.ProtocolMessages.RegisterBrokerResponse;
import com.minimq.protocol.message.ProtocolMessages.UpdateReplicaLeoRequest;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Broker 侧 Controller 客户端，用于向 Controller 注册自身并同步最新集群元数据。
 */
public class ControllerClient implements AutoCloseable {
    private static final AtomicInteger CORR = new AtomicInteger(1);
    private final EventLoopGroup group = new NioEventLoopGroup(1);
    private Channel channel;
    private final ArrayBlockingQueue<ByteBuf> responses = new ArrayBlockingQueue<>(16);

    public void connect(String host, int port) throws InterruptedException {
        Bootstrap bootstrap = new Bootstrap()
                .group(group)
                .channel(NioSocketChannel.class)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline().addLast(new MqDecoder(), new SimpleChannelInboundHandler<ByteBuf>() {
                            @Override
                            protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) {
                                ByteBuf response = msg.retain();
                                if (!responses.offer(response)) {
                                    response.release();
                                }
                            }
                        }, new MqEncoder());
                    }
                });
        channel = bootstrap.connect(host, port).sync().channel();
    }

    public synchronized RegisterBrokerResponse registerBroker(RegisterBrokerRequest request)
            throws InterruptedException {
        send(ApiKeys.REGISTER_BROKER, request);
        MqDecoder.DecodedResponse response = awaitResponse(ApiKeys.REGISTER_BROKER);
        ensureSuccess(response);
        return (RegisterBrokerResponse) response.body();
    }

    public synchronized void heartbeatBroker(BrokerHeartbeatRequest request) throws InterruptedException {
        send(ApiKeys.BROKER_HEARTBEAT, request);
        ensureSuccess(awaitResponse(ApiKeys.BROKER_HEARTBEAT));
    }

    public synchronized MetadataResponse fetchMetadata() throws InterruptedException {
        send(ApiKeys.METADATA, new MetadataRequest(""));
        MqDecoder.DecodedResponse decoded = awaitResponse(ApiKeys.METADATA);
        ensureSuccess(decoded);
        return (MetadataResponse) decoded.body();
    }

    public synchronized void updateReplicaLeo(UpdateReplicaLeoRequest request) throws InterruptedException {
        send(ApiKeys.UPDATE_REPLICA_LEO, request);
        ensureSuccess(awaitResponse(ApiKeys.UPDATE_REPLICA_LEO));
    }

    private void send(short apiKey, Object body) {
        MqRequestHeader header = new MqRequestHeader(apiKey, CORR.getAndIncrement(), "broker-internal");
        channel.writeAndFlush(ProtocolSerdes.encodeRequest(header, body));
    }

    private MqDecoder.DecodedResponse awaitResponse(short apiKey) throws InterruptedException {
        ByteBuf payload = responses.poll(5, TimeUnit.SECONDS);
        if (payload == null) {
            throw new IllegalStateException("Timeout waiting for response");
        }
        try {
            return MqDecoder.decodeResponse(payload, apiKey);
        } finally {
            payload.release();
        }
    }

    private void ensureSuccess(MqDecoder.DecodedResponse response) {
        MqResponseHeader header = response.header();
        if (header.errorCode() != ErrorCode.NONE) {
            throw new ControllerRequestException(header.errorCode());
        }
    }

    public static final class ControllerRequestException extends IllegalStateException {
        private final ErrorCode errorCode;

        ControllerRequestException(ErrorCode errorCode) {
            super("Controller request failed: " + errorCode);
            this.errorCode = errorCode;
        }

        public ErrorCode errorCode() {
            return errorCode;
        }
    }

    @Override
    public void close() throws InterruptedException {
        if (channel != null) {
            channel.close().sync();
        }
        group.shutdownGracefully().sync();
    }
}
