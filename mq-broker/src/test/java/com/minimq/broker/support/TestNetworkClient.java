package com.minimq.broker.support;

import com.minimq.protocol.ProtocolSerdes;
import com.minimq.protocol.codec.MqDecoder;
import com.minimq.protocol.codec.MqEncoder;
import com.minimq.protocol.message.MqRequestHeader;
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

public final class TestNetworkClient implements AutoCloseable {
    private static final AtomicInteger CORR = new AtomicInteger(1);
    private final EventLoopGroup group = new NioEventLoopGroup(1);
    private final Map<Integer, ArrayBlockingQueue<ByteBuf>> pending = new ConcurrentHashMap<>();
    private Channel channel;

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
        channel = bootstrap.connect(host, port).sync().channel();
    }

    public MqDecoder.DecodedResponse send(short apiKey, Object body) throws InterruptedException {
        int corr = CORR.getAndIncrement();
        ArrayBlockingQueue<ByteBuf> queue = new ArrayBlockingQueue<>(1);
        pending.put(corr, queue);
        ByteBuf payload = null;
        try {
            MqRequestHeader header = new MqRequestHeader(apiKey, corr, "broker-test");
            channel.writeAndFlush(ProtocolSerdes.encodeRequest(header, body)).sync();
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

    @Override
    public void close() throws InterruptedException {
        if (channel != null) {
            channel.close().sync();
        }
        group.shutdownGracefully().sync();
    }
}
