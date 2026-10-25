package com.minimq.broker.replication;

import com.minimq.common.error.ErrorCode;
import com.minimq.protocol.ApiKeys;
import com.minimq.protocol.ProtocolSerdes;
import com.minimq.protocol.codec.MqDecoder;
import com.minimq.protocol.codec.MqEncoder;
import com.minimq.protocol.message.MqRequestHeader;
import com.minimq.protocol.message.ProtocolMessages.ReplicaFetchRequest;
import com.minimq.protocol.message.ProtocolMessages.ReplicaFetchResponse;
import com.minimq.protocol.message.ProtocolMessages.ReplicaFetchV2Request;
import com.minimq.protocol.message.ProtocolMessages.ReplicaFetchV2Response;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

final class LeaderReplicaClient implements AutoCloseable {
    private static final AtomicInteger CORR = new AtomicInteger(1);
    private final EventLoopGroup group;
    private final boolean ownsGroup;
    private final ArrayBlockingQueue<ByteBuf> responses = new ArrayBlockingQueue<>(4);
    private Channel channel;

    LeaderReplicaClient() {
        this(new NioEventLoopGroup(1), true);
    }

    LeaderReplicaClient(EventLoopGroup group, boolean ownsGroup) {
        this.group = group;
        this.ownsGroup = ownsGroup;
    }

    void connect(String host, int port) throws InterruptedException {
        Bootstrap bootstrap = new Bootstrap()
                .group(group)
                .channel(NioSocketChannel.class)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline().addLast(new com.minimq.protocol.codec.MqDecoder(),
                                new SimpleChannelInboundHandler<ByteBuf>() {
                                    @Override
                                    protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) {
                                        ByteBuf response = msg.retain();
                                        if (!responses.offer(response)) {
                                            response.release();
                                        }
                                    }
                                },
                                new MqEncoder());
                    }
                });
        channel = bootstrap.connect(host, port).sync().channel();
    }

    ReplicaFetchResponse replicaFetch(
            String topicName,
            int partitionId,
            int replicaBrokerId,
            long fetchOffset,
            int maxBytes) throws InterruptedException {
        return replicaFetch(topicName, partitionId, replicaBrokerId, 0L, fetchOffset, maxBytes);
    }

    ReplicaFetchResponse replicaFetch(
            String topicName,
            int partitionId,
            int replicaBrokerId,
            long leaderEpoch,
            long fetchOffset,
            int maxBytes) throws InterruptedException {
        MqRequestHeader header = new MqRequestHeader(
                ApiKeys.REPLICA_FETCH,
                CORR.getAndIncrement(),
                "replica-fetcher");
        ReplicaFetchRequest request = new ReplicaFetchRequest(
                topicName, partitionId, replicaBrokerId, leaderEpoch, fetchOffset, maxBytes);
        channel.writeAndFlush(ProtocolSerdes.encodeRequest(header, request));
        ByteBuf payload = responses.poll(5, TimeUnit.SECONDS);
        if (payload == null) {
            throw new IllegalStateException("Timeout waiting for replica fetch response");
        }
        try {
            MqDecoder.DecodedResponse decoded = MqDecoder.decodeResponse(payload, ApiKeys.REPLICA_FETCH);
            if (decoded.header().errorCode() != ErrorCode.NONE) {
                throw new ReplicaFetchException(decoded.header().errorCode());
            }
            return (ReplicaFetchResponse) decoded.body();
        } finally {
            payload.release();
        }
    }

    ReplicaFetchV2Response replicaFetchV2(
            String clusterCredential,
            String topicName,
            int partitionId,
            int replicaBrokerId,
            long brokerEpoch,
            long controllerEpoch,
            long leaderEpoch,
            long fetchOffset,
            int maxBytes) throws InterruptedException {
        MqRequestHeader header = new MqRequestHeader(
                ApiKeys.REPLICA_FETCH_V2,
                CORR.getAndIncrement(),
                "replica-fetcher-v2");
        ReplicaFetchV2Request request = new ReplicaFetchV2Request(
                clusterCredential,
                topicName,
                partitionId,
                replicaBrokerId,
                brokerEpoch,
                controllerEpoch,
                leaderEpoch,
                fetchOffset,
                maxBytes);
        channel.writeAndFlush(ProtocolSerdes.encodeRequest(header, request));
        ByteBuf payload = responses.poll(5, TimeUnit.SECONDS);
        if (payload == null) {
            throw new IllegalStateException("Timeout waiting for replica fetch v2 response");
        }
        try {
            MqDecoder.DecodedResponse decoded = MqDecoder.decodeResponse(payload, ApiKeys.REPLICA_FETCH_V2);
            if (decoded.header().errorCode() != ErrorCode.NONE) {
                throw new ReplicaFetchException(decoded.header().errorCode());
            }
            return (ReplicaFetchV2Response) decoded.body();
        } finally {
            payload.release();
        }
    }

    @Override
    public void close() throws InterruptedException {
        if (channel != null) {
            channel.close().sync();
        }
        if (ownsGroup) {
            group.shutdownGracefully().sync();
        }
    }

    static final class ReplicaFetchException extends RuntimeException {
        private final ErrorCode errorCode;

        ReplicaFetchException(ErrorCode errorCode) {
            super("Replica fetch failed: " + errorCode);
            this.errorCode = errorCode;
        }

        ErrorCode errorCode() {
            return errorCode;
        }
    }
}
