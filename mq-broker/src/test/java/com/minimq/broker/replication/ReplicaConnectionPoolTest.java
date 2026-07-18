package com.minimq.broker.replication;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;

class ReplicaConnectionPoolTest {

    @Test
    void reusesClientForSameLeader() throws Exception {
        try (CountingLeader leader = CountingLeader.start();
             ReplicaConnectionPool pool = new ReplicaConnectionPool()) {
            LeaderReplicaClient first = pool.client("127.0.0.1", leader.port());
            LeaderReplicaClient second = pool.client("127.0.0.1", leader.port());

            assertSame(first, second);
            waitForAcceptedConnections(leader, 1);
            assertEquals(1, leader.acceptedConnections());
        }
    }

    @Test
    void invalidateClosesAndRecreatesClient() throws Exception {
        try (CountingLeader leader = CountingLeader.start();
             ReplicaConnectionPool pool = new ReplicaConnectionPool()) {
            LeaderReplicaClient first = pool.client("127.0.0.1", leader.port());
            waitForAcceptedConnections(leader, 1);

            pool.invalidate("127.0.0.1", leader.port());
            LeaderReplicaClient second = pool.client("127.0.0.1", leader.port());

            assertNotSame(first, second);
            waitForAcceptedConnections(leader, 2);
            assertEquals(2, leader.acceptedConnections());
        }
    }

    private static void waitForAcceptedConnections(
            CountingLeader leader,
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

    private static final class CountingLeader implements AutoCloseable {
        private final EventLoopGroup boss;
        private final EventLoopGroup worker;
        private final Channel channel;
        private final AtomicInteger acceptedConnections;

        private CountingLeader(
                EventLoopGroup boss,
                EventLoopGroup worker,
                Channel channel,
                AtomicInteger acceptedConnections) {
            this.boss = boss;
            this.worker = worker;
            this.channel = channel;
            this.acceptedConnections = acceptedConnections;
        }

        static CountingLeader start() throws InterruptedException {
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
                        }
                    })
                    .bind("127.0.0.1", 0)
                    .sync()
                    .channel();
            return new CountingLeader(boss, worker, server, acceptedConnections);
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
