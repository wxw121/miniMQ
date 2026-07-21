package com.minimq.broker.netty;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class NettyTransportFactoryTest {

    @Test
    void nioModeAlwaysSelectsNioGroupsAndChannel() throws InterruptedException {
        NettyTransport transport = new NettyTransportFactory(unavailableNativeSupport())
                .create(NetworkTransportMode.NIO);
        try {
            assertEquals(NetworkTransportMode.NIO, transport.selectedMode());
            assertEquals(NioServerSocketChannel.class, transport.serverChannelClass());
            assertEquals(NioEventLoopGroup.class, transport.bossGroup().getClass());
            assertEquals(NioEventLoopGroup.class, transport.workerGroup().getClass());
        } finally {
            transport.shutdown();
        }
    }

    @Test
    void autoModeFallsBackToNioWhenEpollIsUnavailable() throws InterruptedException {
        NettyTransport transport = new NettyTransportFactory(unavailableNativeSupport())
                .create(NetworkTransportMode.AUTO);
        try {
            assertEquals(NetworkTransportMode.NIO, transport.selectedMode());
            assertEquals(NioServerSocketChannel.class, transport.serverChannelClass());
        } finally {
            transport.shutdown();
        }
    }

    @Test
    void explicitEpollModeFailsWhenEpollIsUnavailable() {
        IllegalStateException failure = assertThrows(IllegalStateException.class,
                () -> new NettyTransportFactory(unavailableNativeSupport()).create(NetworkTransportMode.EPOLL));

        assertEquals("Epoll transport requested but unavailable: test unavailable",
                failure.getMessage());
    }

    private static NettyTransportFactory.NativeSupport unavailableNativeSupport() {
        return new NettyTransportFactory.NativeSupport() {
            @Override
            public boolean epollAvailable() {
                return false;
            }

            @Override
            public String epollUnavailableReason() {
                return "test unavailable";
            }

            @Override
            public EventLoopGroup newEpollEventLoopGroup(int threads) {
                throw new AssertionError("epoll group should not be created");
            }

            @Override
            public Class<? extends ServerChannel> epollServerChannelClass() {
                throw new AssertionError("epoll channel should not be selected");
            }
        };
    }
}
