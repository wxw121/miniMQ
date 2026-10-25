package com.minimq.broker.netty;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;

public final class NettyTransportFactory {
    private final NativeSupport nativeSupport;

    public NettyTransportFactory() {
        this(new ReflectionNativeSupport());
    }

    public NettyTransportFactory(NativeSupport nativeSupport) {
        this.nativeSupport = nativeSupport;
    }

    public NettyTransport create(NetworkTransportMode mode) {
        return switch (mode) {
            case NIO -> nioTransport();
            case AUTO -> nativeSupport.epollAvailable() ? epollTransport() : nioTransport();
            case EPOLL -> {
                if (!nativeSupport.epollAvailable()) {
                    throw new IllegalStateException(
                            "Epoll transport requested but unavailable: "
                                    + nativeSupport.epollUnavailableReason());
                }
                yield epollTransport();
            }
        };
    }

    public interface NativeSupport {
        boolean epollAvailable();

        String epollUnavailableReason();

        EventLoopGroup newEpollEventLoopGroup(int threads);

        Class<? extends ServerChannel> epollServerChannelClass();
    }

    private static NettyTransport nioTransport() {
        return new NettyTransport(
                new NioEventLoopGroup(1),
                new NioEventLoopGroup(),
                NioServerSocketChannel.class,
                NetworkTransportMode.NIO);
    }

    private NettyTransport epollTransport() {
        return new NettyTransport(
                nativeSupport.newEpollEventLoopGroup(1),
                nativeSupport.newEpollEventLoopGroup(0),
                nativeSupport.epollServerChannelClass(),
                NetworkTransportMode.EPOLL);
    }

    private static final class ReflectionNativeSupport implements NativeSupport {
        @Override
        public boolean epollAvailable() {
            try {
                Class<?> epoll = Class.forName("io.netty.channel.epoll.Epoll");
                Method isAvailable = epoll.getMethod("isAvailable");
                return (boolean) isAvailable.invoke(null);
            } catch (ReflectiveOperationException | LinkageError failure) {
                return false;
            }
        }

        @Override
        public String epollUnavailableReason() {
            try {
                Class<?> epoll = Class.forName("io.netty.channel.epoll.Epoll");
                Method cause = epoll.getMethod("unavailabilityCause");
                Throwable unavailable = (Throwable) cause.invoke(null);
                return unavailable == null ? "unknown" : unavailable.getMessage();
            } catch (ReflectiveOperationException | LinkageError failure) {
                return failure.getMessage();
            }
        }

        @Override
        public EventLoopGroup newEpollEventLoopGroup(int threads) {
            try {
                Class<?> groupClass = Class.forName("io.netty.channel.epoll.EpollEventLoopGroup");
                Constructor<?> constructor = groupClass.getConstructor(int.class);
                return (EventLoopGroup) constructor.newInstance(threads);
            } catch (ReflectiveOperationException | LinkageError failure) {
                throw new IllegalStateException("Could not create EpollEventLoopGroup", failure);
            }
        }

        @Override
        public Class<? extends ServerChannel> epollServerChannelClass() {
            try {
                return Class.forName("io.netty.channel.epoll.EpollServerSocketChannel")
                        .asSubclass(ServerChannel.class);
            } catch (ReflectiveOperationException | LinkageError failure) {
                throw new IllegalStateException("Could not load EpollServerSocketChannel", failure);
            }
        }
    }
}
