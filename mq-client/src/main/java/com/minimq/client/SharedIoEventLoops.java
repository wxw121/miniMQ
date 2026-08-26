package com.minimq.client;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Process-wide Netty worker group so many {@link NetworkClient} instances do not
 * each create a dedicated event loop thread.
 */
final class SharedIoEventLoops {
    private static final Object LOCK = new Object();
    private static EventLoopGroup group;
    private static int refCount;

    private SharedIoEventLoops() {
    }

    static EventLoopGroup acquire() {
        synchronized (LOCK) {
            if (group == null) {
                group = new NioEventLoopGroup(ioThreads());
                refCount = 0;
            }
            refCount++;
            return group;
        }
    }

    static void release(EventLoopGroup held) throws InterruptedException {
        release(held, 1L, 15L, TimeUnit.SECONDS, true);
    }

    static void release(
            EventLoopGroup held,
            long quietPeriod,
            long timeout,
            TimeUnit unit) throws InterruptedException {
        release(held, quietPeriod, timeout, unit, false);
    }

    private static void release(
            EventLoopGroup held,
            long quietPeriod,
            long timeout,
            TimeUnit unit,
            boolean awaitIndefinitely) throws InterruptedException {
        Objects.requireNonNull(held, "held");
        io.netty.util.concurrent.Future<?> shutdown = null;
        synchronized (LOCK) {
            if (held != group) {
                shutdown = held.shutdownGracefully(quietPeriod, timeout, unit);
            } else {
                refCount--;
                if (refCount <= 0) {
                    refCount = 0;
                    EventLoopGroup toStop = group;
                    group = null;
                    shutdown = toStop.shutdownGracefully(quietPeriod, timeout, unit);
                }
            }
        }
        if (shutdown == null) {
            return;
        }
        if (awaitIndefinitely) {
            shutdown.sync();
            return;
        }
        shutdown.await(timeout, unit);
    }

    static int ioThreads() {
        return Math.max(1, Runtime.getRuntime().availableProcessors());
    }
}

