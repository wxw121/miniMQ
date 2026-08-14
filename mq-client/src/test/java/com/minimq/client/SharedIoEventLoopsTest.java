package com.minimq.client;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SharedIoEventLoopsTest {

    @Test
    void ioThreadsIsAtLeastOne() {
        assertTrue(SharedIoEventLoops.ioThreads() >= 1);
    }

    @Test
    void releaseShutsDownAnOrphanGroupThatIsNotTheSharedPool() throws Exception {
        EventLoopGroup orphan = new NioEventLoopGroup(1);
        SharedIoEventLoops.release(orphan, 0L, 2L, TimeUnit.SECONDS);
        assertTrue(orphan.isShuttingDown() || orphan.isShutdown() || orphan.isTerminated());
    }

    @Test
    void releaseRejectsNullGroup() {
        assertThrows(NullPointerException.class, () -> SharedIoEventLoops.release(null));
    }
}
