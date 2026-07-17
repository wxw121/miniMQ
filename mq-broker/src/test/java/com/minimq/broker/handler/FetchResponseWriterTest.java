package com.minimq.broker.handler;

import com.minimq.broker.FetchTransferMetrics;
import com.minimq.broker.FetchTransferMode;
import com.minimq.common.model.Record;
import com.minimq.protocol.message.MqResponseHeader;
import com.minimq.protocol.message.ProtocolMessages.FetchRequest;
import com.minimq.common.error.ErrorCode;
import com.minimq.storage.PartitionLog;
import com.minimq.storage.SegmentedLogConfig;
import com.minimq.storage.SegmentedPartitionLog;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.DefaultFileRegion;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.ReferenceCountUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FetchResponseWriterTest {

    @Test
    void autoWritesPrefixAndFileRegion(@TempDir Path dir) throws Exception {
        try (SegmentedPartitionLog log = new SegmentedPartitionLog(dir, new SegmentedLogConfig(1_024, 1))) {
            log.append("k".getBytes(), "value".getBytes());
            FetchTransferMetrics metrics = new FetchTransferMetrics();
            FetchResponseWriter writer = new FetchResponseWriter(
                    FetchTransferMode.AUTO, 30_000, metrics, channel -> true);
            ChannelHandlerContext ctx = captureContext(new EmbeddedChannel());
            EmbeddedChannel channel = (EmbeddedChannel) ctx.channel();
            MqResponseHeader header = new MqResponseHeader(7, ErrorCode.NONE);
            FetchRequest request = new FetchRequest("orders", 0, 0, 1_024);

            try {
                assertTrue(writer.write(ctx, header, request, log));
                channel.runPendingTasks();
                Object first = channel.readOutbound();
                Object second = channel.readOutbound();
                try {
                    assertInstanceOf(ByteBuf.class, first);
                    assertInstanceOf(DefaultFileRegion.class, second);
                    assertEquals(1, metrics.snapshot().sendfileResponses());
                    assertTrue(metrics.snapshot().sendfileBytes() > 0);
                    assertNull(channel.readOutbound());
                } finally {
                    ReferenceCountUtil.release(first);
                    ReferenceCountUtil.release(second);
                }
            } finally {
                channel.finishAndReleaseAll();
            }
        }
    }

    @Test
    void heapModeWritesSingleFrame(@TempDir Path dir) throws Exception {
        try (SegmentedPartitionLog log = new SegmentedPartitionLog(dir, new SegmentedLogConfig(1_024, 1))) {
            log.append("k".getBytes(), "value".getBytes());
            FetchTransferMetrics metrics = new FetchTransferMetrics();
            FetchResponseWriter writer = new FetchResponseWriter(
                    FetchTransferMode.HEAP, 30_000, metrics, channel -> true);
            ChannelHandlerContext ctx = captureContext(new EmbeddedChannel());
            EmbeddedChannel channel = (EmbeddedChannel) ctx.channel();
            try {
                assertTrue(writer.write(ctx, new MqResponseHeader(1, ErrorCode.NONE),
                        new FetchRequest("orders", 0, 0, 1_024), log));
                channel.runPendingTasks();
                Object outbound = channel.readOutbound();
                try {
                    assertInstanceOf(ByteBuf.class, outbound);
                    assertNull(channel.readOutbound());
                    assertEquals(1, metrics.snapshot().heapResponses());
                    assertEquals(1, metrics.snapshot().heapModeFallbacks());
                    assertEquals(0, metrics.snapshot().sendfileResponses());
                } finally {
                    ReferenceCountUtil.release(outbound);
                }
            } finally {
                channel.finishAndReleaseAll();
            }
        }
    }

    @Test
    void unsupportedLogFallsBackToHeap() {
        PartitionLog stub = new PartitionLog() {
            @Override
            public long append(byte[] key, byte[] value) {
                return 0;
            }

            @Override
            public void appendAt(long offset, byte[] key, byte[] value) {
            }

            @Override
            public List<Record> read(long fromOffset, int maxBytes) {
                return List.of(new Record(0, "k".getBytes(), "v".getBytes()));
            }

            @Override
            public long highWatermark() {
                return 1;
            }

            @Override
            public void flush() {
            }

            @Override
            public void close() {
            }
        };
        FetchTransferMetrics metrics = new FetchTransferMetrics();
        FetchResponseWriter writer = new FetchResponseWriter(
                FetchTransferMode.AUTO, 30_000, metrics, channel -> true);
        ChannelHandlerContext ctx = captureContext(new EmbeddedChannel());
        EmbeddedChannel channel = (EmbeddedChannel) ctx.channel();
        try {
            assertTrue(writer.write(ctx, new MqResponseHeader(1, ErrorCode.NONE),
                    new FetchRequest("orders", 0, 0, 1_024), stub));
            channel.runPendingTasks();
            Object outbound = channel.readOutbound();
            try {
                assertInstanceOf(ByteBuf.class, outbound);
                assertEquals(1, metrics.snapshot().heapResponses());
                assertEquals(1, metrics.snapshot().unsupportedLogFallbacks());
            } finally {
                ReferenceCountUtil.release(outbound);
            }
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    void unsupportedPipelineFallsBackToHeap(@TempDir Path dir) throws Exception {
        try (SegmentedPartitionLog log = new SegmentedPartitionLog(dir, new SegmentedLogConfig(1_024, 1))) {
            log.append("k".getBytes(), "v".getBytes());
            FetchTransferMetrics metrics = new FetchTransferMetrics();
            FetchResponseWriter writer = new FetchResponseWriter(
                    FetchTransferMode.AUTO, 30_000, metrics, channel -> false);
            ChannelHandlerContext ctx = captureContext(new EmbeddedChannel());
            EmbeddedChannel channel = (EmbeddedChannel) ctx.channel();
            try {
                assertTrue(writer.write(ctx, new MqResponseHeader(1, ErrorCode.NONE),
                        new FetchRequest("orders", 0, 0, 1_024), log));
                channel.runPendingTasks();
                Object outbound = channel.readOutbound();
                try {
                    assertInstanceOf(ByteBuf.class, outbound);
                    assertEquals(1, metrics.snapshot().unsupportedPipelineFallbacks());
                } finally {
                    ReferenceCountUtil.release(outbound);
                }
            } finally {
                channel.finishAndReleaseAll();
            }
        }
    }

    @Test
    void timeoutReleasesReaderLeaseAndClosesChannel(@TempDir Path dir) throws Exception {
        try (SegmentedPartitionLog log = new SegmentedPartitionLog(dir, new SegmentedLogConfig(1_024, 1))) {
            log.append("k".getBytes(), "value".getBytes());
            FetchTransferMetrics metrics = new FetchTransferMetrics();
            FetchResponseWriter writer = new FetchResponseWriter(
                    FetchTransferMode.AUTO, 1, metrics, channel -> true);

            AtomicReference<ChannelPromise> heldPromise = new AtomicReference<>();
            EmbeddedChannel channel = new EmbeddedChannel(new ChannelOutboundHandlerAdapter() {
                @Override
                public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
                    if (msg instanceof DefaultFileRegion) {
                        heldPromise.set(promise);
                        return;
                    }
                    ctx.write(msg, promise);
                }
            });
            ChannelHandlerContext ctx = captureContext(channel);

            assertTrue(writer.write(ctx, new MqResponseHeader(1, ErrorCode.NONE),
                    new FetchRequest("orders", 0, 0, 1_024), log));
            channel.runPendingTasks();

            ExecutorService executor = Executors.newSingleThreadExecutor();
            try {
                Future<?> closeFuture = executor.submit(log::close);
                assertFalse(closeFuture.isDone());
                channel.advanceTimeBy(10, TimeUnit.MILLISECONDS);
                channel.runScheduledPendingTasks();
                channel.runPendingTasks();

                assertFalse(channel.isActive());
                assertEquals(1, metrics.snapshot().transferTimeouts());
                assertDoesNotThrow(() -> closeFuture.get(1, TimeUnit.SECONDS));
            } finally {
                ChannelPromise promise = heldPromise.get();
                if (promise != null && !promise.isDone()) {
                    promise.tryFailure(new RuntimeException("cancelled"));
                }
                channel.finishAndReleaseAll();
                executor.shutdownNow();
            }
        }
    }

    private static ChannelHandlerContext captureContext(EmbeddedChannel channel) {
        AtomicReference<ChannelHandlerContext> ctxRef = new AtomicReference<>();
        channel.pipeline().addLast(new ChannelInboundHandlerAdapter() {
            @Override
            public void handlerAdded(ChannelHandlerContext ctx) {
                ctxRef.set(ctx);
            }
        });
        return ctxRef.get();
    }
}
