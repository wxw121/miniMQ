package com.minimq.broker.handler;

import com.minimq.broker.BrokerConfig;
import com.minimq.broker.FetchTransferMetrics;
import com.minimq.broker.FetchTransferMode;
import com.minimq.common.model.Record;
import com.minimq.protocol.ProtocolSerdes;
import com.minimq.protocol.message.MqResponseHeader;
import com.minimq.protocol.message.ProtocolMessages.FetchRequest;
import com.minimq.protocol.message.ProtocolMessages.FetchResponse;
import com.minimq.protocol.message.ProtocolMessages.FetchedRecord;
import com.minimq.storage.FetchPlan;
import com.minimq.storage.FileRegionSlice;
import com.minimq.storage.PartitionLog;
import com.minimq.storage.TransferablePartitionLog;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.DefaultFileRegion;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.ScheduledFuture;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;

final class FetchResponseWriter {
    private final FetchTransferMode mode;
    private final long timeoutMs;
    private final FetchTransferMetrics metrics;
    private final Predicate<Channel> fileRegionSupported;

    FetchResponseWriter(BrokerConfig config, FetchTransferMetrics metrics) {
        this(config.fetchTransferMode(), config.fetchTransferTimeoutMs(), metrics,
                channel -> channel instanceof SocketChannel);
    }

    FetchResponseWriter(FetchTransferMode mode, long timeoutMs,
                        FetchTransferMetrics metrics,
                        Predicate<Channel> fileRegionSupported) {
        this.mode = mode;
        this.timeoutMs = timeoutMs;
        this.metrics = metrics;
        this.fileRegionSupported = fileRegionSupported;
    }

    boolean write(ChannelHandlerContext ctx, MqResponseHeader header,
                  FetchRequest request, PartitionLog log, long effectiveHighWatermark) {
        if (mode == FetchTransferMode.HEAP) {
            metrics.recordHeapMode();
            writeHeap(ctx, header, request, log, effectiveHighWatermark);
            return true;
        }
        if (!(log instanceof TransferablePartitionLog transferable)) {
            metrics.recordUnsupportedLog();
            writeHeap(ctx, header, request, log, effectiveHighWatermark);
            return true;
        }
        if (ctx.pipeline().get(SslHandler.class) != null) {
            metrics.recordTlsFallback();
            writeHeap(ctx, header, request, log, effectiveHighWatermark);
            return true;
        }
        if (!fileRegionSupported.test(ctx.channel())) {
            metrics.recordUnsupportedPipeline();
            writeHeap(ctx, header, request, log, effectiveHighWatermark);
            return true;
        }
        writeSendfile(ctx, header, request, transferable, effectiveHighWatermark);
        return true;
    }

    private void writeHeap(ChannelHandlerContext ctx, MqResponseHeader header,
                           FetchRequest request, PartitionLog log, long effectiveHighWatermark) {
        List<Record> records = log.read(request.fetchOffset(), request.maxBytes());
        List<FetchedRecord> fetched = new ArrayList<>(records.size());
        for (Record record : records) {
            fetched.add(new FetchedRecord(record.offset(), record.key(), record.value()));
        }
        FetchResponse response = new FetchResponse(
                request.topicName(), request.partitionId(), effectiveHighWatermark, fetched);
        ByteBuf frame = ProtocolSerdes.encodeResponse(header, response);
        ctx.writeAndFlush(frame);
    }

    private void writeSendfile(ChannelHandlerContext ctx, MqResponseHeader header,
                               FetchRequest request, TransferablePartitionLog transferable,
                               long effectiveHighWatermark) {
        FetchPlan plan = transferable.planFetch(request.fetchOffset(), request.maxBytes());
        TransferCleanup cleanup = new TransferCleanup(plan);
        List<DefaultFileRegion> regions = new ArrayList<>();
        try {
            for (FileRegionSlice slice : plan.slices()) {
                FileChannel channel = FileChannel.open(slice.path(), StandardOpenOption.READ);
                try {
                    regions.add(new DefaultFileRegion(channel, slice.position(), slice.length()));
                } catch (RuntimeException failure) {
                    try {
                        channel.close();
                    } catch (IOException ignored) {
                        // best-effort close before propagating the open/wrap failure
                    }
                    throw failure;
                }
            }

            ByteBuf prefix = ProtocolSerdes.encodeFetchResponsePrefix(
                    header,
                    request.topicName(),
                    request.partitionId(),
                    Math.min(plan.highWatermark(), effectiveHighWatermark),
                    plan.recordCount(),
                    plan.recordBytes());

            final ChannelFuture lastFuture;
            if (regions.isEmpty()) {
                lastFuture = ctx.writeAndFlush(prefix);
            } else {
                ctx.write(prefix);
                ChannelFuture flushFuture = null;
                for (int i = 0; i < regions.size(); i++) {
                    DefaultFileRegion region = regions.get(i);
                    if (i == regions.size() - 1) {
                        flushFuture = ctx.writeAndFlush(region);
                    } else {
                        ctx.write(region);
                    }
                }
                lastFuture = flushFuture;
            }

            ScheduledFuture<?> timeout = ctx.executor().schedule(() -> {
                if (!cleanup.finished()) {
                    metrics.recordTimeout();
                    ctx.close();
                    cleanup.run();
                }
            }, timeoutMs, TimeUnit.MILLISECONDS);

            lastFuture.addListener(future -> {
                timeout.cancel(false);
                if (future.isSuccess()) {
                    metrics.recordSendfile(plan.recordBytes());
                } else {
                    metrics.recordFailure();
                    ctx.close();
                }
                cleanup.run();
            });
        } catch (IOException | RuntimeException failure) {
            for (DefaultFileRegion region : regions) {
                ReferenceCountUtil.safeRelease(region);
            }
            cleanup.run();
            throw new IllegalStateException("Failed to write sendfile FETCH response", failure);
        }
    }

    private static final class TransferCleanup implements Runnable {
        private final FetchPlan plan;
        private final AtomicBoolean closed = new AtomicBoolean();

        private TransferCleanup(FetchPlan plan) {
            this.plan = plan;
        }

        boolean finished() {
            return closed.get();
        }

        @Override
        public void run() {
            if (closed.compareAndSet(false, true)) {
                plan.close();
            }
        }
    }
}
