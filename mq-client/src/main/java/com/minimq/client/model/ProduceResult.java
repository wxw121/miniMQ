package com.minimq.client.model;

import com.minimq.client.Producer;
import com.minimq.client.exception.MiniMqClientException;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * 生产请求成功后的返回结果。{@link #offset()} 在记录仍在客户端批次中时会先 flush 再等待 broker。
 */
public final class ProduceResult {
    private final String topic;
    private final int partitionId;
    private final long offset;
    private final CompletableFuture<Long> pendingOffset;
    private final Producer producer;

    public ProduceResult(String topic, int partitionId, long offset) {
        this.topic = Objects.requireNonNull(topic, "topic");
        this.partitionId = partitionId;
        this.offset = offset;
        this.pendingOffset = null;
        this.producer = null;
    }

    public ProduceResult(
            String topic,
            int partitionId,
            CompletableFuture<Long> pendingOffset,
            Producer producer) {
        this.topic = Objects.requireNonNull(topic, "topic");
        this.partitionId = partitionId;
        this.offset = Long.MIN_VALUE;
        this.pendingOffset = Objects.requireNonNull(pendingOffset, "pendingOffset");
        this.producer = Objects.requireNonNull(producer, "producer");
    }

    public String topic() {
        return topic;
    }

    public int partitionId() {
        return partitionId;
    }

    public long offset() {
        if (pendingOffset == null) {
            return offset;
        }
        producer.flushQuietly();
        try {
            return pendingOffset.get();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new MiniMqClientException("Interrupted waiting for produce offset", interrupted);
        } catch (ExecutionException failure) {
            throw unwrap(failure);
        }
    }

    public String getTopic() {
        return topic();
    }

    public int getPartitionId() {
        return partitionId();
    }

    public long getOffset() {
        return offset();
    }

    private static RuntimeException unwrap(ExecutionException failure) {
        Throwable cause = failure.getCause();
        if (cause instanceof RuntimeException runtime) {
            return runtime;
        }
        if (cause instanceof Error error) {
            throw error;
        }
        return new MiniMqClientException(
                cause != null ? cause.getMessage() : "produce failed", cause);
    }
}
