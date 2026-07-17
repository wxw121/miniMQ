package com.minimq.broker;

import java.util.concurrent.atomic.LongAdder;

public final class FetchTransferMetrics {
    private final LongAdder sendfileResponses = new LongAdder();
    private final LongAdder sendfileBytes = new LongAdder();
    private final LongAdder heapResponses = new LongAdder();
    private final LongAdder heapModeFallbacks = new LongAdder();
    private final LongAdder unsupportedLogFallbacks = new LongAdder();
    private final LongAdder unsupportedPipelineFallbacks = new LongAdder();
    private final LongAdder tlsFallbacks = new LongAdder();
    private final LongAdder transferFailures = new LongAdder();
    private final LongAdder transferTimeouts = new LongAdder();

    public void recordSendfile(long bytes) {
        if (bytes < 0) {
            throw new IllegalArgumentException("sendfile bytes must be non-negative");
        }
        sendfileResponses.increment();
        sendfileBytes.add(bytes);
    }

    public void recordHeapMode() {
        heapResponses.increment();
        heapModeFallbacks.increment();
    }

    public void recordUnsupportedLog() {
        heapResponses.increment();
        unsupportedLogFallbacks.increment();
    }

    public void recordUnsupportedPipeline() {
        heapResponses.increment();
        unsupportedPipelineFallbacks.increment();
    }

    public void recordTlsFallback() {
        heapResponses.increment();
        tlsFallbacks.increment();
    }

    public void recordFailure() {
        transferFailures.increment();
    }

    public void recordTimeout() {
        transferTimeouts.increment();
    }

    public FetchTransferMetricsSnapshot snapshot() {
        return new FetchTransferMetricsSnapshot(
                sendfileResponses.sum(),
                sendfileBytes.sum(),
                heapResponses.sum(),
                heapModeFallbacks.sum(),
                unsupportedLogFallbacks.sum(),
                unsupportedPipelineFallbacks.sum(),
                tlsFallbacks.sum(),
                transferFailures.sum(),
                transferTimeouts.sum());
    }
}
