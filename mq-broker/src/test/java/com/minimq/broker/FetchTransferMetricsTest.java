package com.minimq.broker;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class FetchTransferMetricsTest {

    @Test
    void snapshotsTransferAndFallbackCounters() {
        FetchTransferMetrics metrics = new FetchTransferMetrics();
        metrics.recordSendfile(1_024);
        metrics.recordHeapMode();
        metrics.recordUnsupportedLog();
        metrics.recordUnsupportedPipeline();
        metrics.recordTlsFallback();
        metrics.recordFailure();
        metrics.recordTimeout();

        assertEquals(new FetchTransferMetricsSnapshot(
                1, 1_024, 4, 1, 1, 1, 1, 1, 1), metrics.snapshot());
    }
}
