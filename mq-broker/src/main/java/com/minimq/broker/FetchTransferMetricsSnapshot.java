package com.minimq.broker;

public record FetchTransferMetricsSnapshot(
        long sendfileResponses,
        long sendfileBytes,
        long heapResponses,
        long heapModeFallbacks,
        long unsupportedLogFallbacks,
        long unsupportedPipelineFallbacks,
        long tlsFallbacks,
        long transferFailures,
        long transferTimeouts) {
}
