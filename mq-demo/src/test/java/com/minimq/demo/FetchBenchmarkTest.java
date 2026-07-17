package com.minimq.demo;

import com.minimq.broker.FetchTransferMetricsSnapshot;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FetchBenchmarkTest {

    @Test
    void parsesBenchmarkArguments() {
        FetchBenchmark.Config config = FetchBenchmark.Config.parse(new String[] {
                "benchmark-fetch",
                "--records", "64",
                "--value-bytes", "4096",
                "--fetch-bytes", "65536",
                "--warmup", "2",
                "--iterations", "5",
                "--segment-bytes", "32768"
        });
        assertEquals(64, config.records());
        assertEquals(4_096, config.valueBytes());
        assertEquals(65_536, config.fetchBytes());
        assertEquals(2, config.warmup());
        assertEquals(5, config.iterations());
        assertEquals(32_768, config.segmentBytes());
    }

    @Test
    void summarizesLatencyPercentiles() {
        FetchBenchmark.Result result = FetchBenchmark.Result.from(
                "heap", 2_048, new long[] {10, 20, 30, 40, 50},
                new FetchTransferMetricsSnapshot(0, 0, 5, 5, 0, 0, 0, 0, 0));
        assertEquals(30, result.p50Nanos());
        assertEquals(50, result.p95Nanos());
        assertEquals("heap", result.mode());
    }
}
