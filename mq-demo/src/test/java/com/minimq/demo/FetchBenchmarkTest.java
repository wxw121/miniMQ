package com.minimq.demo;

import com.minimq.broker.FetchTransferMetricsSnapshot;
import com.minimq.broker.FetchTransferMode;
import com.minimq.broker.netty.NetworkTransportMode;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
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
                "--segment-bytes", "32768",
                "--network-transport", "nio"
        });
        assertEquals(64, config.records());
        assertEquals(4_096, config.valueBytes());
        assertEquals(65_536, config.fetchBytes());
        assertEquals(2, config.warmup());
        assertEquals(5, config.iterations());
        assertEquals(32_768, config.segmentBytes());
        assertEquals(NetworkTransportMode.NIO, config.networkTransportMode());
    }

    @Test
    void rejectsNonPositiveAndOversizedFetchBudget() {
        assertThrows(IllegalArgumentException.class, () -> FetchBenchmark.Config.parse(new String[] {
                "benchmark-fetch", "--records", "0"
        }));
        assertThrows(IllegalArgumentException.class, () -> FetchBenchmark.Config.parse(new String[] {
                "benchmark-fetch", "--warmup", "-1"
        }));
        assertThrows(IllegalArgumentException.class, () -> FetchBenchmark.Config.parse(new String[] {
                "benchmark-fetch", "--fetch-bytes", String.valueOf(16 * 1024 * 1024)
        }));
        assertThrows(IllegalArgumentException.class, () -> FetchBenchmark.Config.parse(new String[] {
                "benchmark-fetch", "--network-transport", "kqueue"
        }));
    }

    @Test
    void summarizesLatencyPercentiles() {
        FetchBenchmark.Result result = FetchBenchmark.Result.from(
                "heap", 2_048, new long[] {10, 20, 30, 40, 50},
                new FetchTransferMetricsSnapshot(0, 0, 5, 5, 0, 0, 0, 0, 0));
        assertEquals(30, result.p50Nanos());
        assertEquals(50, result.p95Nanos());
        assertEquals("heap", result.mode());
        assertEquals(30, result.meanNanos());
        assertTrue(result.mibPerSecond() > 0);
    }

    @Test
    void runsTinyHeapAndAutoBenchmarkModes() throws Exception {
        FetchBenchmark.Config config = new FetchBenchmark.Config(
                2, 64, 4_096, 0, 1, 128, NetworkTransportMode.NIO);
        FetchBenchmark.Result heap = FetchBenchmark.runMode(config, FetchTransferMode.HEAP);
        FetchBenchmark.Result auto = FetchBenchmark.runMode(config, FetchTransferMode.AUTO);

        assertEquals("heap", heap.mode());
        assertEquals("auto", auto.mode());
        assertTrue(heap.fetchedBytes() > 0);
        assertTrue(auto.fetchedBytes() > 0);
        assertEquals(1, heap.metrics().heapModeFallbacks());
        assertEquals(1, auto.metrics().sendfileResponses());
        assertTrue(auto.metrics().sendfileBytes() > 0);
    }

    @Test
    void printsHeapAndAutoRows() throws Exception {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        FetchBenchmark.run(new String[] {
                "benchmark-fetch",
                "--records", "2",
                "--value-bytes", "64",
                "--fetch-bytes", "4096",
                "--warmup", "0",
                "--iterations", "1",
                "--segment-bytes", "128",
                "--network-transport", "nio"
        }, new PrintStream(buffer, true, StandardCharsets.UTF_8));
        String output = buffer.toString(StandardCharsets.UTF_8);
        assertTrue(output.contains("networkTransport=nio"));
        assertTrue(output.contains("mode requests bytes"));
        assertTrue(output.contains("heap "));
        assertTrue(output.contains("auto "));
    }
}
