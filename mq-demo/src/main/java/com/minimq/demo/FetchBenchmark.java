package com.minimq.demo;

import com.minimq.broker.BrokerConfig;
import com.minimq.broker.BrokerServer;
import com.minimq.broker.FetchTransferMetricsSnapshot;
import com.minimq.broker.FetchTransferMode;
import com.minimq.broker.netty.NetworkTransportMode;
import com.minimq.client.NetworkClient;
import com.minimq.protocol.ApiKeys;
import com.minimq.protocol.MqFrame;
import com.minimq.protocol.codec.MqDecoder;
import com.minimq.protocol.message.ProtocolMessages.CreateTopicRequest;
import com.minimq.protocol.message.ProtocolMessages.FetchRequest;
import com.minimq.protocol.message.ProtocolMessages.FetchResponse;
import com.minimq.protocol.message.ProtocolMessages.FetchedRecord;
import com.minimq.protocol.message.ProtocolMessages.ProduceRecord;
import com.minimq.protocol.message.ProtocolMessages.ProduceRequest;

import java.io.IOException;
import java.io.PrintStream;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

final class FetchBenchmark {
    private FetchBenchmark() {
    }

    static void run(String[] args, PrintStream out) throws Exception {
        Config config = Config.parse(args);
        Result heap = runMode(config, FetchTransferMode.HEAP);
        Result auto = runMode(config, FetchTransferMode.AUTO);
        out.println("networkTransport=" + config.networkTransportMode().name().toLowerCase(Locale.ROOT));
        out.println("mode requests bytes MiB/s mean-ms p50-ms p95-ms sendfile-responses sendfile-bytes heap-responses");
        printRow(out, heap, config.iterations());
        printRow(out, auto, config.iterations());
    }

    private static void printRow(PrintStream out, Result result, int requests) {
        out.printf(Locale.ROOT,
                "%s %d %d %.3f %.3f %.3f %.3f %d %d %d%n",
                result.mode(),
                requests,
                result.fetchedBytes(),
                result.mibPerSecond(),
                result.meanNanos() / 1_000_000.0,
                result.p50Nanos() / 1_000_000.0,
                result.p95Nanos() / 1_000_000.0,
                result.metrics().sendfileResponses(),
                result.metrics().sendfileBytes(),
                result.metrics().heapResponses());
    }

    static Result runMode(Config config, FetchTransferMode mode) throws Exception {
        Path dataDir = Files.createTempDirectory("minimq-fetch-benchmark-");
        int port = freePort();
        BrokerConfig brokerConfig = BrokerConfig.parse(new String[] {
                "--broker-id", "0",
                "--host", "127.0.0.1",
                "--port", Integer.toString(port),
                "--data-dir", dataDir.toString(),
                "--flush-policy", "manual-on-close",
                "--sparse-index-interval-records", "1",
                "--segment-max-bytes", Long.toString(config.segmentBytes()),
                "--network-transport", config.networkTransportMode().name().toLowerCase(Locale.ROOT),
                "--fetch-transfer-mode", mode == FetchTransferMode.AUTO ? "auto" : "heap",
                "--fetch-transfer-timeout-ms", "30000",
                "--admin-token", "benchmark-admin"
        }).withLegacyController(true, "127.0.0.1", port);
        BrokerServer server = new BrokerServer(brokerConfig);
        server.start();
        try (NetworkClient client = new NetworkClient("fetch-benchmark")) {
            client.connect("127.0.0.1", port);
            client.ensureSuccess(client.send(ApiKeys.CREATE_TOPIC, new CreateTopicRequest("fetch-benchmark", 1)));

            int maxRecordsPerProduce = Math.max(1, MqFrame.MAX_FRAME_BYTES / Math.max(1, config.valueBytes() + 64) / 2);
            for (int offset = 0; offset < config.records(); ) {
                int batch = Math.min(maxRecordsPerProduce, config.records() - offset);
                List<ProduceRecord> records = new ArrayList<>(batch);
                for (int i = 0; i < batch; i++) {
                    int recordIndex = offset + i;
                    records.add(new ProduceRecord(
                            ("k-" + recordIndex).getBytes(),
                            deterministicValue(recordIndex, config.valueBytes())));
                }
                client.ensureSuccess(client.send(ApiKeys.PRODUCE, new ProduceRequest("fetch-benchmark", 0, records)));
                offset += batch;
            }

            for (int i = 0; i < config.warmup(); i++) {
                fetchOnce(client, config.fetchBytes());
            }

            long[] latencies = new long[config.iterations()];
            long fetchedBytes = 0;
            for (int i = 0; i < config.iterations(); i++) {
                long start = System.nanoTime();
                FetchResponse response = fetchOnce(client, config.fetchBytes());
                latencies[i] = System.nanoTime() - start;
                fetchedBytes += payloadBytes(response);
            }

            FetchTransferMetricsSnapshot metrics = server.fetchTransferMetrics();
            return Result.from(mode == FetchTransferMode.AUTO ? "auto" : "heap", fetchedBytes, latencies, metrics);
        } finally {
            server.stop();
        }
    }

    private static FetchResponse fetchOnce(NetworkClient client, int fetchBytes) throws InterruptedException {
        MqDecoder.DecodedResponse response = client.send(
                ApiKeys.FETCH, new FetchRequest("fetch-benchmark", 0, 0, fetchBytes));
        client.ensureSuccess(response);
        return (FetchResponse) response.body();
    }

    private static long payloadBytes(FetchResponse response) {
        long total = 0;
        for (FetchedRecord record : response.records()) {
            if (record.key() != null) {
                total += record.key().length;
            }
            if (record.value() != null) {
                total += record.value().length;
            }
        }
        return total;
    }

    private static byte[] deterministicValue(int recordIndex, int valueBytes) {
        byte[] value = new byte[valueBytes];
        for (int i = 0; i < valueBytes; i++) {
            value[i] = (byte) (recordIndex * 31 + i);
        }
        return value;
    }

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    record Config(int records, int valueBytes, int fetchBytes,
                  int warmup, int iterations, long segmentBytes,
                  NetworkTransportMode networkTransportMode) {
        static Config defaults() {
            return new Config(
                    256,
                    32 * 1024,
                    8 * 1024 * 1024,
                    10,
                    50,
                    1024 * 1024,
                    NetworkTransportMode.NIO);
        }

        static Config parse(String[] args) {
            Config defaults = defaults();
            int records = parsePositive(DemoCli.arg(args, "--records", Integer.toString(defaults.records())), "--records");
            int valueBytes = parsePositive(
                    DemoCli.arg(args, "--value-bytes", Integer.toString(defaults.valueBytes())), "--value-bytes");
            int fetchBytes = parsePositive(
                    DemoCli.arg(args, "--fetch-bytes", Integer.toString(defaults.fetchBytes())), "--fetch-bytes");
            int warmup = parseNonNegative(
                    DemoCli.arg(args, "--warmup", Integer.toString(defaults.warmup())), "--warmup");
            int iterations = parsePositive(
                    DemoCli.arg(args, "--iterations", Integer.toString(defaults.iterations())), "--iterations");
            long segmentBytes = parsePositiveLong(
                    DemoCli.arg(args, "--segment-bytes", Long.toString(defaults.segmentBytes())), "--segment-bytes");
            NetworkTransportMode networkTransportMode = NetworkTransportMode.parse(
                    DemoCli.arg(
                            args,
                            "--network-transport",
                            defaults.networkTransportMode().name().toLowerCase(Locale.ROOT)));
            if (fetchBytes >= MqFrame.MAX_FRAME_BYTES) {
                throw new IllegalArgumentException("--fetch-bytes must be below frame max");
            }
            return new Config(records, valueBytes, fetchBytes, warmup, iterations, segmentBytes, networkTransportMode);
        }

        private static int parsePositive(String value, String name) {
            int parsed = Integer.parseInt(value);
            if (parsed <= 0) {
                throw new IllegalArgumentException(name + " must be positive");
            }
            return parsed;
        }

        private static int parseNonNegative(String value, String name) {
            int parsed = Integer.parseInt(value);
            if (parsed < 0) {
                throw new IllegalArgumentException(name + " must be non-negative");
            }
            return parsed;
        }

        private static long parsePositiveLong(String value, String name) {
            long parsed = Long.parseLong(value);
            if (parsed <= 0) {
                throw new IllegalArgumentException(name + " must be positive");
            }
            return parsed;
        }
    }

    record Result(String mode, long fetchedBytes, double mibPerSecond,
                  long meanNanos, long p50Nanos, long p95Nanos,
                  FetchTransferMetricsSnapshot metrics) {
        static Result from(
                String mode,
                long fetchedBytes,
                long[] latencies,
                FetchTransferMetricsSnapshot metrics) {
            long[] sorted = latencies.clone();
            Arrays.sort(sorted);
            long totalNanos = 0;
            for (long latency : sorted) {
                totalNanos += latency;
            }
            long meanNanos = sorted.length == 0 ? 0 : totalNanos / sorted.length;
            long p50Nanos = percentile(sorted, 0.50);
            long p95Nanos = percentile(sorted, 0.95);
            double mibPerSecond = totalNanos <= 0
                    ? 0.0
                    : (fetchedBytes * 1_000_000_000.0) / totalNanos / (1024.0 * 1024.0);
            return new Result(mode, fetchedBytes, mibPerSecond, meanNanos, p50Nanos, p95Nanos, metrics);
        }

        private static long percentile(long[] sorted, double percentile) {
            if (sorted.length == 0) {
                return 0;
            }
            int index = Math.min(sorted.length - 1, Math.max(0, (int) Math.ceil(percentile * sorted.length) - 1));
            return sorted[index];
        }
    }
}
