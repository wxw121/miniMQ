package com.minimq.broker.support;

import com.minimq.broker.BrokerConfig;
import com.minimq.broker.BrokerServer;
import com.minimq.broker.FetchTransferMetricsSnapshot;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public final class BrokerTestHarness implements AutoCloseable {
    private final BrokerServer server;
    private final BrokerConfig config;

    private BrokerTestHarness(BrokerServer server, BrokerConfig config) {
        this.server = server;
        this.config = config;
    }

    public static BrokerTestHarness start(BrokerConfig config) throws Exception {
        BrokerServer server = new BrokerServer(config);
        server.start();
        Thread.sleep(300);
        return new BrokerTestHarness(server, config);
    }

    public static BrokerTestHarness startController(Path dataDir) throws Exception {
        return start(controllerConfig(0, dataDir));
    }

    public static BrokerConfig controllerConfig(int brokerId, Path dataDir) throws IOException {
        return controllerConfig(brokerId, dataDir, new String[0]);
    }

    public static BrokerConfig controllerConfig(int brokerId, Path dataDir, String... extraArgs) throws IOException {
        List<String> args = new ArrayList<>(List.of(
                "--broker-id", Integer.toString(brokerId),
                "--host", "127.0.0.1",
                "--port", Integer.toString(freePort()),
                "--data-dir", dataDir.toString(),
                "--controller"));
        args.addAll(List.of(extraArgs));
        return BrokerConfig.parse(args.toArray(String[]::new));
    }

    public static BrokerTestHarness startFollower(
            int brokerId, Path dataDir, String controllerHost, int controllerPort) throws Exception {
        int port = freePort();
        BrokerConfig config = new BrokerConfig(
                brokerId, "127.0.0.1", port, dataDir, false, controllerHost, controllerPort, false);
        return start(config);
    }

    public String bootstrapServers() {
        return "127.0.0.1:" + config.port();
    }

    public int port() {
        return config.port();
    }

    public int brokerId() {
        return config.brokerId();
    }

    public FetchTransferMetricsSnapshot fetchTransferMetrics() {
        return server.fetchTransferMetrics();
    }

    public long localHighWatermark(String topic, int partition) {
        return server.localHighWatermark(topic, partition);
    }

    @Override
    public void close() throws Exception {
        server.stop();
    }

    public static Path newDataDir(String name) throws IOException {
        return Files.createTempDirectory("minimq-" + name + "-");
    }

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}
