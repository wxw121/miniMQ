package com.minimq.broker.support;

import com.minimq.broker.BrokerConfig;
import com.minimq.broker.BrokerAvailabilityGuard;
import com.minimq.broker.BrokerServer;
import com.minimq.broker.FetchTransferMetricsSnapshot;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

public final class BrokerTestHarness implements AutoCloseable {
    public static final String ADMIN_TOKEN = "test-admin-token";

    private final BrokerServer server;
    private final BrokerConfig config;

    private BrokerTestHarness(BrokerServer server, BrokerConfig config) {
        this.server = server;
        this.config = config;
    }

    public static BrokerTestHarness start(BrokerConfig config) throws Exception {
        BrokerServer server = new BrokerServer(config);
        server.start();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (server.availabilityState() != BrokerAvailabilityGuard.State.ACTIVE
                && System.nanoTime() < deadline) {
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10));
        }
        if (server.availabilityState() != BrokerAvailabilityGuard.State.ACTIVE) {
            server.stop();
            throw new AssertionError("Broker " + config.brokerId()
                    + " did not become ACTIVE; final state=" + server.availabilityState());
        }
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
                "--controller",
                "--admin-token", ADMIN_TOKEN));
        args.addAll(List.of(extraArgs));
        return BrokerConfig.parse(args.toArray(String[]::new));
    }

    public static BrokerTestHarness startFollower(
            int brokerId, Path dataDir, String controllerHost, int controllerPort) throws Exception {
        return startFollower(brokerId, dataDir, controllerHost, controllerPort, new String[0]);
    }

    public static BrokerTestHarness startFollower(
            int brokerId,
            Path dataDir,
            String controllerHost,
            int controllerPort,
            String... extraArgs) throws Exception {
        List<String> args = new ArrayList<>(List.of(
                "--broker-id", Integer.toString(brokerId),
                "--host", "127.0.0.1",
                "--port", Integer.toString(freePort()),
                "--data-dir", dataDir.toString(),
                "--controller-addr", controllerHost + ":" + controllerPort,
                "--admin-token", ADMIN_TOKEN));
        args.addAll(List.of(extraArgs));
        return start(BrokerConfig.parse(args.toArray(String[]::new)));
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

    public BrokerAvailabilityGuard.State availabilityState() {
        return server.availabilityState();
    }

    public long brokerEpoch() {
        return server.brokerEpoch();
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
