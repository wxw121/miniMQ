package com.minimq.broker.support;

import com.minimq.broker.BrokerConfig;
import com.minimq.broker.BrokerServer;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;

public final class BrokerTestHarness implements AutoCloseable {
    private final BrokerServer server;
    private final BrokerConfig config;

    private BrokerTestHarness(BrokerServer server, BrokerConfig config) {
        this.server = server;
        this.config = config;
    }

    public static BrokerTestHarness startController(Path dataDir) throws Exception {
        BrokerConfig config = controllerConfig(0, dataDir);
        BrokerServer server = new BrokerServer(config);
        server.start();
        Thread.sleep(300);
        return new BrokerTestHarness(server, config);
    }

    public static BrokerConfig controllerConfig(int brokerId, Path dataDir) throws IOException {
        int port = freePort();
        return new BrokerConfig(
                brokerId, "127.0.0.1", port, dataDir, true, "127.0.0.1", port);
    }

    public static BrokerTestHarness startFollower(
            int brokerId, Path dataDir, String controllerHost, int controllerPort) throws Exception {
        int port = freePort();
        BrokerConfig config = new BrokerConfig(
                brokerId, "127.0.0.1", port, dataDir, false, controllerHost, controllerPort);
        BrokerServer server = new BrokerServer(config);
        server.start();
        Thread.sleep(200);
        return new BrokerTestHarness(server, config);
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
