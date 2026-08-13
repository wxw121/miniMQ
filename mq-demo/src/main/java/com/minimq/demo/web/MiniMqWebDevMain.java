package com.minimq.demo.web;

import com.minimq.broker.BrokerConfig;
import com.minimq.broker.BrokerServer;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;

/**
 * Local development launcher that keeps a broker and the Web frontend in one JVM.
 */
public final class MiniMqWebDevMain implements AutoCloseable {
    private final BrokerServer broker;
    private final MiniMqWebApp web;
    private final DevConfig config;
    private final int brokerPort;

    private MiniMqWebDevMain(BrokerServer broker, MiniMqWebApp web, DevConfig config, int brokerPort) {
        this.broker = broker;
        this.web = web;
        this.config = config;
        this.brokerPort = brokerPort;
    }

    public static void main(String[] args) throws Exception {
        DevConfig config = DevConfig.fromSystemProperties();
        MiniMqWebDevMain launcher = start(config);
        Runtime.getRuntime().addShutdownHook(new Thread(launcher::close));
        System.out.printf("MiniMQ broker started at %s:%d%n", launcher.config.host(), launcher.brokerPort());
        System.out.printf("MiniMQ Web started at http://127.0.0.1:%d/%n", launcher.webPort());
        new CountDownLatch(1).await();
    }

    static MiniMqWebDevMain start(DevConfig config) throws Exception {
        String adminToken = MiniMqWebApp.requiredAdminTokenProperty();
        Files.createDirectories(config.dataDir());
        int brokerPort = config.brokerPort() == 0 ? freePort() : config.brokerPort();
        BrokerConfig brokerConfig = new BrokerConfig(
                0,
                config.host(),
                brokerPort,
                config.dataDir(),
                true,
                config.host(),
                brokerPort,
                false,
                adminToken);
        BrokerServer broker = new BrokerServer(brokerConfig);
        broker.start();
        MiniMqWebApp web = MiniMqWebApp.start(
                config.webPort(), config.host() + ":" + brokerPort, adminToken);
        return new MiniMqWebDevMain(broker, web, config, brokerPort);
    }

    int webPort() {
        return web.port();
    }

    int brokerPort() {
        return brokerPort;
    }

    Path dataDir() {
        return config.dataDir();
    }

    @Override
    public void close() {
        web.close();
        try {
            broker.stop();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    record DevConfig(String host, int brokerPort, int webPort, Path dataDir) {
        static DevConfig fromSystemProperties() {
            return new DevConfig(
                    System.getProperty("minimq.host", "127.0.0.1"),
                    Integer.parseInt(System.getProperty("minimq.broker.port", "19092")),
                    Integer.parseInt(System.getProperty("minimq.web.port", "18082")),
                    Path.of(System.getProperty("minimq.data.dir", ".run/data-live")));
        }
    }
}
