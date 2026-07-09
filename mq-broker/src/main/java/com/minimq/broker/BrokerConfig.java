package com.minimq.broker;

import java.nio.file.Path;

/**
 * Broker 启动配置，集中保存 brokerId、监听地址、数据目录和 Controller 模式。
 */
public record BrokerConfig(
        int brokerId,
        String host,
        int port,
        Path dataDir,
        boolean controller,
        String controllerHost,
        int controllerPort,
        boolean recoverCorruptMetadata) {

    public static BrokerConfig parse(String[] args) {
        int brokerId = 0;
        String host = "localhost";
        int port = 9092;
        Path dataDir = Path.of("./data/broker-0");
        boolean controller = false;
        String controllerHost = "localhost";
        int controllerPort = 9092;
        boolean recoverCorruptMetadata = false;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--broker-id" -> brokerId = Integer.parseInt(value(args, ++i, "--broker-id"));
                case "--host" -> host = value(args, ++i, "--host");
                case "--port" -> port = Integer.parseInt(value(args, ++i, "--port"));
                case "--data-dir" -> dataDir = Path.of(value(args, ++i, "--data-dir"));
                case "--controller" -> controller = true;
                case "--recover-corrupt-metadata" -> recoverCorruptMetadata = true;
                case "--controller-addr" -> {
                    String[] parts = value(args, ++i, "--controller-addr").split(":", 2);
                    if (parts.length != 2 || parts[0].isBlank() || parts[1].isBlank()) {
                        throw new IllegalArgumentException("--controller-addr must be host:port");
                    }
                    controllerHost = parts[0];
                    controllerPort = Integer.parseInt(parts[1]);
                }
                default -> throw new IllegalArgumentException("Unknown argument: " + args[i]);
            }
        }
        return new BrokerConfig(
                brokerId,
                host,
                port,
                dataDir,
                controller,
                controllerHost,
                controllerPort,
                recoverCorruptMetadata);
    }

    private static String value(String[] args, int index, String name) {
        if (index >= args.length || args[index].startsWith("--")) {
            throw new IllegalArgumentException("Missing value for " + name);
        }
        return args[index];
    }
}
