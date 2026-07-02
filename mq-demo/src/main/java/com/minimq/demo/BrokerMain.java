package com.minimq.demo;

import com.minimq.broker.BrokerConfig;
import com.minimq.broker.BrokerServer;

/**
 * Broker 命令行启动入口，解析参数后组装 BrokerConfig 并启动服务。
 */
public final class BrokerMain {
    public static void main(String[] args) throws Exception {
        BrokerConfig config = parseConfig(args);
        BrokerServer server = new BrokerServer(config);
        server.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                server.stop();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }));
        Thread.currentThread().join();
    }

    static BrokerConfig parseConfig(String[] args) {
        return BrokerConfig.parse(args);
    }
}
