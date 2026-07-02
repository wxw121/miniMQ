package com.minimq.demo;

import com.minimq.broker.BrokerConfig;
import com.minimq.broker.BrokerServer;

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
