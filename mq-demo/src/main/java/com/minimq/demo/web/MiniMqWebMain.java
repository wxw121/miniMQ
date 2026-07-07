package com.minimq.demo.web;

import java.util.concurrent.CountDownLatch;

/**
 * MiniMQ Web 前端启动入口，默认监听 18080，并连接本地 9092 Broker。
 */
public final class MiniMqWebMain {
    private MiniMqWebMain() {
    }

    public static void main(String[] args) throws Exception {
        int port = Integer.parseInt(System.getProperty("minimq.web.port", "18080"));
        String bootstrapServers = System.getProperty("minimq.bootstrap", "127.0.0.1:9092");
        MiniMqWebApp app = MiniMqWebApp.start(port, bootstrapServers);
        System.out.printf("MiniMQ Web started at http://127.0.0.1:%d%n", app.port());
        Runtime.getRuntime().addShutdownHook(new Thread(app::close));
        new CountDownLatch(1).await();
    }
}
