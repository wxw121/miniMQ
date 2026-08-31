package com.minimq.broker.replication;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

final class ReplicaConnectionPool implements AutoCloseable {
    private static final long SHUTDOWN_TIMEOUT_SECONDS = 5;

    private final EventLoopGroup group = new NioEventLoopGroup(1);
    private final Map<LeaderKey, LeaderReplicaClient> clients = new HashMap<>();

    synchronized LeaderReplicaClient client(String host, int port) throws InterruptedException {
        LeaderKey key = new LeaderKey(host, port);
        LeaderReplicaClient existing = clients.get(key);
        if (existing != null) {
            return existing;
        }

        LeaderReplicaClient client = new LeaderReplicaClient(group, false);
        boolean connected = false;
        try {
            client.connect(host, port);
            connected = true;
            clients.put(key, client);
            return client;
        } finally {
            if (!connected) {
                client.close();
            }
        }
    }

    synchronized void invalidate(String host, int port) throws InterruptedException {
        LeaderReplicaClient client = clients.remove(new LeaderKey(host, port));
        if (client != null) {
            client.close();
        }
    }

    @Override
    public synchronized void close() throws InterruptedException {
        try {
            for (LeaderReplicaClient client : clients.values()) {
                client.close();
            }
            clients.clear();
        } finally {
            group.shutdownGracefully(0, SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .await(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        }
    }

    private record LeaderKey(String host, int port) {
    }
}
