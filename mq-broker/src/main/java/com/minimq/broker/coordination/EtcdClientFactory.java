package com.minimq.broker.coordination;

import io.etcd.jetcd.Client;

import java.net.URI;
import java.time.Duration;
import java.util.List;

public final class EtcdClientFactory {
    private EtcdClientFactory() {
    }

    public static Client create(List<String> endpoints) {
        if (endpoints == null || endpoints.isEmpty()) {
            throw new IllegalArgumentException("at least one etcd endpoint is required");
        }
        URI[] uris = endpoints.stream().map(URI::create).toArray(URI[]::new);
        return Client.builder()
                .endpoints(uris)
                .connectTimeout(Duration.ofSeconds(3))
                .keepaliveTime(Duration.ofSeconds(1))
                .keepaliveTimeout(Duration.ofSeconds(2))
                .keepaliveWithoutCalls(true)
                .build();
    }
}
