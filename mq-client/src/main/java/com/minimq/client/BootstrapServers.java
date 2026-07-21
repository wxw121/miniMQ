package com.minimq.client;

import java.util.ArrayList;
import java.util.List;

/** Validated, ordered bootstrap endpoints used for Controller discovery. */
public record BootstrapServers(List<Endpoint> endpoints) {
    public BootstrapServers {
        endpoints = List.copyOf(endpoints);
        if (endpoints.isEmpty()) {
            throw new IllegalArgumentException("bootstrapServers must not be empty");
        }
    }

    public static BootstrapServers parse(String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("bootstrapServers must not be empty");
        }
        List<Endpoint> endpoints = new ArrayList<>();
        for (String token : value.split(",")) {
            String[] parts = token.trim().split(":", -1);
            if (parts.length != 2 || parts[0].isBlank() || parts[1].isBlank()) {
                throw new IllegalArgumentException("bootstrapServers must contain host:port entries");
            }
            try {
                int port = Integer.parseInt(parts[1]);
                if (port < 1 || port > 65535) {
                    throw new IllegalArgumentException("bootstrap port out of range: " + port);
                }
                endpoints.add(new Endpoint(parts[0], port));
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("invalid bootstrap port: " + parts[1], e);
            }
        }
        return new BootstrapServers(endpoints);
    }

    public record Endpoint(String host, int port) {
    }
}
