package com.minimq.broker.netty;

import java.util.Locale;

public enum NetworkTransportMode {
    NIO,
    AUTO,
    EPOLL;

    public static NetworkTransportMode parse(String value) {
        try {
            return NetworkTransportMode.valueOf(value.trim().toUpperCase(Locale.ROOT));
        } catch (RuntimeException failure) {
            throw new IllegalArgumentException("Unknown network transport: " + value, failure);
        }
    }
}
