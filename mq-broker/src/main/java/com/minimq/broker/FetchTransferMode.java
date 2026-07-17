package com.minimq.broker;

public enum FetchTransferMode {
    AUTO,
    HEAP;

    static FetchTransferMode parse(String value) {
        return switch (value) {
            case "auto" -> AUTO;
            case "heap" -> HEAP;
            default -> throw new IllegalArgumentException("Unknown fetch transfer mode: " + value);
        };
    }
}
