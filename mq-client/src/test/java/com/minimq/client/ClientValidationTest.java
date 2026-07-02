package com.minimq.client;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ClientValidationTest {

    @Test
    void rejectsInvalidBootstrapServers() {
        assertThrows(IllegalArgumentException.class, () -> new Producer("localhost", "p1"));
        assertThrows(IllegalArgumentException.class, () -> new Consumer("localhost", "g1", "c1"));
    }

    @Test
    void networkClientCanCloseWithoutConnecting() {
        assertDoesNotThrow(() -> {
            try (NetworkClient client = new NetworkClient("client")) {
                // no-op
            }
        });
    }
}
