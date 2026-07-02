package com.minimq.client;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NetworkClientRetryTest {

    @Test
    void connectRetriesUntilAttemptBudgetIsExhausted() throws Exception {
        int unusedPort = freePort();

        long started = System.nanoTime();
        try (NetworkClient client = new NetworkClient(
                "retry-test", NetworkClient.RetryPolicy.fixedBackoff(3, 25))) {
            IllegalStateException ex = assertThrows(IllegalStateException.class,
                    () -> client.connect("127.0.0.1", unusedPort));

            assertTrue(ex.getMessage().contains("after 3 attempts"));
            assertTrue(client.connectionAttempts() >= 3);
        }

        long elapsedMillis = (System.nanoTime() - started) / 1_000_000;
        assertTrue(elapsedMillis >= 50, "expected retry backoff delay");
    }

    @Test
    void retryPolicyRejectsInvalidValues() {
        List<Runnable> invalidPolicies = List.of(
                () -> NetworkClient.RetryPolicy.fixedBackoff(0, 1),
                () -> NetworkClient.RetryPolicy.fixedBackoff(1, -1));

        for (Runnable invalidPolicy : invalidPolicies) {
            assertThrows(IllegalArgumentException.class, invalidPolicy::run);
        }
    }

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}
