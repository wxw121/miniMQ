package com.minimq.client;

import com.minimq.client.exception.MiniMqMetadataException;
import com.minimq.client.exception.MiniMqServerException;
import com.minimq.common.error.ErrorCode;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ClientValidationTest {

    @Test
    void rejectsInvalidBootstrapServers() {
        assertThrows(IllegalArgumentException.class, () -> new Producer("localhost", "p1"));
        assertThrows(IllegalArgumentException.class, () -> new Consumer("localhost", "g1", "c1"));
        assertThrows(IllegalArgumentException.class,
                () -> new Consumer("localhost:9092", "g1", "c1", 999));
    }

    @Test
    void networkClientCanCloseWithoutConnecting() {
        assertDoesNotThrow(() -> {
            try (NetworkClient client = new NetworkClient("client")) {
                // no-op
            }
        });
    }

    @Test
    void clientExceptionsExposeActionableContext() {
        MiniMqMetadataException metadata = new MiniMqMetadataException("Topic not found: orders");
        MiniMqServerException server = new MiniMqServerException(ErrorCode.UNKNOWN_PARTITION);

        assertEquals("Topic not found: orders", metadata.getMessage());
        assertEquals(ErrorCode.UNKNOWN_PARTITION, server.errorCode());
        assertEquals("Request failed: UNKNOWN_PARTITION", server.getMessage());
    }
}
