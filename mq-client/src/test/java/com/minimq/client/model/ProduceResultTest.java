package com.minimq.client.model;

import com.minimq.client.Producer;
import com.minimq.client.ProducerConfig;
import com.minimq.client.exception.MiniMqClientException;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ProduceResultTest {

    @Test
    void completedResultReturnsAssignedOffset() {
        ProduceResult result = new ProduceResult("orders", 2, 9L);
        assertEquals("orders", result.topic());
        assertEquals(2, result.partitionId());
        assertEquals(9L, result.offset());
        assertEquals("orders", result.getTopic());
        assertEquals(2, result.getPartitionId());
        assertEquals(9L, result.getOffset());
    }

    @Test
    void pendingResultUnwrapsNonRuntimeFailures() throws Exception {
        try (Producer producer = new Producer("127.0.0.1:1", "produce-result", new ProducerConfig(1, 0))) {
            CompletableFuture<Long> pending = new CompletableFuture<>();
            pending.completeExceptionally(new Exception("boom"));
            ProduceResult result = new ProduceResult("orders", 0, pending, producer);
            MiniMqClientException ex = assertThrows(MiniMqClientException.class, result::offset);
            assertEquals("boom", ex.getMessage());
        }
    }
}
