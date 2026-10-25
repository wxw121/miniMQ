package com.minimq.client;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ProducerConfigTest {

    @Test
    void defaultsMatchDocumentedBatchAndLinger() {
        ProducerConfig config = ProducerConfig.defaults();
        assertEquals(50, config.batchSize());
        assertEquals(5L, config.lingerMs());
    }

    @Test
    void rejectsNonPositiveBatchSizeAndNegativeLinger() {
        assertThrows(IllegalArgumentException.class, () -> new ProducerConfig(0, 0));
        assertThrows(IllegalArgumentException.class, () -> new ProducerConfig(-1, 5));
        assertThrows(IllegalArgumentException.class, () -> new ProducerConfig(1, -1));
    }
}
