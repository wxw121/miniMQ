package com.minimq.client;

/**
 * Client-side produce batching. Sequential {@code send(record)} calls that do not
 * immediately read {@link com.minimq.client.model.ProduceResult#offset()} are
 * grouped into one Produce request per partition.
 */
public record ProducerConfig(int batchSize, long lingerMs) {
    public static final int DEFAULT_BATCH_SIZE = 50;
    public static final long DEFAULT_LINGER_MS = 5L;

    public ProducerConfig {
        if (batchSize < 1) {
            throw new IllegalArgumentException("batchSize must be positive");
        }
        if (lingerMs < 0) {
            throw new IllegalArgumentException("lingerMs must not be negative");
        }
    }

    public static ProducerConfig defaults() {
        return new ProducerConfig(DEFAULT_BATCH_SIZE, DEFAULT_LINGER_MS);
    }
}
