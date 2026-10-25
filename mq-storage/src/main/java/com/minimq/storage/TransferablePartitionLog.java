package com.minimq.storage;

public interface TransferablePartitionLog {
    FetchPlan planFetch(long fromOffset, int maxBytes);

    FetchPlan planFetch(long fromOffset, int maxBytes, long exclusiveHighWatermark);
}
