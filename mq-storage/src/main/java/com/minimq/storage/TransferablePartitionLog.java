package com.minimq.storage;

public interface TransferablePartitionLog {
    FetchPlan planFetch(long fromOffset, int maxBytes);
}
