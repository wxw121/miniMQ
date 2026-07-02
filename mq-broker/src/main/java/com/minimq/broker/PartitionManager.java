package com.minimq.broker;

import com.minimq.storage.LogManager;
import com.minimq.storage.PartitionLog;

public class PartitionManager {
    private final LogManager logManager;

    public PartitionManager(LogManager logManager) {
        this.logManager = logManager;
    }

    public PartitionLog getOrCreate(String topic, int partitionId) {
        return logManager.getOrCreate(topic, partitionId);
    }
}
