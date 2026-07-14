package com.minimq.broker;

import com.minimq.storage.LogManager;
import com.minimq.storage.PartitionLog;

/**
 * 分区日志管理门面，按 TopicPartition 懒加载并复用本地 PartitionLog。
 */
public class PartitionManager {
    private final LogManager logManager;

    public PartitionManager(LogManager logManager) {
        this.logManager = logManager;
    }

    public PartitionLog getOrCreate(String topic, int partitionId) {
        return logManager.getOrCreate(topic, partitionId);
    }

    public long append(String topic, int partitionId, byte[] key, byte[] value) {
        return logManager.append(topic, partitionId, key, value);
    }

    public void checkpoint() {
        logManager.checkpoint();
    }

    public void close() {
        logManager.close();
    }
}
