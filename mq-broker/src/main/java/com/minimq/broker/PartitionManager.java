package com.minimq.broker;

import com.minimq.storage.LogMaintenanceMetrics;
import com.minimq.storage.LogMaintenanceReport;
import com.minimq.storage.LogManager;
import com.minimq.storage.PartitionLog;

import java.util.List;

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

    public long append(String topic, int partitionId, byte[] key, byte[] value, long deliverAtEpochMs) {
        return logManager.append(topic, partitionId, key, value, deliverAtEpochMs);
    }

    public long appendBatch(String topic, int partitionId, List<LogManager.AppendRecord> records) {
        return logManager.appendBatch(topic, partitionId, records);
    }

    public void replicateAt(String topic, int partitionId, long offset, byte[] key, byte[] value) {
        logManager.replicateAt(topic, partitionId, offset, key, value);
    }

    public void replicateAt(
            String topic, int partitionId, long offset, byte[] key, byte[] value, long deliverAtEpochMs) {
        logManager.replicateAt(topic, partitionId, offset, key, value, deliverAtEpochMs);
    }

    public long highWatermark(String topic, int partitionId) {
        return logManager.getOrCreate(topic, partitionId).highWatermark();
    }

    public long visibleHighWatermark(String topic, int partitionId) {
        return logManager.visibleHighWatermark(topic, partitionId);
    }

    public long nextVisibleAtEpochMs(String topic, int partitionId) {
        return logManager.nextVisibleAtEpochMs(topic, partitionId);
    }

    public LogManager.VisibilitySnapshot visibilitySnapshot(String topic, int partitionId) {
        return logManager.visibilitySnapshot(topic, partitionId);
    }

    public long deliveryAtEpochMs(String topic, int partitionId, long offset) {
        return logManager.deliveryAtEpochMs(topic, partitionId, offset);
    }

    public void truncateTo(String topic, int partitionId, long offset) {
        logManager.truncateTo(topic, partitionId, offset);
    }

    public void checkpoint() {
        logManager.checkpoint();
    }

    public void deleteTopic(String topic, long tombstoneVersion) {
        logManager.deleteTopic(topic, tombstoneVersion);
    }

    public LogMaintenanceReport dryRunMaintenance() {
        return logManager.dryRunMaintenance();
    }

    public LogMaintenanceReport maintain() {
        return logManager.maintain();
    }

    public LogMaintenanceMetrics maintenanceMetrics() {
        return logManager.maintenanceMetrics();
    }

    public void close() {
        logManager.close();
    }
}
