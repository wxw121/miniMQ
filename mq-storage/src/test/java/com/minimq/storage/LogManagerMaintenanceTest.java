package com.minimq.storage;

import com.minimq.common.model.Record;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class LogManagerMaintenanceTest {

    @Test
    void appendCadenceRunsRetentionAndWritesCheckpoint(@TempDir Path dir) {
        LogMaintenanceConfig maintenance = LogMaintenanceConfig.of(
                RetentionPolicy.of(0, 1),
                CompactionPolicy.disabled(),
                1);
        LogManagerConfig config = new LogManagerConfig(
                FlushPolicy.always(),
                0,
                new SegmentedLogConfig(1, 1),
                maintenance);
        LogManager manager = new LogManager(dir, config);

        manager.append("orders", 0, "k0".getBytes(), "v0".getBytes());
        manager.append("orders", 0, "k1".getBytes(), "v1".getBytes());
        manager.close();

        LogManager recovered = new LogManager(dir, LogManagerConfig.defaults());
        try {
            assertEquals(List.of(1L), recovered.getOrCreate("orders", 0).read(0, 4_096)
                    .stream().map(Record::offset).toList());
        } finally {
            recovered.close();
        }
    }

    @Test
    void manualMaintenanceRunsCompaction(@TempDir Path dir) {
        LogMaintenanceConfig maintenance = LogMaintenanceConfig.of(
                RetentionPolicy.disabled(),
                CompactionPolicy.enabled(10),
                0);
        LogManager manager = new LogManager(dir, new LogManagerConfig(
                FlushPolicy.always(),
                0,
                new SegmentedLogConfig(1, 1),
                maintenance));

        manager.append("profiles", 0, "a".getBytes(), "old".getBytes());
        manager.append("profiles", 0, "a".getBytes(), "new".getBytes());
        manager.maintain();

        assertEquals(List.of(1L), manager.getOrCreate("profiles", 0).read(0, 4_096)
                .stream().map(Record::offset).toList());
    }

    @Test
    void walReplaySkipsRetainedOffsetsBelowLogStartOffset(@TempDir Path dir) {
        LogMaintenanceConfig maintenance = LogMaintenanceConfig.of(
                RetentionPolicy.of(0, 1),
                CompactionPolicy.disabled(),
                1);
        LogManagerConfig config = new LogManagerConfig(
                FlushPolicy.always(),
                0,
                new SegmentedLogConfig(1, 1),
                maintenance);
        LogManager manager = new LogManager(dir, config);
        manager.append("orders", 0, "k0".getBytes(), "v0".getBytes());
        manager.append("orders", 0, "k1".getBytes(), "v1".getBytes());
        manager.close();

        new CheckpointStore(dir).save(new RecoveryCheckpoint(0, Map.of("orders-0", 999L), 1));

        LogManager recovered = new LogManager(dir, LogManagerConfig.defaults());
        try {
            assertEquals(List.of(1L), recovered.getOrCreate("orders", 0).read(0, 4_096)
                    .stream().map(Record::offset).toList());
        } finally {
            recovered.close();
        }
    }
}
