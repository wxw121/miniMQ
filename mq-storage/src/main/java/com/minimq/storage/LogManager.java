package com.minimq.storage;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class LogManager {
    private final Path dataDir;
    private final Map<String, PartitionLog> logs = new ConcurrentHashMap<>();

    public LogManager(Path dataDir) {
        this.dataDir = dataDir;
    }

    public PartitionLog getOrCreate(String topic, int partitionId) {
        String name = topic + "-" + partitionId + ".log";
        return logs.computeIfAbsent(name, n -> {
            try {
                return new FilePartitionLog(dataDir.resolve(n));
            } catch (IOException e) {
                throw new IllegalStateException("Failed to open log " + n, e);
            }
        });
    }
}
