package com.minimq.storage;

import java.io.IOException;
import java.time.Clock;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 日志管理器，按 TopicPartition 创建和缓存 FilePartitionLog。
 */
public class LogManager {
    private final Path dataDir;
    private final Map<String, PartitionLog> logs = new ConcurrentHashMap<>();
    private final WriteAheadLog writeAheadLog;
    private final CheckpointStore checkpointStore;
    private final LogManagerConfig config;
    private final Clock clock;
    private long successfulAppends;
    private long lastFlushEpochMs;
    private boolean closed;

    public LogManager(Path dataDir) {
        this(dataDir, LogManagerConfig.defaults());
    }

    public LogManager(Path dataDir, LogManagerConfig config) {
        this(dataDir, config, Clock.systemUTC());
    }

    LogManager(Path dataDir, LogManagerConfig config, Clock clock) {
        this.dataDir = dataDir;
        this.config = config;
        this.clock = clock;
        this.lastFlushEpochMs = clock.millis();
        try {
            this.writeAheadLog = new WriteAheadLog(dataDir.resolve("wal").resolve("minimq.wal"));
            this.checkpointStore = new CheckpointStore(dataDir);
            replayWal();
            saveCheckpoint();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to initialize log manager recovery", e);
        }
    }

    public PartitionLog getOrCreate(String topic, int partitionId) {
        ensureOpen();
        String name = topic + "-" + partitionId + ".log";
        return logs.computeIfAbsent(name, n -> {
            try {
                return new FilePartitionLog(dataDir.resolve(n), false);
            } catch (IOException e) {
                throw new IllegalStateException("Failed to open log " + n, e);
            }
        });
    }

    public synchronized long append(String topic, int partitionId, byte[] key, byte[] value) {
        ensureOpen();
        PartitionLog log = getOrCreate(topic, partitionId);
        long offset = log.highWatermark();
        writeAheadLog.append(WalRecord.dataAppend(topic, partitionId, offset, key, value), false);
        log.appendAt(offset, key, value);
        successfulAppends++;
        flushAfterAppendIfNeeded();
        checkpointAfterAppendIfNeeded();
        return offset;
    }

    public synchronized void checkpoint() {
        ensureOpen();
        saveCheckpoint();
    }

    public synchronized void close() {
        if (closed) {
            return;
        }
        saveCheckpoint();
        try {
            writeAheadLog.close();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to close WAL", e);
        } finally {
            logs.values().forEach(PartitionLog::close);
            closed = true;
        }
    }

    private void replayWal() {
        long position = checkpointStore.load()
                .filter(this::partitionLogsMatchCheckpoint)
                .map(checkpoint -> checkpoint.walPosition)
                .orElse(0L);
        List<WalRecord> records = writeAheadLog.readFrom(position);
        for (WalRecord record : records) {
            if (record.type() != WalRecordType.DATA_APPEND) {
                throw new IllegalStateException("Unsupported WAL replay type " + record.type());
            }
            PartitionLog log = getOrCreate(record.topic(), record.partitionId());
            log.appendAt(record.offset(), record.key(), record.value());
        }
    }

    private boolean partitionLogsMatchCheckpoint(RecoveryCheckpoint checkpoint) {
        for (Map.Entry<String, Long> entry : checkpoint.partitionHighWatermarks.entrySet()) {
            PartitionRef partitionRef = parsePartitionKey(entry.getKey());
            long actualHighWatermark = getOrCreate(partitionRef.topic(), partitionRef.partitionId()).highWatermark();
            if (actualHighWatermark < entry.getValue()) {
                return false;
            }
        }
        return true;
    }

    private PartitionRef parsePartitionKey(String partitionKey) {
        int separator = partitionKey.lastIndexOf('-');
        if (separator <= 0 || separator == partitionKey.length() - 1) {
            throw new IllegalStateException("Invalid checkpoint partition key " + partitionKey);
        }
        try {
            return new PartitionRef(
                    partitionKey.substring(0, separator),
                    Integer.parseInt(partitionKey.substring(separator + 1)));
        } catch (NumberFormatException e) {
            throw new IllegalStateException("Invalid checkpoint partition key " + partitionKey, e);
        }
    }

    private void saveCheckpoint() {
        flushAll();
        checkpointStore.save(checkpointStore.checkpoint(writeAheadLog.size(), highWatermarks()));
    }

    private void flushAfterAppendIfNeeded() {
        if (config.flushPolicy().shouldFlush(successfulAppends, clock.millis(), lastFlushEpochMs)) {
            flushAll();
        }
    }

    private void checkpointAfterAppendIfNeeded() {
        if (config.shouldCheckpointAfterAppend(successfulAppends)) {
            saveCheckpoint();
        }
    }

    private void flushAll() {
        writeAheadLog.flush();
        logs.values().forEach(PartitionLog::flush);
        lastFlushEpochMs = clock.millis();
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("LogManager is closed");
        }
    }

    private Map<String, Long> highWatermarks() {
        Map<String, Long> highWatermarks = new HashMap<>();
        logs.forEach((name, log) -> {
            String key = name.endsWith(".log") ? name.substring(0, name.length() - ".log".length()) : name;
            highWatermarks.put(key, log.highWatermark());
        });
        return highWatermarks;
    }

    private record PartitionRef(String topic, int partitionId) {
    }
}
