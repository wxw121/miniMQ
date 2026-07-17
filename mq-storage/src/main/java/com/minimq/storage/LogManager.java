package com.minimq.storage;

import java.io.IOException;
import java.time.Clock;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
    private final Set<Path> pendingLegacyCleanup = new HashSet<>();
    private long successfulAppends;
    private long lastFlushEpochMs;
    private long maintenanceRuns;
    private long maintenanceDryRuns;
    private long maintenanceDeletedBytes;
    private long maintenanceDeletedSegments;
    private long maintenanceCompactedBytes;
    private long maintenanceCompactedSegments;
    private long maintenanceSkippedDueToReaders;
    private long maintenanceCleanupFailures;
    private MaintenanceStatus lastMaintenanceStatus = MaintenanceStatus.NEVER_RUN;
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
        String partitionKey = topic + "-" + partitionId;
        return logs.computeIfAbsent(partitionKey, this::openPartitionLog);
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
        maintenanceAfterAppendIfNeeded();
        return offset;
    }

    public synchronized void replicateAt(String topic, int partitionId, long offset, byte[] key, byte[] value) {
        ensureOpen();
        PartitionLog log = getOrCreate(topic, partitionId);
        log.appendAt(offset, key, value);
    }

    public synchronized LogMaintenanceReport dryRunMaintenance() {
        ensureOpen();
        if (!config.maintenanceConfig().enabled()) {
            LogMaintenanceReport report = LogMaintenanceReport.skipped(true, MaintenanceStatus.SKIPPED_DISABLED);
            recordDryRun();
            return report;
        }
        List<LogMaintenanceReport> reports = new ArrayList<>();
        for (Map.Entry<String, PartitionLog> entry : logs.entrySet()) {
            if (entry.getValue() instanceof SegmentedPartitionLog segmented) {
                reports.add(segmented.previewMaintenance(
                        entry.getKey(),
                        config.maintenanceConfig().retentionPolicy(),
                        config.maintenanceConfig().compactionPolicy(),
                        clock.millis()));
            }
        }
        LogMaintenanceReport report = aggregateReport(true, reports);
        recordDryRun();
        return report;
    }

    public synchronized LogMaintenanceReport maintain() {
        ensureOpen();
        if (!config.maintenanceConfig().enabled()) {
            LogMaintenanceReport report = LogMaintenanceReport.skipped(false, MaintenanceStatus.SKIPPED_DISABLED);
            lastMaintenanceStatus = report.status();
            return report;
        }
        maintenanceRuns++;
        try {
            List<LogMaintenanceReport> reports = new ArrayList<>();
            for (Map.Entry<String, PartitionLog> entry : logs.entrySet()) {
                if (entry.getValue() instanceof SegmentedPartitionLog segmented) {
                    reports.add(segmented.applyMaintenanceWithReport(
                            entry.getKey(),
                            config.maintenanceConfig().retentionPolicy(),
                            config.maintenanceConfig().compactionPolicy(),
                            clock.millis()));
                }
            }
            LogMaintenanceReport report = aggregateReport(false, reports);
            recordRun(report);
            saveCheckpoint();
            return report;
        } catch (RuntimeException failure) {
            maintenanceCleanupFailures++;
            lastMaintenanceStatus = MaintenanceStatus.FAILED;
            throw failure;
        }
    }

    public synchronized LogMaintenanceMetrics maintenanceMetrics() {
        return new LogMaintenanceMetrics(
                maintenanceRuns,
                maintenanceDryRuns,
                maintenanceDeletedBytes,
                maintenanceDeletedSegments,
                maintenanceCompactedBytes,
                maintenanceCompactedSegments,
                maintenanceSkippedDueToReaders,
                maintenanceCleanupFailures,
                lastMaintenanceStatus);
    }

    private LogMaintenanceReport aggregateReport(boolean dryRun, List<LogMaintenanceReport> reports) {
        List<MaintenanceSegmentReport> segmentReports = new ArrayList<>();
        long skippedDueToReaders = 0;
        long cleanupFailures = 0;
        for (LogMaintenanceReport report : reports) {
            segmentReports.addAll(report.segmentReports());
            skippedDueToReaders += report.skippedDueToReaders();
            cleanupFailures += report.cleanupFailures();
        }
        segmentReports.sort(Comparator
                .comparing(MaintenanceSegmentReport::partition)
                .thenComparingLong(MaintenanceSegmentReport::baseOffset)
                .thenComparing(report -> report.action().name()));
        boolean skippedReaders = reports.stream()
                .anyMatch(report -> report.status() == MaintenanceStatus.SKIPPED_READERS);
        MaintenanceStatus status = !segmentReports.isEmpty()
                ? MaintenanceStatus.SUCCESS
                : skippedReaders ? MaintenanceStatus.SKIPPED_READERS : MaintenanceStatus.NOOP;
        return new LogMaintenanceReport(dryRun, status, segmentReports, skippedDueToReaders, cleanupFailures);
    }

    private void recordDryRun() {
        maintenanceDryRuns++;
    }

    private void recordRun(LogMaintenanceReport report) {
        maintenanceDeletedBytes += report.deletedBytes();
        maintenanceDeletedSegments += report.deletedSegments();
        maintenanceCompactedBytes += report.compactedBytes();
        maintenanceCompactedSegments += report.compactedSegments();
        maintenanceSkippedDueToReaders += report.skippedDueToReaders();
        maintenanceCleanupFailures += report.cleanupFailures();
        lastMaintenanceStatus = report.status();
    }

    private void recordWindowSkip() {
        lastMaintenanceStatus = MaintenanceStatus.SKIPPED_WINDOW;
    }

    private void maintenanceAfterAppendIfNeeded() {
        if (config.maintenanceConfig().shouldRunAfterAppend(successfulAppends, clock)) {
            maintain();
        } else if (config.maintenanceConfig().shouldSkipAfterAppendForWindow(successfulAppends, clock)) {
            recordWindowSkip();
        }
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
        cleanupMigratedLegacyFiles();
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
        logs.forEach((partitionKey, log) -> highWatermarks.put(partitionKey, log.highWatermark()));
        return highWatermarks;
    }

    private PartitionLog openPartitionLog(String partitionKey) {
        Path segmentsDir = dataDir.resolve("segments").resolve(partitionKey);
        Path legacyFile = dataDir.resolve(partitionKey + ".log");
        try {
            if (!Files.exists(segmentsDir) && Files.exists(legacyFile)) {
                migrateLegacyLog(partitionKey, legacyFile, segmentsDir);
            }
            if (Files.exists(legacyFile)) {
                pendingLegacyCleanup.add(legacyFile);
            }
            return new SegmentedPartitionLog(segmentsDir, config.segmentedLogConfig());
        } catch (IOException e) {
            throw new IllegalStateException("Failed to open segmented log " + partitionKey, e);
        }
    }

    private void migrateLegacyLog(String partitionKey, Path legacyFile, Path segmentsDir) throws IOException {
        Path stagingDir = segmentsDir.resolveSibling(partitionKey + ".migrating");
        deleteRecursively(stagingDir);
        try (FilePartitionLog legacy = new FilePartitionLog(legacyFile);
             SegmentedPartitionLog staged = new SegmentedPartitionLog(stagingDir, config.segmentedLogConfig())) {
            long fromOffset = 0;
            while (true) {
                List<com.minimq.common.model.Record> records = legacy.read(fromOffset, 1024 * 1024);
                if (records.isEmpty()) {
                    break;
                }
                for (com.minimq.common.model.Record record : records) {
                    staged.appendAt(record.offset(), record.key(), record.value());
                    fromOffset = record.offset() + 1;
                }
            }
            staged.flush();
        }
        moveDirectory(stagingDir, segmentsDir);
        pendingLegacyCleanup.add(legacyFile);
    }

    private void cleanupMigratedLegacyFiles() {
        for (Path legacyFile : new ArrayList<>(pendingLegacyCleanup)) {
            try {
                Files.deleteIfExists(legacyFile);
                pendingLegacyCleanup.remove(legacyFile);
            } catch (IOException e) {
                throw new IllegalStateException("Failed to delete migrated legacy log " + legacyFile, e);
            }
        }
    }

    private void moveDirectory(Path source, Path target) throws IOException {
        try {
            Files.move(source, target, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException atomicFailure) {
            Files.move(source, target);
        }
    }

    private void deleteRecursively(Path path) throws IOException {
        if (!Files.exists(path)) {
            return;
        }
        try (java.util.stream.Stream<Path> paths = Files.walk(path)) {
            for (Path child : paths.sorted(Comparator.reverseOrder()).toList()) {
                Files.deleteIfExists(child);
            }
        }
    }

    private record PartitionRef(String topic, int partitionId) {
    }
}
