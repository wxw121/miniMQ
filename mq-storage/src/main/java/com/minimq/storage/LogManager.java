package com.minimq.storage;

import com.minimq.common.validation.TopicNames;

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
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 日志管理器，按 TopicPartition 创建和缓存 FilePartitionLog。
 */
public class LogManager implements AutoCloseable {
    private final Path dataDir;
    private final Map<String, PartitionLog> logs = new ConcurrentHashMap<>();
    private final WriteAheadLog writeAheadLog;
    private final CheckpointStore checkpointStore;
    private final LogManagerConfig config;
    private final Clock clock;
    private final DeliverySchedule deliverySchedule;
    private final Set<Path> pendingLegacyCleanup = new HashSet<>();
    /** Names deleted by a durable tombstone stay blocked for this process. */
    private final Set<String> deletedTopics = ConcurrentHashMap.newKeySet();
    private final Map<String, String> topicStorageOwners = new ConcurrentHashMap<>();
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
        this.deliverySchedule = new DeliverySchedule(clock::millis);
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

    public synchronized PartitionLog getOrCreate(String topic, int partitionId) {
        ensureOpen();
        if (!TopicNames.isValidReference(topic) || partitionId < 0) {
            throw new IllegalArgumentException("invalid topic partition");
        }
        registerTopicStorageOwner(topic);
        if (deletedTopics.contains(topic)) {
            throw new IllegalStateException("Topic has been deleted: " + topic);
        }
        String partitionKey = topic + "-" + partitionId;
        return logs.computeIfAbsent(partitionKey, this::openPartitionLog);
    }

    /**
     * Crash-safe topic-wide local cleanup. Begin and commit are forced in
     * order; recovery completes an unmatched begin before serving data.
     */
    public synchronized void deleteTopic(String topic, long tombstoneVersion) {
        ensureOpen();
        if (!TopicNames.isValidReference(topic) || tombstoneVersion < 0) {
            throw new IllegalArgumentException("invalid topic deletion marker");
        }
        registerTopicStorageOwner(topic);
        if (deletedTopics.contains(topic)) {
            return;
        }
        writeAheadLog.append(WalRecord.topicDeleteBegin(topic, tombstoneVersion), true);
        deleteTopicFiles(topic);
        deletedTopics.add(topic);
        writeAheadLog.append(WalRecord.topicDeleteCommit(topic, tombstoneVersion), true);
        saveCheckpoint();
    }

    public synchronized long append(String topic, int partitionId, byte[] key, byte[] value) {
        return append(topic, partitionId, key, value, 0L);
    }

    public synchronized long append(
            String topic,
            int partitionId,
            byte[] key,
            byte[] value,
            long deliverAtEpochMs) {
        ensureOpen();
        if (deliverAtEpochMs < 0) {
            throw new IllegalArgumentException("delivery time must be non-negative");
        }
        long now = clock.millis();
        long persistedDeliverAtEpochMs = deliverAtEpochMs > now ? deliverAtEpochMs : 0L;
        if (persistedDeliverAtEpochMs > 0) {
            ensureScheduledCapacity(1);
        }
        PartitionLog log = getOrCreate(topic, partitionId);
        long offset = log.highWatermark();
        writeAheadLog.append(WalRecord.dataAppend(
                topic, partitionId, offset, key, value, persistedDeliverAtEpochMs), false);
        log.appendAt(offset, key, value);
        scheduleIfFuture(topic, partitionId, offset, persistedDeliverAtEpochMs, now);
        successfulAppends++;
        flushAfterAppendIfNeeded();
        checkpointAfterAppendIfNeeded();
        maintenanceAfterAppendIfNeeded();
        return offset;
    }

    public synchronized long appendBatch(String topic, int partitionId, List<AppendRecord> records) {
        ensureOpen();
        if (records == null || records.isEmpty()) {
            return -1L;
        }
        long now = clock.millis();
        int futureRecordCount = 0;
        for (AppendRecord record : records) {
            if (record.deliverAtEpochMs() < 0) {
                throw new IllegalArgumentException("delivery time must be non-negative");
            }
            if (record.deliverAtEpochMs() > now) {
                futureRecordCount++;
            }
        }
        ensureScheduledCapacity(futureRecordCount);
        PartitionLog log = getOrCreate(topic, partitionId);
        long baseOffset = log.highWatermark();
        for (int i = 0; i < records.size(); i++) {
            AppendRecord record = records.get(i);
            long offset = baseOffset + i;
            long persistedDeliverAtEpochMs =
                    record.deliverAtEpochMs() > now ? record.deliverAtEpochMs() : 0L;
            writeAheadLog.append(WalRecord.dataAppend(
                    topic,
                    partitionId,
                    offset,
                    record.key(),
                    record.value(),
                    persistedDeliverAtEpochMs), false);
            log.appendAt(offset, record.key(), record.value());
            scheduleIfFuture(topic, partitionId, offset, persistedDeliverAtEpochMs, now);
            successfulAppends++;
        }
        flushAfterAppendIfNeeded();
        checkpointAfterAppendIfNeeded();
        maintenanceAfterAppendIfNeeded();
        return baseOffset;
    }

    public synchronized void replicateAt(String topic, int partitionId, long offset, byte[] key, byte[] value) {
        replicateAt(topic, partitionId, offset, key, value, 0L);
    }

    public synchronized void replicateAt(
            String topic,
            int partitionId,
            long offset,
            byte[] key,
            byte[] value,
            long deliverAtEpochMs) {
        ensureOpen();
        if (deliverAtEpochMs < 0) {
            throw new IllegalArgumentException("delivery time must be non-negative");
        }
        long now = clock.millis();
        long persistedDeliverAtEpochMs = deliverAtEpochMs > now ? deliverAtEpochMs : 0L;
        if (persistedDeliverAtEpochMs > 0) {
            ensureScheduledCapacity(1);
        }
        PartitionLog log = getOrCreate(topic, partitionId);
        writeAheadLog.append(WalRecord.dataAppend(
                topic, partitionId, offset, key, value, persistedDeliverAtEpochMs), false);
        log.appendAt(offset, key, value);
        scheduleIfFuture(topic, partitionId, offset, persistedDeliverAtEpochMs, now);
        successfulAppends++;
        flushAfterAppendIfNeeded();
        checkpointAfterAppendIfNeeded();
        maintenanceAfterAppendIfNeeded();
    }

    public synchronized void truncateTo(String topic, int partitionId, long offset) {
        ensureOpen();
        PartitionLog log = getOrCreate(topic, partitionId);
        if (offset < 0 || offset > log.highWatermark()) {
            throw new IllegalArgumentException("truncate offset outside log range: " + offset);
        }
        if (offset == log.highWatermark()) {
            return;
        }
        writeAheadLog.append(WalRecord.truncateBegin(topic, partitionId, offset), true);
        log.truncateTo(offset);
        deliverySchedule.truncatePartition(topic, partitionId, offset);
        writeAheadLog.append(WalRecord.truncateCommit(topic, partitionId, offset), true);
        saveCheckpoint();
    }

    public synchronized long visibleHighWatermark(String topic, int partitionId) {
        return visibilitySnapshot(topic, partitionId).visibleHighWatermark();
    }

    public synchronized long nextVisibleAtEpochMs(String topic, int partitionId) {
        return visibilitySnapshot(topic, partitionId).nextVisibleAtEpochMs();
    }

    public synchronized VisibilitySnapshot visibilitySnapshot(String topic, int partitionId) {
        ensureOpen();
        PartitionLog log = getOrCreate(topic, partitionId);
        long physicalHighWatermark = log.highWatermark();
        long now = clock.millis();
        DeliverySchedule.VisibilityUpdate visibility = deliverySchedule.pruneAndGetVisibilityAt(
                topic, partitionId, physicalHighWatermark, now);
        if (visibility.prunedCount() > 0) {
            saveCheckpoint(now);
        }
        return new VisibilitySnapshot(
                physicalHighWatermark,
                visibility.visibleHighWatermark(),
                visibility.nextVisibleAtEpochMs());
    }

    public synchronized long deliveryAtEpochMs(String topic, int partitionId, long offset) {
        ensureOpen();
        return deliverySchedule.deliveryAt(topic, partitionId, offset).orElse(0L);
    }

    public synchronized int scheduledRecordCount() {
        ensureOpen();
        if (pruneDueDeliveries(highWatermarks()) > 0) {
            saveCheckpoint();
        }
        return deliverySchedule.count();
    }

    public synchronized void ensureScheduledCapacity(int futureRecordCount) {
        ensureOpen();
        if (futureRecordCount < 0) {
            throw new IllegalArgumentException("futureRecordCount must be non-negative");
        }
        int maxScheduledRecords = config.maxScheduledRecords();
        if (futureRecordCount == 0) {
            return;
        }
        if (maxScheduledRecords == 0) {
            throw new ScheduledCapacityExceededException("Scheduled delivery is disabled");
        }
        if (pruneDueDeliveries(highWatermarks()) > 0) {
            saveCheckpoint();
        }
        int requestedScheduledRecords = deliverySchedule.count() + futureRecordCount;
        if (requestedScheduledRecords > maxScheduledRecords) {
            throw new ScheduledCapacityExceededException("Scheduled delivery capacity exceeded: "
                    + requestedScheduledRecords + " > " + maxScheduledRecords);
        }
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
                        clock.millis(),
                        protectedFutureOffsets(entry)));
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
                            clock.millis(),
                            protectedFutureOffsets(entry)));
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
        // Scan all markers before opening any partition log. This handles a
        // stale or lost checkpoint and permanently blocks deleted names.
        Map<String, Long> begunDeletes = new HashMap<>();
        Set<String> committedDeletes = new HashSet<>();
        writeAheadLog.scanFrom(0L, record -> {
            if (record.type() == WalRecordType.TOPIC_DELETE_BEGIN) {
                begunDeletes.merge(record.topic(), record.offset(), Math::max);
            } else if (record.type() == WalRecordType.TOPIC_DELETE_COMMIT) {
                committedDeletes.add(record.topic());
            }
        });
        Set<String> deletedInWal = new HashSet<>(begunDeletes.keySet());
        deletedInWal.addAll(committedDeletes);
        for (String topic : deletedInWal) {
            // Keep the portable identity reserved across restarts, just as it is
            // during the original delete operation.
            registerTopicStorageOwner(topic);
            deleteTopicFiles(topic);
            deletedTopics.add(topic);
        }

        Optional<RecoveryCheckpoint> checkpoint = checkpointStore.load()
                .filter(this::partitionLogsMatchCheckpoint);
        checkpoint.ifPresent(this::restoreScheduledDeliveries);
        long position = checkpoint
                .map(loaded -> loaded.walPosition)
                .orElse(0L);
        writeAheadLog.scanFrom(position, record -> {
            if (deletedTopics.contains(record.topic())) {
                return;
            }
            PartitionLog log = getOrCreate(record.topic(), record.partitionId());
            switch (record.type()) {
                case DATA_APPEND, SCHEDULED_DATA_APPEND -> {
                    log.appendAt(record.offset(), record.key(), record.value());
                    deliverySchedule.restore(
                            record.topic(),
                            record.partitionId(),
                            record.offset(),
                            record.deliverAtEpochMs());
                }
                case TRUNCATE_BEGIN -> {
                    log.truncateTo(record.offset());
                    deliverySchedule.truncatePartition(record.topic(), record.partitionId(), record.offset());
                }
                case TRUNCATE_COMMIT -> { }
                case TOPIC_DELETE_BEGIN, TOPIC_DELETE_COMMIT -> { }
                default -> throw new IllegalStateException("Unsupported WAL replay type " + record.type());
            }
        });
        // A crash after a forced begin but before cleanup/commit is repaired
        // idempotently before the manager can become active.
        for (Map.Entry<String, Long> begunDelete : begunDeletes.entrySet()) {
            String topic = begunDelete.getKey();
            if (!committedDeletes.contains(topic)) {
                writeAheadLog.append(
                        WalRecord.topicDeleteCommit(topic, begunDelete.getValue()), true);
            }
        }
    }

    private boolean partitionLogsMatchCheckpoint(RecoveryCheckpoint checkpoint) {
        for (Map.Entry<String, Long> entry : checkpoint.partitionHighWatermarks.entrySet()) {
            PartitionRef partitionRef = parsePartitionKey(entry.getKey());
            if (deletedTopics.contains(partitionRef.topic())) {
                continue;
            }
            long actualHighWatermark = getOrCreate(partitionRef.topic(), partitionRef.partitionId()).highWatermark();
            if (actualHighWatermark < entry.getValue()) {
                return false;
            }
        }
        return true;
    }

    private void restoreScheduledDeliveries(RecoveryCheckpoint checkpoint) {
        checkpoint.visibleDeliveryWatermarks.forEach((partitionKey, visibleWatermark) -> {
            PartitionRef partitionRef = tryParsePartitionKey(partitionKey);
            if (partitionRef == null || deletedTopics.contains(partitionRef.topic())) {
                return;
            }
            long highWatermark = getOrCreate(partitionRef.topic(), partitionRef.partitionId()).highWatermark();
            deliverySchedule.restoreVisibleWatermark(
                    partitionRef.topic(),
                    partitionRef.partitionId(),
                    Math.min(visibleWatermark, highWatermark));
        });
        checkpoint.scheduledDeliveries.forEach((partitionKey, deliveries) -> {
            PartitionRef partitionRef = tryParsePartitionKey(partitionKey);
            if (partitionRef == null || deletedTopics.contains(partitionRef.topic())) {
                return;
            }
            long highWatermark = getOrCreate(partitionRef.topic(), partitionRef.partitionId()).highWatermark();
            deliveries.forEach((offset, deliverAtEpochMs) -> {
                if (offset < highWatermark) {
                    deliverySchedule.restore(
                            partitionRef.topic(),
                            partitionRef.partitionId(),
                            offset,
                            deliverAtEpochMs);
                }
            });
        });
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

    private PartitionRef tryParsePartitionKey(String partitionKey) {
        if (partitionKey == null) {
            return null;
        }
        int separator = partitionKey.lastIndexOf('-');
        if (separator <= 0 || separator == partitionKey.length() - 1) {
            return null;
        }
        try {
            int partitionId = Integer.parseInt(partitionKey.substring(separator + 1));
            return partitionId < 0
                    ? null
                    : new PartitionRef(partitionKey.substring(0, separator), partitionId);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private void deleteTopicFiles(String topic) {
        deliverySchedule.removeTopic(topic);
        // Close/remove cached logs first. Partition keys are parsed at the
        // final '-' so topics containing '-' are not confused with prefixes.
        for (Map.Entry<String, PartitionLog> entry : new ArrayList<>(logs.entrySet())) {
            PartitionRef ref = tryParsePartitionKey(entry.getKey());
            if (ref == null || !topic.equals(ref.topic())) {
                continue;
            }
            entry.getValue().close();
            logs.remove(entry.getKey(), entry.getValue());
            try {
                deleteRecursively(dataDir.resolve("segments").resolve(entry.getKey()));
                Files.deleteIfExists(dataDir.resolve(entry.getKey() + ".log"));
            } catch (IOException e) {
                throw new IllegalStateException("Failed to delete topic partition " + entry.getKey(), e);
            }
        }
        Path segmentsRoot = dataDir.resolve("segments");
        try {
            if (Files.exists(segmentsRoot)) {
                try (var children = Files.list(segmentsRoot)) {
                    for (Path child : children.toList()) {
                        PartitionRef ref = tryParsePartitionKey(child.getFileName().toString());
                        if (ref != null && topic.equals(ref.topic())) {
                            deleteRecursively(child);
                        }
                    }
                }
            }
            if (Files.exists(dataDir)) {
                try (var children = Files.list(dataDir)) {
                    for (Path child : children.toList()) {
                        if (!Files.isRegularFile(child)) {
                            continue;
                        }
                        String name = child.getFileName().toString();
                        if (!name.endsWith(".log")) {
                            continue;
                        }
                        String partitionKey = name.substring(0, name.length() - 4);
                        PartitionRef ref = tryParsePartitionKey(partitionKey);
                        if (ref != null && topic.equals(ref.topic())) {
                            Files.deleteIfExists(child);
                        }
                    }
                }
            }
            pendingLegacyCleanup.removeIf(path -> {
                String name = path.getFileName().toString();
                if (name.endsWith(".log")) {
                    name = name.substring(0, name.length() - 4);
                }
                PartitionRef ref = tryParsePartitionKey(name);
                return ref != null && topic.equals(ref.topic());
            });
        } catch (IOException e) {
            throw new IllegalStateException("Failed to delete topic files for " + topic, e);
        }
    }

    private void saveCheckpoint() {
        saveCheckpoint(clock.millis());
    }

    private void saveCheckpoint(long now) {
        flushAll();
        Map<String, Long> highWatermarks = highWatermarks();
        pruneDueDeliveries(highWatermarks, now);
        checkpointStore.save(checkpointStore.checkpoint(
                writeAheadLog.size(),
                highWatermarks,
                deliverySchedule.snapshotByPartitionKey(),
                deliverySchedule.visibleWatermarkSnapshotByPartitionKey()));
        cleanupMigratedLegacyFiles();
    }

    private void flushAfterAppendIfNeeded() {
        if (config.flushPolicy().shouldFlush(successfulAppends, clock.millis(), lastFlushEpochMs)) {
            flushAll();
        }
    }

    private void scheduleIfFuture(String topic, int partitionId, long offset, long deliverAtEpochMs) {
        scheduleIfFuture(topic, partitionId, offset, deliverAtEpochMs, clock.millis());
    }

    private void scheduleIfFuture(
            String topic, int partitionId, long offset, long deliverAtEpochMs, long now) {
        if (deliverAtEpochMs > now) {
            deliverySchedule.put(topic, partitionId, offset, deliverAtEpochMs);
        } else {
            deliverySchedule.noteReadyOffset(topic, partitionId, offset);
        }
    }

    private List<Long> protectedFutureOffsets(Map.Entry<String, PartitionLog> entry) {
        PartitionRef partitionRef = tryParsePartitionKey(entry.getKey());
        if (partitionRef == null) {
            return List.of();
        }
        return deliverySchedule.futureOffsets(
                partitionRef.topic(),
                partitionRef.partitionId(),
                entry.getValue().highWatermark());
    }

    private int pruneDueDeliveries(Map<String, Long> highWatermarks) {
        return pruneDueDeliveries(highWatermarks, clock.millis());
    }

    private int pruneDueDeliveries(Map<String, Long> highWatermarks, long now) {
        int pruned = 0;
        for (Map.Entry<String, Long> entry : highWatermarks.entrySet()) {
            PartitionRef partitionRef = tryParsePartitionKey(entry.getKey());
            if (partitionRef != null) {
                pruned += deliverySchedule.pruneDueAt(
                        partitionRef.topic(), partitionRef.partitionId(), entry.getValue(), now);
            }
        }
        return pruned;
    }

    private void registerTopicStorageOwner(String topic) {
        String identity = TopicNames.portableStorageIdentity(topic);
        String existing = topicStorageOwners.putIfAbsent(identity, topic);
        if (existing != null && !existing.equals(topic)) {
            throw new IllegalArgumentException(
                    "Topic names '" + existing + "' and '" + topic
                            + "' share the same portable storage identity");
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

    public record AppendRecord(byte[] key, byte[] value, long deliverAtEpochMs) {
        public AppendRecord {
            if (deliverAtEpochMs < 0) {
                throw new IllegalArgumentException("delivery time must be non-negative");
            }
        }
    }

    public record VisibilitySnapshot(
            long physicalHighWatermark,
            long visibleHighWatermark,
            long nextVisibleAtEpochMs) {
        public VisibilitySnapshot {
            if (physicalHighWatermark < 0L
                    || visibleHighWatermark < 0L
                    || visibleHighWatermark > physicalHighWatermark) {
                throw new IllegalArgumentException("invalid visibility watermarks");
            }
            if (visibleHighWatermark == physicalHighWatermark
                    ? nextVisibleAtEpochMs != -1L
                    : nextVisibleAtEpochMs < 0L) {
                throw new IllegalArgumentException("invalid next visibility timestamp");
            }
        }
    }

    public static final class ScheduledCapacityExceededException extends IllegalStateException {
        public ScheduledCapacityExceededException(String message) {
            super(message);
        }
    }
}
