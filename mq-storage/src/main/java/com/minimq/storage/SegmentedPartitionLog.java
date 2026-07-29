package com.minimq.storage;

import com.minimq.common.model.Record;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.FileTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

/**
 * A partition log backed by ordered, size-rolled log segments.
 */
public final class SegmentedPartitionLog implements PartitionLog, TransferablePartitionLog {
    private final Path partitionDir;
    private final SegmentedLogConfig config;
    private final SegmentCatalogStore catalogStore;
    private final CopyOnWriteArrayList<SegmentFile> segments = new CopyOnWriteArrayList<>();
    private final Object readerMonitor = new Object();
    private final AtomicInteger activeReaders = new AtomicInteger();
    private boolean maintenanceDeleting;
    private SegmentCatalog catalog;
    private long nextOffset;
    private volatile boolean closed;

    public SegmentedPartitionLog(Path partitionDir, SegmentedLogConfig config) throws IOException {
        this.partitionDir = partitionDir;
        this.config = config;
        this.catalogStore = new SegmentCatalogStore(partitionDir);
        Files.createDirectories(partitionDir);
        initialize();
    }

    @Override
    public synchronized long append(byte[] key, byte[] value) {
        long offset = highWatermark();
        appendAt(offset, key, value);
        return offset;
    }

    @Override
    public synchronized void appendAt(long offset, byte[] key, byte[] value) {
        ensureOpen();
        if (offset < nextOffset) {
            if (offset < catalog.logStartOffset()) {
                return;
            }
            Record existing = findRecord(offset);
            if (existing != null && java.util.Arrays.equals(existing.key(), key)
                    && java.util.Arrays.equals(existing.value(), value)) {
                return;
            }
            if (existing == null && isHistoricalOffsetHole(offset)) {
                return;
            }
            throw new IllegalStateException("Conflicting record already exists at offset " + offset);
        }
        if (offset > nextOffset) {
            throw new IllegalStateException("Cannot append offset " + offset + " when next offset is " + nextOffset);
        }
        try {
            SegmentFile active = activeSegment();
            int encodedSize = PartitionLogRecordCodec.encodedSize(offset, key, value);
            if (active.hasRecords() && active.size() + encodedSize > config.segmentMaxBytes()) {
                active = roll(offset);
            }
            active.append(offset, key, value);
            nextOffset = offset + 1;
        } catch (IOException e) {
            throw new IllegalStateException("Failed to append segmented record", e);
        }
    }

    @Override
    public List<Record> read(long fromOffset, int maxBytes) {
        try (ReadSnapshot ignored = acquireReadSnapshot()) {
            ensureOpen();
            if (maxBytes <= 0) {
                return List.of();
            }
            try {
                return readFrom(snapshotSegments(), fromOffset, maxBytes);
            } catch (IOException e) {
                throw new IllegalStateException("Failed to read segmented records", e);
            }
        }
    }

    @Override
    public FetchPlan planFetch(long fromOffset, int maxBytes) {
        long leaderHighWatermark;
        synchronized (this) {
            leaderHighWatermark = nextOffset;
        }
        return planFetch(fromOffset, maxBytes, leaderHighWatermark);
    }

    @Override
    public FetchPlan planFetch(long fromOffset, int maxBytes, long exclusiveHighWatermark) {
        ReadSnapshot snapshot = acquireReadSnapshot();
        try {
            ensureOpen();
            final long fetchHighWatermark;
            final List<SegmentFile> segmentSnapshot;
            synchronized (this) {
                fetchHighWatermark = Math.min(nextOffset, exclusiveHighWatermark);
                segmentSnapshot = List.copyOf(segments);
            }
            if (maxBytes <= 0) {
                return new FetchPlan(fetchHighWatermark, 0, 0, List.of(), snapshot::close);
            }
            int recordCount = 0;
            int recordBytes = 0;
            List<FileRegionSlice> slices = new ArrayList<>();
            int startIndex = segmentIndexFor(segmentSnapshot, fromOffset);
            for (int i = startIndex; i < segmentSnapshot.size(); i++) {
                if (recordCount > 0 && recordBytes >= maxBytes) {
                    break;
                }
                int remaining = Math.max(1, maxBytes - recordBytes);
                SegmentFile.PlannedRead planned =
                        segmentSnapshot.get(i).planRead(
                                fromOffset, remaining, fetchHighWatermark, recordCount == 0);
                if (planned.recordCount() == 0) {
                    continue;
                }
                recordCount += planned.recordCount();
                recordBytes += planned.recordBytes();
                slices.add(planned.slice());
            }
            return new FetchPlan(fetchHighWatermark, recordCount, recordBytes, slices, snapshot::close);
        } catch (IOException | RuntimeException failure) {
            snapshot.close();
            throw new IllegalStateException("Failed to plan segmented fetch", failure);
        }
    }

    @Override
    public synchronized long highWatermark() {
        ensureOpen();
        return nextOffset;
    }

    @Override
    public synchronized void truncateTo(long offset) {
        ensureOpen();
        if (offset < catalog.logStartOffset() || offset > nextOffset) {
            throw new IllegalArgumentException("truncate offset outside retained log range: " + offset);
        }
        if (offset == nextOffset) {
            return;
        }
        beginReaderQuiescence();
        try {
            int targetIndex = segmentIndexFor(offset);
            SegmentFile target = segments.get(targetIndex);
            target.truncateTo(offset);
            target.flush();

            List<SegmentDescriptor> retainedDescriptors =
                    new ArrayList<>(catalog.segments().subList(0, targetIndex + 1));
            SegmentCatalog nextCatalog = new SegmentCatalog(
                    catalog.generation() + 1,
                    target.baseOffset(),
                    catalog.logStartOffset(),
                    retainedDescriptors);
            catalogStore.save(nextCatalog);

            List<SegmentFile> discarded = new ArrayList<>(segments.subList(targetIndex + 1, segments.size()));
            segments.subList(targetIndex + 1, segments.size()).clear();
            catalog = nextCatalog;
            nextOffset = offset;
            for (SegmentFile segment : discarded) {
                long baseOffset = segment.baseOffset();
                segment.close();
                deleteSegmentFiles(baseOffset);
            }
        } catch (IOException e) {
            throw new IllegalStateException("Failed to truncate segmented log " + partitionDir, e);
        } finally {
            endReaderQuiescence();
        }
    }

    @Override
    public synchronized void flush() {
        ensureOpen();
        segments.stream().filter(segment -> !segment.isClosed()).forEach(SegmentFile::flush);
    }

    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        try {
            closed = true;
            beginReaderQuiescence();
            try {
                // A segment may already have been closed by a completed maintenance/truncation
                // replacement. Its close path forced both files, so it must not make shutdown fail.
                segments.stream().filter(segment -> !segment.isClosed()).forEach(SegmentFile::flush);
                segments.forEach(SegmentFile::close);
            } finally {
                endReaderQuiescence();
            }
        } catch (RuntimeException e) {
            throw new IllegalStateException("Failed to close segmented partition log", e);
        }
    }

    synchronized LogMaintenanceReport previewMaintenance(
            String partition,
            RetentionPolicy retentionPolicy,
            CompactionPolicy compactionPolicy,
            long nowEpochMs,
            List<Long> protectedOffsets) {
        ensureOpen();
        try {
            MaintenancePlan plan = planMaintenance(
                    partition,
                    retentionPolicy,
                    compactionPolicy,
                    nowEpochMs,
                    protectedOffsets);
            return new LogMaintenanceReport(
                    true,
                    plan.reports().isEmpty() ? MaintenanceStatus.NOOP : MaintenanceStatus.SUCCESS,
                    plan.reports(),
                    0,
                    0);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to preview segmented log maintenance " + partitionDir, e);
        }
    }

    synchronized LogMaintenanceReport applyMaintenanceWithReport(
            String partition,
            RetentionPolicy retentionPolicy,
            CompactionPolicy compactionPolicy,
            long nowEpochMs,
            List<Long> protectedOffsets) {
        ensureOpen();
        if (!tryBeginReaderQuiescence()) {
            return new LogMaintenanceReport(
                    false,
                    MaintenanceStatus.SKIPPED_READERS,
                    List.of(),
                    1,
                    0);
        }
        try {
            MaintenancePlan plan = planMaintenance(
                    partition,
                    retentionPolicy,
                    compactionPolicy,
                    nowEpochMs,
                    protectedOffsets);
            if (plan.reports().isEmpty()) {
                return new LogMaintenanceReport(false, MaintenanceStatus.NOOP, List.of(), 0, 0);
            }
            applyRetentionPlan(plan.retentionBases());
            applyCompactionPlan(plan.compactionReplacements());
            return new LogMaintenanceReport(false, MaintenanceStatus.SUCCESS, plan.reports(), 0, 0);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to maintain segmented log " + partitionDir, e);
        } finally {
            endReaderQuiescence();
        }
    }

    synchronized void applyRetention(RetentionPolicy policy, long nowEpochMs) {
        applyRetentionWithReport(partitionName(), policy, nowEpochMs);
    }

    synchronized LogMaintenanceReport applyRetentionWithReport(String partition, RetentionPolicy policy, long nowEpochMs) {
        ensureOpen();
        List<Long> basesToDelete = RetentionPlanner.segmentsToDelete(segmentMetadata(), policy, nowEpochMs);
        if (basesToDelete.isEmpty()) {
            return new LogMaintenanceReport(false, MaintenanceStatus.NOOP, List.of(), 0, 0);
        }
        List<MaintenanceSegmentReport> reports = retentionReports(partition, basesToDelete);
        if (!tryBeginReaderQuiescence()) {
            return new LogMaintenanceReport(
                    false,
                    MaintenanceStatus.SKIPPED_READERS,
                    List.of(),
                    1,
                    0);
        }
        try {
            applyRetentionPlan(basesToDelete);
        } finally {
            endReaderQuiescence();
        }
        return new LogMaintenanceReport(false, MaintenanceStatus.SUCCESS, reports, 0, 0);
    }

    private void applyRetentionPlan(List<Long> basesToDelete) {
        if (basesToDelete.isEmpty()) {
            return;
        }
        Set<Long> deleteSet = new HashSet<>(basesToDelete);
        List<SegmentFile> keptSegments = segments.stream()
                .filter(segment -> !deleteSet.contains(segment.baseOffset()))
                .toList();
        if (keptSegments.isEmpty()) {
            return;
        }
        List<SegmentDescriptor> keptDescriptors = catalog.segments().stream()
                .filter(descriptor -> !deleteSet.contains(descriptor.baseOffset()))
                .toList();
        SegmentCatalog nextCatalog = new SegmentCatalog(
                catalog.generation() + 1,
                catalog.activeBaseOffset(),
                keptDescriptors.get(0).baseOffset(),
                keptDescriptors);
        catalogStore.save(nextCatalog);
        List<SegmentFile> deletedSegments = segments.stream()
                .filter(segment -> deleteSet.contains(segment.baseOffset()))
                .toList();
        segments.removeIf(segment -> deleteSet.contains(segment.baseOffset()));
        catalog = nextCatalog;
        for (SegmentFile segment : deletedSegments) {
            long baseOffset = segment.baseOffset();
            segment.close();
            deleteSegmentFiles(baseOffset);
        }
    }

    synchronized void applyCompaction(CompactionPolicy policy) {
        applyCompactionWithReport(partitionName(), policy);
    }

    synchronized LogMaintenanceReport applyCompactionWithReport(String partition, CompactionPolicy policy) {
        ensureOpen();
        if (!tryBeginReaderQuiescence()) {
            return new LogMaintenanceReport(
                    false,
                    MaintenanceStatus.SKIPPED_READERS,
                    List.of(),
                    1,
                    0);
        }
        try {
            List<SegmentCompactor.SegmentReplacement> replacements =
                    SegmentCompactor.plan(List.copyOf(segments), policy);
            if (replacements.isEmpty()) {
                return new LogMaintenanceReport(false, MaintenanceStatus.NOOP, List.of(), 0, 0);
            }
            List<MaintenanceSegmentReport> reports = compactionReports(partition, replacements);
            applyCompactionPlan(replacements);
            return new LogMaintenanceReport(false, MaintenanceStatus.SUCCESS, reports, 0, 0);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to compact segmented log " + partitionDir, e);
        } finally {
            endReaderQuiescence();
        }
    }

    private void applyCompactionPlan(List<SegmentCompactor.SegmentReplacement> replacements) throws IOException {
        if (replacements.isEmpty()) {
            return;
        }
        Map<Long, SegmentCompactor.SegmentReplacement> replacementByBase = new HashMap<>();
        for (SegmentCompactor.SegmentReplacement replacement : replacements) {
            replacementByBase.put(replacement.baseOffset(), replacement);
            if (!replacement.retainedRecords().isEmpty()) {
                SegmentFile.writeCleaned(
                        partitionDir,
                        replacement.baseOffset(),
                        replacement.retainedRecords(),
                        config);
            }
        }

        List<SegmentDescriptor> compactedDescriptors = catalog.segments().stream()
                .filter(descriptor -> {
                    SegmentCompactor.SegmentReplacement replacement =
                            replacementByBase.get(descriptor.baseOffset());
                    return replacement == null || !replacement.retainedRecords().isEmpty();
                })
                .toList();
        SegmentCatalog nextCatalog = new SegmentCatalog(
                catalog.generation() + 1,
                catalog.activeBaseOffset(),
                catalog.logStartOffset(),
                compactedDescriptors);

        catalogStore.save(nextCatalog);
        List<SegmentFile> compactedSegments = new ArrayList<>();
        for (SegmentFile segment : segments) {
            SegmentCompactor.SegmentReplacement replacement = replacementByBase.get(segment.baseOffset());
            if (replacement == null) {
                compactedSegments.add(segment);
                continue;
            }
            segment.close();
            if (replacement.retainedRecords().isEmpty()) {
                deleteSegmentFiles(replacement.baseOffset());
            } else {
                replaceWithCleanedSegment(replacement.baseOffset());
                compactedSegments.add(SegmentFile.open(
                        partitionDir,
                        descriptorFor(replacement.baseOffset()),
                        false,
                        config));
            }
        }
        segments.clear();
        segments.addAll(compactedSegments);
        catalog = nextCatalog;
    }

    private void initialize() throws IOException {
        var loadedCatalog = catalogStore.load();
        if (loadedCatalog.isPresent()) {
            recoverMaintenanceArtifacts(loadedCatalog.get());
            catalog = loadedCatalog.get();
        } else {
            catalogStore.cleanupMaintenanceArtifacts();
            catalog = discoverOrCreateCatalog();
        }
        List<SegmentDescriptor> descriptors = catalog.segments();
        long previousLastOffset = -1;
        for (int i = 0; i < descriptors.size(); i++) {
            SegmentFile segment = SegmentFile.open(partitionDir, descriptors.get(i), i == descriptors.size() - 1, config);
            if (segment.firstOffset() >= 0 && segment.firstOffset() <= previousLastOffset) {
                closeOpenedSegments();
                throw new IOException("Corrupt segment catalog " + partitionDir + ": overlapping offsets");
            }
            if (segment.lastOffset() >= 0) {
                previousLastOffset = segment.lastOffset();
            }
            segments.add(segment);
        }
        nextOffset = previousLastOffset < 0 ? catalog.activeBaseOffset() : previousLastOffset + 1;
    }

    private SegmentCatalog discoverOrCreateCatalog() {
        try {
            List<SegmentDescriptor> descriptors = discoverSegments();
            if (descriptors.isEmpty()) {
                SegmentDescriptor initial = new SegmentDescriptor(0, System.currentTimeMillis());
                SegmentFile created = SegmentFile.create(partitionDir, initial, config);
                created.close();
                descriptors = List.of(initial);
            }
            SegmentCatalog discovered = new SegmentCatalog(
                    0,
                    descriptors.get(descriptors.size() - 1).baseOffset(),
                    descriptors.get(0).baseOffset(),
                    descriptors);
            catalogStore.save(discovered);
            return discovered;
        } catch (IOException e) {
            throw new IllegalStateException("Failed to discover segment catalog " + partitionDir, e);
        }
    }

    private List<SegmentDescriptor> discoverSegments() throws IOException {
        try (Stream<Path> paths = Files.list(partitionDir)) {
            return paths
                    .filter(path -> path.getFileName().toString().matches("\\d{20}\\.log"))
                    .map(path -> descriptorFromLog(path))
                    .sorted(Comparator.comparingLong(SegmentDescriptor::baseOffset))
                    .toList();
        }
    }

    private SegmentDescriptor descriptorFromLog(Path logPath) {
        try {
            String name = logPath.getFileName().toString();
            long baseOffset = Long.parseLong(name.substring(0, name.length() - ".log".length()));
            FileTime created = Files.getLastModifiedTime(logPath);
            return new SegmentDescriptor(baseOffset, created.toMillis());
        } catch (IOException | NumberFormatException e) {
            throw new IllegalStateException("Invalid segment log " + logPath, e);
        }
    }

    private SegmentFile roll(long baseOffset) throws IOException {
        SegmentDescriptor descriptor = new SegmentDescriptor(baseOffset, System.currentTimeMillis());
        SegmentFile next = SegmentFile.create(partitionDir, descriptor, config);
        List<SegmentDescriptor> descriptors = new ArrayList<>(catalog.segments());
        descriptors.add(descriptor);
        SegmentCatalog nextCatalog = new SegmentCatalog(
                catalog.generation() + 1,
                descriptor.baseOffset(),
                catalog.logStartOffset(),
                descriptors);
        catalogStore.save(nextCatalog);
        catalog = nextCatalog;
        segments.add(next);
        return next;
    }

    private Record findRecord(long offset) {
        try {
            for (SegmentFile segment : segments) {
                if (segment.baseOffset() > offset) {
                    break;
                }
                Record record = segment.recordAt(offset);
                if (record != null) {
                    return record;
                }
            }
            return null;
        } catch (IOException e) {
            throw new IllegalStateException("Failed to read existing segmented record", e);
        }
    }

    private boolean isHistoricalOffsetHole(long offset) {
        return offset >= catalog.logStartOffset() && offset < nextOffset;
    }

    private int segmentIndexFor(long offset) {
        return segmentIndexFor(segments, offset);
    }

    private int segmentIndexFor(List<SegmentFile> candidateSegments, long offset) {
        int selected = 0;
        for (int i = 0; i < candidateSegments.size(); i++) {
            if (candidateSegments.get(i).baseOffset() > offset) {
                break;
            }
            selected = i;
        }
        return selected;
    }

    private SegmentFile activeSegment() {
        return segments.get(segments.size() - 1);
    }

    private List<Record> readFrom(List<SegmentFile> segmentSnapshot, long fromOffset, int maxBytes)
            throws IOException {
        List<Record> records = new ArrayList<>();
        int bytesRead = 0;
        int startIndex = segmentIndexFor(segmentSnapshot, fromOffset);
        for (int i = startIndex; i < segmentSnapshot.size(); i++) {
            int remaining = Math.max(1, maxBytes - bytesRead);
            List<Record> segmentRecords = segmentSnapshot.get(i).read(fromOffset, remaining);
            for (Record record : segmentRecords) {
                int recordSize = PartitionLogRecordCodec.encodedSize(record.offset(), record.key(), record.value());
                if (!records.isEmpty() && bytesRead + recordSize > maxBytes) {
                    return records;
                }
                records.add(record);
                bytesRead += recordSize;
            }
        }
        return records;
    }

    private List<SegmentFile> snapshotSegments() {
        return List.copyOf(segments);
    }

    private ReadSnapshot acquireReadSnapshot() {
        boolean interrupted = false;
        synchronized (readerMonitor) {
            while (maintenanceDeleting) {
                try {
                    readerMonitor.wait(50);
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
            activeReaders.incrementAndGet();
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
        return new ReadSnapshot();
    }

    private void beginReaderQuiescence() {
        boolean interrupted = false;
        synchronized (readerMonitor) {
            maintenanceDeleting = true;
            while (activeReaders.get() > 0) {
                try {
                    readerMonitor.wait(50);
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private boolean tryBeginReaderQuiescence() {
        synchronized (readerMonitor) {
            if (activeReaders.get() > 0) {
                return false;
            }
            maintenanceDeleting = true;
            return true;
        }
    }

    private void endReaderQuiescence() {
        synchronized (readerMonitor) {
            maintenanceDeleting = false;
            readerMonitor.notifyAll();
        }
    }

    private List<SegmentMetadata> segmentMetadata() {
        List<SegmentMetadata> metadata = new ArrayList<>();
        for (int i = 0; i < segments.size(); i++) {
            SegmentFile segment = segments.get(i);
            metadata.add(new SegmentMetadata(
                    segment.baseOffset(),
                    segment.createdAtEpochMs(),
                    segment.size(),
                    i == segments.size() - 1));
        }
        return metadata;
    }

    private List<MaintenanceSegmentReport> retentionReports(String partition, List<Long> basesToDelete) {
        if (basesToDelete.isEmpty()) {
            return List.of();
        }
        Set<Long> deleteSet = new HashSet<>(basesToDelete);
        return segments.stream()
                .filter(segment -> deleteSet.contains(segment.baseOffset()))
                .map(segment -> segmentReport(partition, MaintenanceAction.RETENTION_DELETE, segment))
                .toList();
    }

    private MaintenancePlan planMaintenance(
            String partition,
            RetentionPolicy retentionPolicy,
            CompactionPolicy compactionPolicy,
            long nowEpochMs,
            List<Long> protectedOffsets) throws IOException {
        Set<Long> protectedBases = protectedSegmentBases(protectedOffsets);
        List<Long> retentionBases = RetentionPlanner.segmentsToDelete(
                segmentMetadata(),
                retentionPolicy,
                nowEpochMs,
                protectedBases);
        Set<Long> retentionSet = new HashSet<>(retentionBases);
        List<SegmentFile> survivingSegments = segments.stream()
                .filter(segment -> !retentionSet.contains(segment.baseOffset()))
                .toList();
        List<SegmentCompactor.SegmentReplacement> compactionReplacements =
                SegmentCompactor.plan(survivingSegments, compactionPolicy, protectedBases);
        List<MaintenanceSegmentReport> reports = new ArrayList<>();
        reports.addAll(retentionReports(partition, retentionBases));
        reports.addAll(compactionReports(partition, compactionReplacements));
        return new MaintenancePlan(retentionBases, compactionReplacements, reports);
    }

    private Set<Long> protectedSegmentBases(List<Long> protectedOffsets) {
        if (protectedOffsets.isEmpty()) {
            return Set.of();
        }
        List<Long> sortedOffsets = protectedOffsets.stream().sorted().toList();
        Set<Long> protectedBases = new HashSet<>();
        int offsetIndex = 0;
        for (SegmentFile segment : segments) {
            while (offsetIndex < sortedOffsets.size()
                    && sortedOffsets.get(offsetIndex) < segment.firstOffset()) {
                offsetIndex++;
            }
            if (offsetIndex >= sortedOffsets.size()) {
                break;
            }
            if (sortedOffsets.get(offsetIndex) <= segment.lastOffset()) {
                protectedBases.add(segment.baseOffset());
            }
        }
        return Set.copyOf(protectedBases);
    }

    private List<MaintenanceSegmentReport> compactionReports(
            String partition,
            List<SegmentCompactor.SegmentReplacement> replacements) {
        return replacements.stream()
                .map(replacement -> segmentReport(
                        partition,
                        replacement.retainedRecords().isEmpty()
                                ? MaintenanceAction.COMPACTION_DELETE
                                : MaintenanceAction.COMPACTION_REWRITE,
                        replacement.original()))
                .toList();
    }

    private MaintenanceSegmentReport segmentReport(
            String partition,
            MaintenanceAction action,
            SegmentFile segment) {
        return new MaintenanceSegmentReport(
                partition,
                action,
                segment.baseOffset(),
                segment.firstOffset(),
                segment.lastOffset(),
                segment.size());
    }

    private String partitionName() {
        Path fileName = partitionDir.getFileName();
        return fileName == null ? "partition" : fileName.toString();
    }

    private void deleteSegmentFiles(long baseOffset) {
        try {
            Files.deleteIfExists(SegmentFile.logPath(partitionDir, baseOffset));
            Files.deleteIfExists(SegmentFile.indexPath(partitionDir, baseOffset));
        } catch (IOException e) {
            throw new IllegalStateException("Failed to delete retained segment " + baseOffset, e);
        }
    }

    private SegmentDescriptor descriptorFor(long baseOffset) {
        for (SegmentDescriptor descriptor : catalog.segments()) {
            if (descriptor.baseOffset() == baseOffset) {
                return descriptor;
            }
        }
        throw new IllegalStateException("Missing segment descriptor " + baseOffset);
    }

    private void recoverMaintenanceArtifacts(SegmentCatalog visibleCatalog) throws IOException {
        for (SegmentDescriptor descriptor : visibleCatalog.segments()) {
            recoverVisibleSegmentFiles(descriptor.baseOffset());
        }
        catalogStore.cleanupMaintenanceArtifacts();
    }

    private void recoverVisibleSegmentFiles(long baseOffset) throws IOException {
        Path logPath = SegmentFile.logPath(partitionDir, baseOffset);
        if (Files.exists(logPath)) {
            return;
        }
        Path cleanedLogPath = SegmentFile.cleanedLogPath(partitionDir, baseOffset);
        if (Files.exists(cleanedLogPath)) {
            moveReplacing(cleanedLogPath, logPath);
            promoteMatchingIndex(baseOffset, true);
            return;
        }
        Path obsoleteLogPath = SegmentFile.obsoleteLogPath(partitionDir, baseOffset);
        if (Files.exists(obsoleteLogPath)) {
            moveReplacing(obsoleteLogPath, logPath);
            promoteMatchingIndex(baseOffset, false);
        }
    }

    private void promoteMatchingIndex(long baseOffset, boolean cleaned) throws IOException {
        Path indexPath = SegmentFile.indexPath(partitionDir, baseOffset);
        if (Files.exists(indexPath)) {
            return;
        }
        Path source = cleaned
                ? SegmentFile.cleanedIndexPath(partitionDir, baseOffset)
                : SegmentFile.obsoleteIndexPath(partitionDir, baseOffset);
        if (Files.exists(source)) {
            moveReplacing(source, indexPath);
        }
    }

    private void replaceWithCleanedSegment(long baseOffset) throws IOException {
        Path logPath = SegmentFile.logPath(partitionDir, baseOffset);
        Path indexPath = SegmentFile.indexPath(partitionDir, baseOffset);
        Path obsoleteLogPath = SegmentFile.obsoleteLogPath(partitionDir, baseOffset);
        Path obsoleteIndexPath = SegmentFile.obsoleteIndexPath(partitionDir, baseOffset);
        Files.deleteIfExists(obsoleteLogPath);
        Files.deleteIfExists(obsoleteIndexPath);
        moveReplacing(logPath, obsoleteLogPath);
        moveReplacing(indexPath, obsoleteIndexPath);
        moveReplacing(SegmentFile.cleanedLogPath(partitionDir, baseOffset), logPath);
        moveReplacing(SegmentFile.cleanedIndexPath(partitionDir, baseOffset), indexPath);
        Files.deleteIfExists(obsoleteLogPath);
        Files.deleteIfExists(obsoleteIndexPath);
    }

    private void moveReplacing(Path source, Path target) throws IOException {
        try {
            Files.move(source, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException atomicFailure) {
            Files.move(source, target, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    private void closeOpenedSegments() {
        segments.forEach(SegmentFile::close);
        segments.clear();
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("SegmentedPartitionLog is closed");
        }
    }

    private record MaintenancePlan(
            List<Long> retentionBases,
            List<SegmentCompactor.SegmentReplacement> compactionReplacements,
            List<MaintenanceSegmentReport> reports) {

        private MaintenancePlan {
            retentionBases = List.copyOf(retentionBases);
            compactionReplacements = List.copyOf(compactionReplacements);
            reports = List.copyOf(reports);
        }
    }

    private final class ReadSnapshot implements AutoCloseable {
        private final AtomicBoolean snapshotClosed = new AtomicBoolean();

        @Override
        public void close() {
            if (!snapshotClosed.compareAndSet(false, true)) {
                return;
            }
            synchronized (readerMonitor) {
                if (activeReaders.decrementAndGet() == 0) {
                    readerMonitor.notifyAll();
                }
            }
        }
    }
}
