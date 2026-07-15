package com.minimq.storage;

import com.minimq.common.model.Record;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

/**
 * A partition log backed by ordered, size-rolled log segments.
 */
public final class SegmentedPartitionLog implements PartitionLog {
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
            Record existing = findRecord(offset);
            if (existing != null && java.util.Arrays.equals(existing.key(), key)
                    && java.util.Arrays.equals(existing.value(), value)) {
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
    public synchronized long highWatermark() {
        ensureOpen();
        return nextOffset;
    }

    @Override
    public synchronized void flush() {
        ensureOpen();
        segments.forEach(SegmentFile::flush);
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
                flushSegments();
                segments.forEach(SegmentFile::close);
            } finally {
                endReaderQuiescence();
            }
        } catch (RuntimeException e) {
            throw new IllegalStateException("Failed to close segmented partition log", e);
        }
    }

    synchronized void applyRetention(RetentionPolicy policy, long nowEpochMs) {
        ensureOpen();
        List<Long> basesToDelete = RetentionPlanner.segmentsToDelete(segmentMetadata(), policy, nowEpochMs);
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
        beginReaderQuiescence();
        try {
            for (SegmentFile segment : deletedSegments) {
                long baseOffset = segment.baseOffset();
                segment.close();
                deleteSegmentFiles(baseOffset);
            }
        } finally {
            endReaderQuiescence();
        }
    }

    private void initialize() throws IOException {
        catalogStore.cleanupMaintenanceArtifacts();
        catalog = catalogStore.load().orElseGet(this::discoverOrCreateCatalog);
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

    private void endReaderQuiescence() {
        synchronized (readerMonitor) {
            maintenanceDeleting = false;
            readerMonitor.notifyAll();
        }
    }

    private void flushSegments() {
        segments.forEach(SegmentFile::flush);
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

    private void deleteSegmentFiles(long baseOffset) {
        try {
            Files.deleteIfExists(SegmentFile.logPath(partitionDir, baseOffset));
            Files.deleteIfExists(SegmentFile.indexPath(partitionDir, baseOffset));
        } catch (IOException e) {
            throw new IllegalStateException("Failed to delete retained segment " + baseOffset, e);
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

    private final class ReadSnapshot implements AutoCloseable {
        @Override
        public void close() {
            synchronized (readerMonitor) {
                if (activeReaders.decrementAndGet() == 0) {
                    readerMonitor.notifyAll();
                }
            }
        }
    }
}
