package com.minimq.storage;

import com.minimq.common.model.Record;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

/**
 * One physical log segment and its sparse offset index.
 */
final class SegmentFile implements AutoCloseable {
    private final SegmentDescriptor descriptor;
    private final Path logPath;
    private final FileChannel logChannel;
    private final SparseOffsetIndex index;
    private final SegmentedLogConfig config;
    private long recordCount;
    private long firstOffset = -1;
    private long lastOffset = -1;
    private boolean closed;

    private SegmentFile(
            SegmentDescriptor descriptor,
            Path logPath,
            FileChannel logChannel,
            SparseOffsetIndex index,
            SegmentedLogConfig config) {
        this.descriptor = descriptor;
        this.logPath = logPath;
        this.logChannel = logChannel;
        this.index = index;
        this.config = config;
    }

    static SegmentFile create(Path partitionDir, SegmentDescriptor descriptor, SegmentedLogConfig config)
            throws IOException {
        Files.createDirectories(partitionDir);
        Path logPath = logPath(partitionDir, descriptor.baseOffset());
        FileChannel logChannel = FileChannel.open(logPath,
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE);
        try {
            SparseOffsetIndex index = SparseOffsetIndex.open(indexPath(partitionDir, descriptor.baseOffset()));
            return new SegmentFile(descriptor, logPath, logChannel, index, config);
        } catch (IOException | RuntimeException failure) {
            logChannel.close();
            throw failure;
        }
    }

    static SegmentFile open(
            Path partitionDir,
            SegmentDescriptor descriptor,
            boolean active,
            SegmentedLogConfig config) throws IOException {
        Path logPath = logPath(partitionDir, descriptor.baseOffset());
        if (!Files.exists(logPath)) {
            throw new IOException("Missing segment log " + logPath);
        }
        FileChannel logChannel = FileChannel.open(logPath, StandardOpenOption.READ, StandardOpenOption.WRITE);
        try {
            List<SparseOffsetIndex.IndexEntry> expectedIndex = new ArrayList<>();
            long fileSize = logChannel.size();
            long previousOffset = -1;
            long recordCount = 0;
            long firstOffset = -1;
            long lastOffset = -1;
            logChannel.position(0);
            while (logChannel.position() < fileSize) {
                long recordStart = logChannel.position();
                PartitionLogRecordCodec.DecodedRecord decoded;
                try {
                    decoded = PartitionLogRecordCodec.read(logChannel, recordStart, fileSize);
                } catch (PartitionLogRecordCodec.IncompleteRecordException incomplete) {
                    if (!active) {
                        throw new IOException("Corrupt sealed segment " + logPath + ": incomplete tail", incomplete);
                    }
                    logChannel.truncate(recordStart);
                    fileSize = recordStart;
                    break;
                }
                long offset = decoded.record().offset();
                if (offset <= previousOffset) {
                    throw new IOException("Corrupt segment " + logPath + ": non-increasing offset " + offset);
                }
                if (recordCount == 0) {
                    firstOffset = offset;
                }
                if (recordCount % config.sparseIndexIntervalRecords() == 0) {
                    expectedIndex.add(new SparseOffsetIndex.IndexEntry(offset, recordStart));
                }
                previousOffset = offset;
                lastOffset = offset;
                recordCount++;
            }

            SparseOffsetIndex index = rebuildIndex(partitionDir, descriptor.baseOffset(), expectedIndex);
            SegmentFile segment = new SegmentFile(descriptor, logPath, logChannel, index, config);
            segment.recordCount = recordCount;
            segment.firstOffset = firstOffset;
            segment.lastOffset = lastOffset;
            logChannel.position(fileSize);
            return segment;
        } catch (IOException | RuntimeException failure) {
            logChannel.close();
            throw failure;
        }
    }

    long baseOffset() {
        return descriptor.baseOffset();
    }

    long createdAtEpochMs() {
        return descriptor.createdAtEpochMs();
    }

    long firstOffset() {
        return firstOffset;
    }

    long lastOffset() {
        return lastOffset;
    }

    long nextOffset() {
        return lastOffset < 0 ? descriptor.baseOffset() : lastOffset + 1;
    }

    boolean hasRecords() {
        return recordCount > 0;
    }

    long size() {
        try {
            ensureOpen();
            return logChannel.size();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to read segment size " + logPath, e);
        }
    }

    void append(long offset, byte[] key, byte[] value) throws IOException {
        ensureOpen();
        if (offset < descriptor.baseOffset() || offset <= lastOffset) {
            throw new IOException("Invalid append offset " + offset + " for segment " + logPath);
        }
        long position = logChannel.size();
        logChannel.position(position);
        PartitionLogRecordCodec.write(logChannel, offset, key, value);
        if (recordCount % config.sparseIndexIntervalRecords() == 0) {
            index.append(offset, position);
        }
        if (recordCount == 0) {
            firstOffset = offset;
        }
        lastOffset = offset;
        recordCount++;
    }

    synchronized List<Record> read(long fromOffset, int maxBytes) throws IOException {
        ensureOpen();
        if (maxBytes <= 0) {
            return List.of();
        }
        List<Record> records = new ArrayList<>();
        int bytesRead = 0;
        long fileSize = logChannel.size();
        logChannel.position(index.floorPosition(fromOffset));
        while (logChannel.position() < fileSize) {
            long recordStart = logChannel.position();
            PartitionLogRecordCodec.DecodedRecord decoded =
                    PartitionLogRecordCodec.read(logChannel, recordStart, fileSize);
            if (decoded.record().offset() < fromOffset) {
                continue;
            }
            if (!records.isEmpty() && bytesRead + decoded.encodedBytes() > maxBytes) {
                break;
            }
            records.add(decoded.record());
            bytesRead += decoded.encodedBytes();
        }
        return records;
    }

    synchronized Record recordAt(long offset) throws IOException {
        for (Record record : read(offset, Integer.MAX_VALUE)) {
            if (record.offset() == offset) {
                return record;
            }
            if (record.offset() > offset) {
                return null;
            }
        }
        return null;
    }

    void flush() {
        try {
            ensureOpen();
            logChannel.force(false);
            index.flush();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to flush segment " + logPath, e);
        }
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        try {
            logChannel.force(false);
            index.close();
            logChannel.close();
            closed = true;
        } catch (IOException e) {
            throw new IllegalStateException("Failed to close segment " + logPath, e);
        }
    }

    static Path logPath(Path partitionDir, long baseOffset) {
        return partitionDir.resolve(String.format("%020d.log", baseOffset));
    }

    static Path indexPath(Path partitionDir, long baseOffset) {
        return partitionDir.resolve(String.format("%020d.index", baseOffset));
    }

    static Path cleanedLogPath(Path partitionDir, long baseOffset) {
        return partitionDir.resolve(String.format("%020d.cleaned.log", baseOffset));
    }

    static Path cleanedIndexPath(Path partitionDir, long baseOffset) {
        return partitionDir.resolve(String.format("%020d.cleaned.index", baseOffset));
    }

    static Path obsoleteLogPath(Path partitionDir, long baseOffset) {
        return partitionDir.resolve(String.format("%020d.obsolete.log", baseOffset));
    }

    static Path obsoleteIndexPath(Path partitionDir, long baseOffset) {
        return partitionDir.resolve(String.format("%020d.obsolete.index", baseOffset));
    }

    static void writeCleaned(
            Path partitionDir,
            long baseOffset,
            List<Record> records,
            SegmentedLogConfig config) throws IOException {
        Path logPath = cleanedLogPath(partitionDir, baseOffset);
        Path indexPath = cleanedIndexPath(partitionDir, baseOffset);
        Files.deleteIfExists(logPath);
        Files.deleteIfExists(indexPath);
        try (FileChannel logChannel = FileChannel.open(logPath,
                StandardOpenOption.CREATE_NEW,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE);
             SparseOffsetIndex index = SparseOffsetIndex.open(indexPath)) {
            long previousOffset = -1;
            long recordCount = 0;
            for (Record record : records) {
                if (record.offset() < baseOffset || record.offset() <= previousOffset) {
                    throw new IOException("Invalid compacted record offset " + record.offset());
                }
                long position = logChannel.size();
                PartitionLogRecordCodec.write(logChannel, record.offset(), record.key(), record.value());
                if (recordCount % config.sparseIndexIntervalRecords() == 0) {
                    index.append(record.offset(), position);
                }
                previousOffset = record.offset();
                recordCount++;
            }
            logChannel.force(false);
            index.flush();
        }
    }

    private static SparseOffsetIndex rebuildIndex(
            Path partitionDir,
            long baseOffset,
            List<SparseOffsetIndex.IndexEntry> expectedIndex) throws IOException {
        Path indexPath = indexPath(partitionDir, baseOffset);
        try (SparseOffsetIndex index = SparseOffsetIndex.open(indexPath)) {
            index.replace(expectedIndex);
            index.flush();
        } catch (IOException corruptIndex) {
            Files.deleteIfExists(indexPath);
            try (SparseOffsetIndex rebuilt = SparseOffsetIndex.open(indexPath)) {
                rebuilt.replace(expectedIndex);
                rebuilt.flush();
            }
        }
        return SparseOffsetIndex.open(indexPath);
    }

    private void ensureOpen() throws IOException {
        if (closed || !logChannel.isOpen()) {
            throw new IOException("Segment is closed " + logPath);
        }
    }
}
