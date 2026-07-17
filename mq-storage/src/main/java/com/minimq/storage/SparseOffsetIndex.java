package com.minimq.storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

/**
 * A fixed-width sparse index from absolute offset to segment file position.
 */
final class SparseOffsetIndex implements AutoCloseable {
    static final int ENTRY_BYTES = Long.BYTES * 2;

    private final Path path;
    private final FileChannel channel;
    private final List<IndexEntry> entries;
    private boolean closed;

    private SparseOffsetIndex(Path path, FileChannel channel, List<IndexEntry> entries) {
        this.path = path;
        this.channel = channel;
        this.entries = entries;
    }

    static SparseOffsetIndex open(Path path) throws IOException {
        Path parent = path.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        FileChannel channel = FileChannel.open(path,
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE);
        try {
            long fileSize = channel.size();
            if (fileSize % ENTRY_BYTES != 0) {
                throw new IOException("Corrupt sparse index " + path + ": truncated entry");
            }
            List<IndexEntry> entries = new ArrayList<>();
            channel.position(0);
            while (channel.position() < fileSize) {
                IndexEntry entry = readEntry(channel);
                validateLoadedEntry(entries, entry, path);
                entries.add(entry);
            }
            channel.position(fileSize);
            return new SparseOffsetIndex(path, channel, entries);
        } catch (IOException | RuntimeException failure) {
            channel.close();
            throw failure;
        }
    }

    synchronized void append(long offset, long filePosition) {
        ensureOpenUnchecked();
        IndexEntry entry = new IndexEntry(offset, filePosition);
        validateAppendEntry(entry);
        try {
            writeEntry(channel, entry);
            entries.add(entry);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to append sparse index entry to " + path, e);
        }
    }

    synchronized long floorPosition(long targetOffset) {
        ensureOpenUnchecked();
        int low = 0;
        int high = entries.size() - 1;
        int floor = -1;
        while (low <= high) {
            int middle = (low + high) >>> 1;
            if (entries.get(middle).offset() <= targetOffset) {
                floor = middle;
                low = middle + 1;
            } else {
                high = middle - 1;
            }
        }
        return floor < 0 ? 0 : entries.get(floor).filePosition();
    }

    synchronized void replace(List<IndexEntry> replacement) {
        ensureOpenUnchecked();
        List<IndexEntry> validated = List.copyOf(replacement);
        validateReplacement(validated);
        try {
            channel.truncate(0);
            channel.position(0);
            for (IndexEntry entry : validated) {
                writeEntry(channel, entry);
            }
            entries.clear();
            entries.addAll(validated);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to replace sparse index " + path, e);
        }
    }

    synchronized void flush() {
        try {
            ensureOpen();
            channel.force(false);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to flush sparse index " + path, e);
        }
    }

    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        try {
            channel.force(false);
            channel.close();
            closed = true;
        } catch (IOException e) {
            throw new IllegalStateException("Failed to close sparse index " + path, e);
        }
    }

    private static IndexEntry readEntry(FileChannel channel) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(ENTRY_BYTES);
        readFully(channel, buffer);
        return new IndexEntry(buffer.getLong(0), buffer.getLong(Long.BYTES));
    }

    private static void writeEntry(FileChannel channel, IndexEntry entry) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(ENTRY_BYTES);
        buffer.putLong(entry.offset());
        buffer.putLong(entry.filePosition());
        buffer.flip();
        while (buffer.hasRemaining()) {
            channel.write(buffer);
        }
    }

    private static void readFully(FileChannel channel, ByteBuffer buffer) throws IOException {
        while (buffer.hasRemaining()) {
            int read = channel.read(buffer);
            if (read < 0) {
                throw new IOException("Unexpected EOF while reading sparse index");
            }
        }
    }

    private static void validateLoadedEntry(List<IndexEntry> existing, IndexEntry entry, Path path)
            throws IOException {
        if (entry.offset() < 0 || entry.filePosition() < 0) {
            throw new IOException("Corrupt sparse index " + path + ": negative offset or position");
        }
        if (existing.isEmpty()) {
            return;
        }
        IndexEntry previous = existing.get(existing.size() - 1);
        if (entry.offset() <= previous.offset()) {
            throw new IOException("Corrupt sparse index " + path + ": non-increasing offset");
        }
        if (entry.filePosition() <= previous.filePosition()) {
            throw new IOException("Corrupt sparse index " + path + ": non-increasing file position");
        }
    }

    private void validateAppendEntry(IndexEntry entry) {
        if (entry.offset() < 0 || entry.filePosition() < 0) {
            throw new IllegalArgumentException("sparse index offset and filePosition must be non-negative");
        }
        if (!entries.isEmpty()) {
            IndexEntry previous = entries.get(entries.size() - 1);
            if (entry.offset() <= previous.offset()) {
                throw new IllegalArgumentException("sparse index offset must increase");
            }
            if (entry.filePosition() <= previous.filePosition()) {
                throw new IllegalArgumentException("sparse index filePosition must increase");
            }
        }
    }

    private void validateReplacement(List<IndexEntry> replacement) {
        List<IndexEntry> validated = new ArrayList<>();
        for (IndexEntry entry : replacement) {
            if (entry == null) {
                throw new IllegalArgumentException("sparse index entry is required");
            }
            try {
                validateLoadedEntry(validated, entry, path);
            } catch (IOException e) {
                throw new IllegalArgumentException(e.getMessage(), e);
            }
            validated.add(entry);
        }
    }

    private void ensureOpenUnchecked() {
        try {
            ensureOpen();
        } catch (IOException e) {
            throw new IllegalStateException("Sparse index is closed", e);
        }
    }

    private void ensureOpen() throws IOException {
        if (closed || !channel.isOpen()) {
            throw new ClosedChannelException();
        }
    }

    record IndexEntry(long offset, long filePosition) {
    }
}
