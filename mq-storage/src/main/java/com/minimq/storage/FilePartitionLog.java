package com.minimq.storage;

import com.minimq.common.model.Record;

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
 * File-backed append-only partition log.
 */
public class FilePartitionLog implements PartitionLog {
    private static final int NULL_LENGTH = -1;
    private static final int LONG_BYTES = Long.BYTES;
    private static final int INT_BYTES = Integer.BYTES;
    private static final int MAX_FIELD_LENGTH = 64 * 1024 * 1024;

    private final FileChannel channel;
    private final boolean forceOnAppend;
    private boolean closed;
    private long nextOffset;

    public FilePartitionLog(Path path) throws IOException {
        this(path, true);
    }

    FilePartitionLog(Path path, boolean forceOnAppend) throws IOException {
        this.forceOnAppend = forceOnAppend;
        Files.createDirectories(path.getParent() != null ? path.getParent() : Path.of("."));
        channel = FileChannel.open(path,
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE);
        recoverOffsets();
    }

    private void recoverOffsets() throws IOException {
        nextOffset = 0;
        channel.position(0);
        long fileSize = channel.size();
        long lastOffset = -1;

        while (channel.position() < fileSize) {
            long recordStart = channel.position();
            if (isIncomplete(channel.position(), LONG_BYTES, fileSize)) {
                truncateTail(recordStart);
                return;
            }

            long offset = readLong();
            if (offset != lastOffset + 1) {
                throw new IOException("Corrupt partition log at byte " + recordStart
                        + ": non-monotonic offset " + offset + " after " + lastOffset);
            }

            if (isIncomplete(channel.position(), INT_BYTES, fileSize)) {
                truncateTail(recordStart);
                return;
            }
            int keyLen = readInt();
            validateLength(keyLen, "key", recordStart);
            if (isIncompletePayload(keyLen, fileSize)) {
                truncateTail(recordStart);
                return;
            }
            skipBytes(keyLen);

            if (isIncomplete(channel.position(), INT_BYTES, fileSize)) {
                truncateTail(recordStart);
                return;
            }
            int valueLen = readInt();
            validateLength(valueLen, "value", recordStart);
            if (isIncompletePayload(valueLen, fileSize)) {
                truncateTail(recordStart);
                return;
            }
            skipBytes(valueLen);

            lastOffset = offset;
            nextOffset = offset + 1;
        }
        channel.position(channel.size());
    }

    @Override
    public synchronized long append(byte[] key, byte[] value) {
        try {
            ensureOpen();
            long offset = nextOffset;
            appendAtInternal(offset, key, value);
            return offset;
        } catch (IOException e) {
            throw new IllegalStateException("Failed to append record", e);
        }
    }

    @Override
    public synchronized void appendAt(long offset, byte[] key, byte[] value) {
        ensureOpenUnchecked();
        if (offset < nextOffset) {
            Record existing = readExisting(offset);
            if (existing != null && bytesEqual(existing.key(), key) && bytesEqual(existing.value(), value)) {
                return;
            }
            throw new IllegalStateException("Conflicting record already exists at offset " + offset);
        }
        if (offset > nextOffset) {
            throw new IllegalStateException("Cannot append offset " + offset + " when next offset is " + nextOffset);
        }
        try {
            appendAtInternal(offset, key, value);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to append record at offset " + offset, e);
        }
    }

    @Override
    public synchronized List<Record> read(long fromOffset, int maxBytes) {
        try {
            ensureOpen();
            List<Record> records = new ArrayList<>();
            channel.position(0);
            int bytesRead = 0;

            while (channel.position() < channel.size() && bytesRead < maxBytes) {
                long startPos = channel.position();
                long offset = readLong();
                byte[] key = readBytes();
                byte[] value = readBytes();

                if (offset >= fromOffset) {
                    records.add(new Record(offset, key, value));
                    bytesRead += (int) (channel.position() - startPos);
                }
            }
            return records;
        } catch (IOException e) {
            throw new IllegalStateException("Failed to read records", e);
        }
    }

    @Override
    public synchronized long highWatermark() {
        return nextOffset;
    }

    @Override
    public synchronized void flush() {
        try {
            ensureOpen();
            channel.force(false);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to flush partition log", e);
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
            throw new IllegalStateException("Failed to close partition log", e);
        }
    }

    private void ensureOpenUnchecked() {
        try {
            ensureOpen();
        } catch (IOException e) {
            throw new IllegalStateException("Partition log is closed", e);
        }
    }

    private void ensureOpen() throws IOException {
        if (closed || !channel.isOpen()) {
            throw new ClosedChannelException();
        }
    }

    private void appendAtInternal(long offset, byte[] key, byte[] value) throws IOException {
        channel.position(channel.size());
        writeLong(offset);
        writeBytes(key);
        writeBytes(value);
        if (forceOnAppend) {
            channel.force(false);
        }
        nextOffset = offset + 1;
    }

    private Record readExisting(long offset) {
        List<Record> records = read(offset, Integer.MAX_VALUE);
        for (Record record : records) {
            if (record.offset() == offset) {
                return record;
            }
            if (record.offset() > offset) {
                return null;
            }
        }
        return null;
    }

    private boolean bytesEqual(byte[] left, byte[] right) {
        return java.util.Arrays.equals(left, right);
    }

    private void writeLong(long value) throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(LONG_BYTES);
        buf.putLong(value);
        buf.flip();
        writeFully(buf);
    }

    private void writeInt(int value) throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(INT_BYTES);
        buf.putInt(value);
        buf.flip();
        writeFully(buf);
    }

    private void writeBytes(byte[] data) throws IOException {
        if (data == null) {
            writeInt(NULL_LENGTH);
            return;
        }
        validateLength(data.length, "record field", channel.position());
        writeInt(data.length);
        if (data.length > 0) {
            writeFully(ByteBuffer.wrap(data));
        }
    }

    private void writeFully(ByteBuffer buf) throws IOException {
        while (buf.hasRemaining()) {
            channel.write(buf);
        }
    }

    private long readLong() throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(LONG_BYTES);
        readFully(buf);
        return buf.getLong(0);
    }

    private int readInt() throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(INT_BYTES);
        readFully(buf);
        return buf.getInt(0);
    }

    private byte[] readBytes() throws IOException {
        long lengthPosition = channel.position();
        int len = readInt();
        validateLength(len, "record field", lengthPosition);
        if (len == NULL_LENGTH) {
            return null;
        }
        if (len == 0) {
            return new byte[0];
        }
        ByteBuffer buf = ByteBuffer.allocate(len);
        readFully(buf);
        return buf.array();
    }

    private void skipBytes(int len) throws IOException {
        validateLength(len, "record field", channel.position());
        if (len == NULL_LENGTH || len == 0) {
            return;
        }
        if (isIncomplete(channel.position(), len, channel.size())) {
            throw new IOException("Unexpected EOF");
        }
        channel.position(channel.position() + len);
    }

    private boolean isIncompletePayload(int len, long fileSize) throws IOException {
        validateLength(len, "record field", channel.position());
        return len > 0 && isIncomplete(channel.position(), len, fileSize);
    }

    private boolean isIncomplete(long position, long byteCount, long fileSize) {
        return fileSize - position < byteCount;
    }

    private void truncateTail(long validEnd) throws IOException {
        channel.truncate(validEnd);
        channel.position(validEnd);
    }

    private void validateLength(int len, String fieldName, long recordStart) throws IOException {
        if (len < NULL_LENGTH || len > MAX_FIELD_LENGTH) {
            throw new IOException("Corrupt partition log at byte " + recordStart
                    + ": invalid " + fieldName + " length " + len);
        }
    }

    private void readFully(ByteBuffer buf) throws IOException {
        while (buf.hasRemaining()) {
            int n = channel.read(buf);
            if (n < 0) {
                throw new IOException("Unexpected EOF");
            }
        }
    }
}
