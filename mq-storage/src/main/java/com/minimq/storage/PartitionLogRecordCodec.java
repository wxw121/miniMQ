package com.minimq.storage;

import com.minimq.common.model.Record;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Binary framing shared by legacy-compatible partition log implementations.
 */
final class PartitionLogRecordCodec {
    static final int NULL_LENGTH = -1;
    static final int MAX_FIELD_LENGTH = 64 * 1024 * 1024;

    private PartitionLogRecordCodec() {
    }

    static int encodedSize(long offset, byte[] key, byte[] value) {
        validateOffset(offset);
        validateWriteField(key, "key");
        validateWriteField(value, "value");
        return Long.BYTES + Integer.BYTES + payloadLength(key) + Integer.BYTES + payloadLength(value);
    }

    static void write(FileChannel channel, long offset, byte[] key, byte[] value) throws IOException {
        encodedSize(offset, key, value);
        writeLong(channel, offset);
        writeField(channel, key);
        writeField(channel, value);
    }

    static DecodedRecord read(FileChannel channel, long startPosition, long fileSize) throws IOException {
        if (startPosition < 0 || startPosition > fileSize) {
            throw new IOException("Invalid partition log position " + startPosition);
        }
        channel.position(startPosition);
        long offset = readLong(channel, fileSize);
        if (offset < 0) {
            throw new IOException("Corrupt partition log at byte " + startPosition + ": negative offset " + offset);
        }
        byte[] key = readField(channel, fileSize, "key", startPosition);
        byte[] value = readField(channel, fileSize, "value", startPosition);
        long nextPosition = channel.position();
        return new DecodedRecord(new Record(offset, key, value), startPosition, nextPosition,
                Math.toIntExact(nextPosition - startPosition));
    }

    static ScannedRecord scan(FileChannel channel, long startPosition, long fileSize) throws IOException {
        if (startPosition < 0 || startPosition > fileSize) {
            throw new IOException("Invalid partition log position " + startPosition);
        }
        channel.position(startPosition);
        long offset = readLong(channel, fileSize);
        if (offset < 0) {
            throw new IOException("Corrupt partition log at byte " + startPosition
                    + ": negative offset " + offset);
        }
        skipField(channel, fileSize, "key", startPosition);
        skipField(channel, fileSize, "value", startPosition);
        long nextPosition = channel.position();
        return new ScannedRecord(offset, startPosition, nextPosition,
                Math.toIntExact(nextPosition - startPosition));
    }

    private static void writeLong(FileChannel channel, long value) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(value);
        buffer.flip();
        writeFully(channel, buffer);
    }

    private static void writeField(FileChannel channel, byte[] data) throws IOException {
        if (data == null) {
            writeInt(channel, NULL_LENGTH);
            return;
        }
        writeInt(channel, data.length);
        if (data.length > 0) {
            writeFully(channel, ByteBuffer.wrap(data));
        }
    }

    private static void writeInt(FileChannel channel, int value) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
        buffer.putInt(value);
        buffer.flip();
        writeFully(channel, buffer);
    }

    private static long readLong(FileChannel channel, long fileSize) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        readFully(channel, buffer, fileSize);
        return buffer.getLong(0);
    }

    private static byte[] readField(FileChannel channel, long fileSize, String fieldName, long recordStart)
            throws IOException {
        ByteBuffer lengthBuffer = ByteBuffer.allocate(Integer.BYTES);
        readFully(channel, lengthBuffer, fileSize);
        int length = lengthBuffer.getInt(0);
        validateFieldLength(length, fieldName, recordStart);
        if (length == NULL_LENGTH) {
            return null;
        }
        ByteBuffer payload = ByteBuffer.allocate(length);
        readFully(channel, payload, fileSize);
        return payload.array();
    }

    private static void skipField(
            FileChannel channel, long fileSize, String fieldName, long recordStart) throws IOException {
        ByteBuffer lengthBuffer = ByteBuffer.allocate(Integer.BYTES);
        readFully(channel, lengthBuffer, fileSize);
        int length = lengthBuffer.getInt(0);
        validateFieldLength(length, fieldName, recordStart);
        if (length > 0) {
            if (fileSize - channel.position() < length) {
                throw new IncompleteRecordException();
            }
            channel.position(channel.position() + length);
        }
    }

    private static void validateFieldLength(int length, String fieldName, long recordStart) throws IOException {
        if (length < NULL_LENGTH || length > MAX_FIELD_LENGTH) {
            throw new IOException("Corrupt partition log at byte " + recordStart
                    + ": invalid " + fieldName + " length " + length);
        }
    }

    private static void writeFully(FileChannel channel, ByteBuffer buffer) throws IOException {
        while (buffer.hasRemaining()) {
            channel.write(buffer);
        }
    }

    private static void readFully(FileChannel channel, ByteBuffer buffer, long fileSize) throws IOException {
        if (fileSize - channel.position() < buffer.remaining()) {
            throw new IncompleteRecordException();
        }
        while (buffer.hasRemaining()) {
            int read = channel.read(buffer);
            if (read < 0) {
                throw new IncompleteRecordException();
            }
        }
    }

    private static int payloadLength(byte[] data) {
        return data == null ? 0 : data.length;
    }

    private static void validateOffset(long offset) {
        if (offset < 0) {
            throw new IllegalArgumentException("partition log offset must be non-negative");
        }
    }

    private static void validateWriteField(byte[] data, String fieldName) {
        if (data != null && data.length > MAX_FIELD_LENGTH) {
            throw new IllegalArgumentException("partition log " + fieldName + " length exceeds maximum");
        }
    }

    record DecodedRecord(Record record, long startPosition, long nextPosition, int encodedBytes) {
    }

    record ScannedRecord(long offset, long startPosition, long nextPosition, int encodedBytes) {
    }

    static final class IncompleteRecordException extends IOException {
    }
}
