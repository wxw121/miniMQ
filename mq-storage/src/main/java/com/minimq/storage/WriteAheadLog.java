package com.minimq.storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32;

/**
 * File-backed write-ahead log for durable append intent.
 */
public class WriteAheadLog implements AutoCloseable {
    static final short MAGIC = 0x4D57;
    static final byte VERSION = 1;

    private static final int NULL_LENGTH = -1;
    private static final int HEADER_BYTES = Short.BYTES + Byte.BYTES + Byte.BYTES + Integer.BYTES;
    private static final int CRC_BYTES = Integer.BYTES;
    private static final int MAX_FIELD_LENGTH = 64 * 1024 * 1024;
    private static final int MAX_TOPIC_BYTES = 1024;

    private final FileChannel channel;

    public WriteAheadLog(Path path) throws IOException {
        Files.createDirectories(path.getParent() != null ? path.getParent() : Path.of("."));
        this.channel = FileChannel.open(path,
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE);
        recoverTail();
    }

    public synchronized long append(WalRecord record) {
        return append(record, true);
    }

    synchronized long append(WalRecord record, boolean forceOnAppend) {
        try {
            long position = channel.size();
            channel.position(position);
            byte[] body = encodeBody(record);
            ByteBuffer frame = ByteBuffer.allocate(HEADER_BYTES + body.length + CRC_BYTES);
            frame.putShort(MAGIC);
            frame.put(VERSION);
            frame.put(record.type().id());
            frame.putInt(body.length);
            frame.put(body);
            frame.putInt(checksum(frame.array(), 0, HEADER_BYTES + body.length));
            frame.flip();
            writeFully(frame);
            if (forceOnAppend) {
                channel.force(false);
            }
            return position;
        } catch (IOException e) {
            throw new IllegalStateException("Failed to append WAL record", e);
        }
    }

    public synchronized List<WalRecord> readAll() {
        return readFrom(0);
    }

    public synchronized List<WalRecord> readFrom(long position) {
        try {
            List<WalRecord> records = new ArrayList<>();
            if (position < 0 || position > channel.size()) {
                throw new IOException("Invalid WAL read position " + position);
            }
            channel.position(position);
            while (channel.position() < channel.size()) {
                records.add(readRecord(channel.position(), channel.size(), false));
            }
            return records;
        } catch (IOException e) {
            throw new IllegalStateException("Failed to read WAL records", e);
        }
    }

    public synchronized long size() {
        try {
            return channel.size();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to read WAL size", e);
        }
    }

    public synchronized void flush() {
        try {
            channel.force(false);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to flush WAL", e);
        }
    }

    @Override
    public synchronized void close() throws IOException {
        channel.force(false);
        channel.close();
    }

    private void recoverTail() throws IOException {
        channel.position(0);
        long fileSize = channel.size();
        while (channel.position() < fileSize) {
            long recordStart = channel.position();
            try {
                readRecord(recordStart, fileSize, true);
            } catch (IncompleteWalRecordException incomplete) {
                truncateTail(recordStart);
                return;
            }
        }
        channel.position(channel.size());
    }

    private WalRecord readRecord(long recordStart, long fileSize, boolean recovering) throws IOException {
        if (isIncomplete(channel.position(), HEADER_BYTES, fileSize)) {
            throw new IncompleteWalRecordException();
        }
        ByteBuffer header = ByteBuffer.allocate(HEADER_BYTES);
        readFully(header);
        byte[] headerBytes = header.array();
        short magic = header.getShort(0);
        byte version = header.get(Short.BYTES);
        byte typeId = header.get(Short.BYTES + Byte.BYTES);
        int bodyLength = header.getInt(Short.BYTES + Byte.BYTES + Byte.BYTES);

        if (magic != MAGIC) {
            throw new IOException("Corrupt WAL at byte " + recordStart + ": invalid magic " + magic);
        }
        if (version != VERSION) {
            throw new IOException("Corrupt WAL at byte " + recordStart + ": unsupported version " + version);
        }
        if (bodyLength < 0 || bodyLength > MAX_FIELD_LENGTH) {
            throw new IOException("Corrupt WAL at byte " + recordStart + ": invalid body length " + bodyLength);
        }
        if (isIncomplete(channel.position(), bodyLength + CRC_BYTES, fileSize)) {
            throw new IncompleteWalRecordException();
        }

        ByteBuffer body = ByteBuffer.allocate(bodyLength);
        readFully(body);
        int storedChecksum = readInt();
        ByteBuffer checksumInput = ByteBuffer.allocate(HEADER_BYTES + bodyLength);
        checksumInput.put(headerBytes);
        checksumInput.put(body.array());
        int actualChecksum = checksum(checksumInput.array(), 0, checksumInput.array().length);
        if (storedChecksum != actualChecksum) {
            throw new IOException("Corrupt WAL at byte " + recordStart + ": checksum mismatch");
        }

        WalRecordType type = WalRecordType.fromId(typeId, recordStart);
        return decodeBody(type, body.array(), recordStart);
    }

    private byte[] encodeBody(WalRecord record) throws IOException {
        byte[] topicBytes = record.topic().getBytes(StandardCharsets.UTF_8);
        validateTopicLength(topicBytes.length, 0);
        validateFieldLength(record.key() == null ? NULL_LENGTH : record.key().length, "key", 0);
        validateFieldLength(record.value() == null ? NULL_LENGTH : record.value().length, "value", 0);

        int bodyLength = Short.BYTES + topicBytes.length
                + Integer.BYTES
                + Long.BYTES
                + Integer.BYTES + payloadLength(record.key())
                + Integer.BYTES + payloadLength(record.value());
        ByteBuffer body = ByteBuffer.allocate(bodyLength);
        body.putShort((short) topicBytes.length);
        body.put(topicBytes);
        body.putInt(record.partitionId());
        body.putLong(record.offset());
        putBytes(body, record.key());
        putBytes(body, record.value());
        return body.array();
    }

    private WalRecord decodeBody(WalRecordType type, byte[] bodyBytes, long recordStart) throws IOException {
        ByteBuffer body = ByteBuffer.wrap(bodyBytes);
        if (body.remaining() < Short.BYTES) {
            throw new IOException("Corrupt WAL at byte " + recordStart + ": missing topic length");
        }
        int topicLength = Short.toUnsignedInt(body.getShort());
        validateTopicLength(topicLength, recordStart);
        if (body.remaining() < topicLength + Integer.BYTES + Long.BYTES + Integer.BYTES + Integer.BYTES) {
            throw new IOException("Corrupt WAL at byte " + recordStart + ": incomplete body");
        }
        byte[] topicBytes = new byte[topicLength];
        body.get(topicBytes);
        String topic = new String(topicBytes, StandardCharsets.UTF_8);
        int partitionId = body.getInt();
        long offset = body.getLong();
        byte[] key = getBytes(body, "key", recordStart);
        byte[] value = getBytes(body, "value", recordStart);
        if (body.hasRemaining()) {
            throw new IOException("Corrupt WAL at byte " + recordStart + ": trailing body bytes");
        }
        return new WalRecord(type, topic, partitionId, offset, key, value);
    }

    private void putBytes(ByteBuffer body, byte[] data) {
        if (data == null) {
            body.putInt(NULL_LENGTH);
            return;
        }
        body.putInt(data.length);
        body.put(data);
    }

    private byte[] getBytes(ByteBuffer body, String fieldName, long recordStart) throws IOException {
        if (body.remaining() < Integer.BYTES) {
            throw new IOException("Corrupt WAL at byte " + recordStart + ": missing " + fieldName + " length");
        }
        int length = body.getInt();
        validateFieldLength(length, fieldName, recordStart);
        if (length == NULL_LENGTH) {
            return null;
        }
        if (body.remaining() < length) {
            throw new IOException("Corrupt WAL at byte " + recordStart + ": incomplete " + fieldName + " payload");
        }
        byte[] data = new byte[length];
        body.get(data);
        return data;
    }

    private int readInt() throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(Integer.BYTES);
        readFully(buf);
        return buf.getInt(0);
    }

    private void readFully(ByteBuffer buf) throws IOException {
        while (buf.hasRemaining()) {
            int n = channel.read(buf);
            if (n < 0) {
                throw new IncompleteWalRecordException();
            }
        }
    }

    private void writeFully(ByteBuffer buf) throws IOException {
        while (buf.hasRemaining()) {
            channel.write(buf);
        }
    }

    private int checksum(byte[] data, int offset, int length) {
        CRC32 crc = new CRC32();
        crc.update(data, offset, length);
        return (int) crc.getValue();
    }

    private int payloadLength(byte[] data) {
        return data == null ? 0 : data.length;
    }

    private void validateTopicLength(int length, long recordStart) throws IOException {
        if (length <= 0 || length > MAX_TOPIC_BYTES) {
            throw new IOException("Corrupt WAL at byte " + recordStart + ": invalid topic length " + length);
        }
    }

    private void validateFieldLength(int length, String fieldName, long recordStart) throws IOException {
        if (length < NULL_LENGTH || length > MAX_FIELD_LENGTH) {
            throw new IOException("Corrupt WAL at byte " + recordStart
                    + ": invalid " + fieldName + " length " + length);
        }
    }

    private boolean isIncomplete(long position, long byteCount, long fileSize) {
        return fileSize - position < byteCount;
    }

    private void truncateTail(long validEnd) throws IOException {
        channel.truncate(validEnd);
        channel.position(validEnd);
    }

    private static class IncompleteWalRecordException extends IOException {
    }
}
