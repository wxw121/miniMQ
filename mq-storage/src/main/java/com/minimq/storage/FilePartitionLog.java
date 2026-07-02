package com.minimq.storage;

import com.minimq.common.model.Record;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

/**
 * 文件型分区日志实现，使用追加写文件保存 record，并在启动时扫描恢复 nextOffset。
 */
public class FilePartitionLog implements PartitionLog {
    private static final int NULL_LENGTH = -1;

    private final FileChannel channel;
    private long nextOffset;

    public FilePartitionLog(Path path) throws IOException {
        Files.createDirectories(path.getParent() != null ? path.getParent() : Path.of("."));
        channel = FileChannel.open(path,
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE);
        recoverOffsets();
    }

    // 启动恢复通过顺序扫描日志计算 nextOffset，不依赖额外索引文件。
    private void recoverOffsets() throws IOException {
        nextOffset = 0;
        channel.position(0);
        while (channel.position() < channel.size()) {
            long offset = readLong();
            int keyLen = readInt();
            skipBytes(keyLen);
            int valueLen = readInt();
            skipBytes(valueLen);
            nextOffset = offset + 1;
        }
    }

    @Override
    // append 串行写入 offset、key、value，并 force 到磁盘以降低测试场景中的数据丢失风险。
    public synchronized long append(byte[] key, byte[] value) {
        try {
            long offset = nextOffset++;
            channel.position(channel.size());
            writeLong(offset);
            writeBytes(key);
            writeBytes(value);
            channel.force(false);
            return offset;
        } catch (IOException e) {
            throw new IllegalStateException("Failed to append record", e);
        }
    }

    @Override
    // read 从文件头开始扫描，跳过小于 fromOffset 的记录，并用 maxBytes 控制单次响应大小。
    public synchronized List<Record> read(long fromOffset, int maxBytes) {
        try {
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

    private void writeLong(long value) throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(8);
        buf.putLong(value);
        buf.flip();
        writeFully(buf);
    }

    private void writeInt(int value) throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(4);
        buf.putInt(value);
        buf.flip();
        writeFully(buf);
    }

    private void writeBytes(byte[] data) throws IOException {
        if (data == null) {
            writeInt(NULL_LENGTH);
            return;
        }
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
        ByteBuffer buf = ByteBuffer.allocate(8);
        readFully(buf);
        return buf.getLong(0);
    }

    private int readInt() throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(4);
        readFully(buf);
        return buf.getInt(0);
    }

    private byte[] readBytes() throws IOException {
        int len = readInt();
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
        if (len == NULL_LENGTH || len <= 0) {
            return;
        }
        channel.position(channel.position() + len);
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
