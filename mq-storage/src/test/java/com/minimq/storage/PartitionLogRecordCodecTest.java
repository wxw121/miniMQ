package com.minimq.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PartitionLogRecordCodecTest {

    @Test
    void roundTripsNullAndEmptyFields(@TempDir Path dir) throws Exception {
        Path path = dir.resolve("records.log");
        try (FileChannel channel = FileChannel.open(path, CREATE, READ, WRITE)) {
            PartitionLogRecordCodec.write(channel, 7, null, new byte[0]);

            PartitionLogRecordCodec.DecodedRecord decoded =
                    PartitionLogRecordCodec.read(channel, 0, channel.size());

            assertEquals(7, decoded.record().offset());
            assertNull(decoded.record().key());
            assertArrayEquals(new byte[0], decoded.record().value());
            assertEquals(channel.size(), decoded.nextPosition());
        }
    }

    @Test
    void classifiesTruncatedPayloadAsIncomplete(@TempDir Path dir) throws Exception {
        Path path = dir.resolve("truncated.log");
        ByteBuffer truncated = ByteBuffer.allocate(Long.BYTES + Integer.BYTES);
        truncated.putLong(0);
        truncated.putInt(1);
        Files.write(path, truncated.array());

        try (FileChannel channel = FileChannel.open(path, READ, WRITE)) {
            assertThrows(PartitionLogRecordCodec.IncompleteRecordException.class,
                    () -> PartitionLogRecordCodec.read(channel, 0, channel.size()));
        }
    }

    @Test
    void rejectsNegativeOffsetsAndInvalidStoredFieldLengths(@TempDir Path dir) throws Exception {
        Path path = dir.resolve("invalid-length.log");
        ByteBuffer invalid = ByteBuffer.allocate(Long.BYTES + Integer.BYTES);
        invalid.putLong(0);
        invalid.putInt(-2);
        Files.write(path, invalid.array());

        try (FileChannel channel = FileChannel.open(path, READ, WRITE)) {
            assertThrows(IOException.class, () -> PartitionLogRecordCodec.read(channel, 0, channel.size()));
        }

        assertThrows(IllegalArgumentException.class,
                () -> PartitionLogRecordCodec.encodedSize(-1, null, null));
    }

    @Test
    void scansRecordMetadataWithoutMaterializingPayload(@TempDir Path dir) throws Exception {
        Path path = dir.resolve("segment.log");
        byte[] key = "key".getBytes(StandardCharsets.UTF_8);
        byte[] value = "value".getBytes(StandardCharsets.UTF_8);
        try (FileChannel channel = FileChannel.open(path, CREATE, READ, WRITE)) {
            PartitionLogRecordCodec.write(channel, 7, key, value);
            long fileSize = channel.size();

            PartitionLogRecordCodec.ScannedRecord scanned =
                    PartitionLogRecordCodec.scan(channel, 0, fileSize);

            assertEquals(7, scanned.offset());
            assertEquals(0, scanned.startPosition());
            assertEquals(fileSize, scanned.nextPosition());
            assertEquals(fileSize, scanned.encodedBytes());
            assertEquals(fileSize, channel.position());
        }
    }

    @Test
    void scannerRejectsTheSameInvalidLengthsAsFullDecode(@TempDir Path dir) throws Exception {
        Path path = dir.resolve("corrupt.log");
        ByteBuffer invalid = ByteBuffer.allocate(Long.BYTES + Integer.BYTES);
        invalid.putLong(0);
        invalid.putInt(-2);
        Files.write(path, invalid.array());

        try (FileChannel channel = FileChannel.open(path, READ, WRITE)) {
            long fileSize = channel.size();
            assertThrows(IOException.class,
                    () -> PartitionLogRecordCodec.scan(channel, 0, fileSize));
        }
    }

    @Test
    void fileRegionSliceRejectsInvalidRanges(@TempDir Path dir) {
        Path path = dir.resolve("segment.log");

        assertThrows(IllegalArgumentException.class, () -> new FileRegionSlice(path, -1, 1));
        assertThrows(IllegalArgumentException.class, () -> new FileRegionSlice(path, 0, 0));
        assertThrows(NullPointerException.class, () -> new FileRegionSlice(null, 0, 1));
    }
}
