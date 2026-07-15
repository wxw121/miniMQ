package com.minimq.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
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
}
