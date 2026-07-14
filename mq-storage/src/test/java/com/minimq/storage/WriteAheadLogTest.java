package com.minimq.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.zip.CRC32;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WriteAheadLogTest {

    @Test
    void appendAndReadDataAppendRecord(@TempDir Path dir) throws IOException {
        Path walPath = dir.resolve("wal").resolve("minimq.wal");
        try (WriteAheadLog wal = new WriteAheadLog(walPath)) {
            long position = wal.append(WalRecord.dataAppend(
                    "orders",
                    0,
                    12,
                    "key".getBytes(),
                    "value".getBytes()));

            assertEquals(0, position);
        }

        try (WriteAheadLog reopened = new WriteAheadLog(walPath)) {
            List<WalRecord> records = reopened.readAll();

            assertEquals(1, records.size());
            WalRecord record = records.get(0);
            assertEquals(WalRecordType.DATA_APPEND, record.type());
            assertEquals("orders", record.topic());
            assertEquals(0, record.partitionId());
            assertEquals(12, record.offset());
            assertArrayEquals("key".getBytes(), record.key());
            assertArrayEquals("value".getBytes(), record.value());
        }
    }

    @Test
    void appendAndReadNullAndEmptyPayloads(@TempDir Path dir) throws IOException {
        Path walPath = dir.resolve("minimq.wal");
        try (WriteAheadLog wal = new WriteAheadLog(walPath)) {
            wal.append(WalRecord.dataAppend("orders", 1, 0, null, new byte[0]));
        }

        try (WriteAheadLog reopened = new WriteAheadLog(walPath)) {
            WalRecord record = reopened.readAll().get(0);

            assertEquals(null, record.key());
            assertArrayEquals(new byte[0], record.value());
        }
    }

    @Test
    void recoverTruncatesIncompleteTailRecord(@TempDir Path dir) throws IOException {
        Path walPath = dir.resolve("minimq.wal");
        long validSize;
        try (WriteAheadLog wal = new WriteAheadLog(walPath)) {
            wal.append(WalRecord.dataAppend("orders", 0, 0, "k".getBytes(), "v1".getBytes()));
            validSize = wal.size();
        }
        Files.write(walPath, new byte[] {0, 1, 2, 3, 4}, StandardOpenOption.APPEND);

        try (WriteAheadLog recovered = new WriteAheadLog(walPath)) {
            assertEquals(validSize, Files.size(walPath));
            assertEquals(1, recovered.readAll().size());
            recovered.append(WalRecord.dataAppend("orders", 0, 1, "k".getBytes(), "v2".getBytes()));
        }

        try (WriteAheadLog reopened = new WriteAheadLog(walPath)) {
            assertEquals(2, reopened.readAll().size());
        }
    }

    @Test
    void recoveryFailsOnChecksumMismatch(@TempDir Path dir) throws IOException {
        Path walPath = dir.resolve("minimq.wal");
        try (WriteAheadLog wal = new WriteAheadLog(walPath)) {
            wal.append(WalRecord.dataAppend("orders", 0, 0, "k".getBytes(), "v".getBytes()));
        }
        byte[] bytes = Files.readAllBytes(walPath);
        bytes[bytes.length - 1] ^= 0x01;
        Files.write(walPath, bytes);

        IOException failure = assertThrows(IOException.class, () -> new WriteAheadLog(walPath));

        assertTrue(failure.getMessage().contains("checksum mismatch"));
    }

    @Test
    void recoveryFailsOnUnknownRecordType(@TempDir Path dir) throws IOException {
        Path walPath = dir.resolve("minimq.wal");
        try (WriteAheadLog wal = new WriteAheadLog(walPath)) {
            wal.append(WalRecord.dataAppend("orders", 0, 0, "k".getBytes(), "v".getBytes()));
        }
        byte[] bytes = Files.readAllBytes(walPath);
        bytes[Short.BYTES + Byte.BYTES] = 99;
        rewriteChecksum(bytes);
        Files.write(walPath, bytes);

        IOException failure = assertThrows(IOException.class, () -> new WriteAheadLog(walPath));

        assertTrue(failure.getMessage().contains("unknown record type"));
    }

    @Test
    void recordCopiesPayloadArrays() {
        byte[] key = "key".getBytes();
        byte[] value = "value".getBytes();
        WalRecord record = WalRecord.dataAppend("orders", 0, 0, key, value);

        key[0] = 'x';
        value[0] = 'x';

        assertArrayEquals("key".getBytes(), record.key());
        assertArrayEquals("value".getBytes(), record.value());
    }

    @Test
    void recordValidatesRequiredFields() {
        assertThrows(IllegalArgumentException.class,
                () -> new WalRecord(null, "orders", 0, 0, null, null));
        assertThrows(IllegalArgumentException.class,
                () -> WalRecord.dataAppend(" ", 0, 0, null, null));
        assertThrows(IllegalArgumentException.class,
                () -> WalRecord.dataAppend("orders", -1, 0, null, null));
        assertThrows(IllegalArgumentException.class,
                () -> WalRecord.dataAppend("orders", 0, -1, null, null));
    }

    @Test
    void payloadEqualsComparesMetadataAndPayload() {
        WalRecord record = WalRecord.dataAppend("orders", 0, 1, "k".getBytes(), "v".getBytes());

        assertTrue(record.payloadEquals(WalRecord.dataAppend("orders", 0, 1, "k".getBytes(), "v".getBytes())));
        assertTrue(!record.payloadEquals(null));
        assertTrue(!record.payloadEquals(WalRecord.dataAppend("payments", 0, 1, "k".getBytes(), "v".getBytes())));
        assertTrue(!record.payloadEquals(WalRecord.dataAppend("orders", 1, 1, "k".getBytes(), "v".getBytes())));
        assertTrue(!record.payloadEquals(WalRecord.dataAppend("orders", 0, 2, "k".getBytes(), "v".getBytes())));
        assertTrue(!record.payloadEquals(WalRecord.dataAppend("orders", 0, 1, "x".getBytes(), "v".getBytes())));
        assertTrue(!record.payloadEquals(WalRecord.dataAppend("orders", 0, 1, "k".getBytes(), "x".getBytes())));
    }

    @Test
    void rejectsInvalidReadPositions(@TempDir Path dir) throws IOException {
        Path walPath = dir.resolve("minimq.wal");
        try (WriteAheadLog wal = new WriteAheadLog(walPath)) {
            wal.append(WalRecord.dataAppend("orders", 0, 0, "k".getBytes(), "v".getBytes()));

            assertThrows(IllegalStateException.class, () -> wal.readFrom(-1));
            assertThrows(IllegalStateException.class, () -> wal.readFrom(wal.size() + 1));
        }
    }

    @Test
    void recoveryFailsOnUnsupportedVersion(@TempDir Path dir) throws IOException {
        Path walPath = dir.resolve("minimq.wal");
        try (WriteAheadLog wal = new WriteAheadLog(walPath)) {
            wal.append(WalRecord.dataAppend("orders", 0, 0, "k".getBytes(), "v".getBytes()));
        }
        byte[] bytes = Files.readAllBytes(walPath);
        bytes[Short.BYTES] = 99;
        rewriteChecksum(bytes);
        Files.write(walPath, bytes);

        IOException failure = assertThrows(IOException.class, () -> new WriteAheadLog(walPath));

        assertTrue(failure.getMessage().contains("unsupported version"));
    }

    @Test
    void recoveryFailsOnInvalidBodyLength(@TempDir Path dir) throws IOException {
        Path walPath = dir.resolve("minimq.wal");
        try (WriteAheadLog wal = new WriteAheadLog(walPath)) {
            wal.append(WalRecord.dataAppend("orders", 0, 0, "k".getBytes(), "v".getBytes()));
        }
        byte[] bytes = Files.readAllBytes(walPath);
        ByteBuffer.wrap(bytes).putInt(Short.BYTES + Byte.BYTES + Byte.BYTES, -1);
        rewriteChecksum(bytes);
        Files.write(walPath, bytes);

        IOException failure = assertThrows(IOException.class, () -> new WriteAheadLog(walPath));

        assertTrue(failure.getMessage().contains("invalid body length"));
    }

    private void rewriteChecksum(byte[] bytes) {
        CRC32 crc = new CRC32();
        crc.update(bytes, 0, bytes.length - Integer.BYTES);
        ByteBuffer.wrap(bytes).putInt(bytes.length - Integer.BYTES, (int) crc.getValue());
    }
}
