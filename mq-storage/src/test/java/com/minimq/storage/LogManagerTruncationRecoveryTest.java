package com.minimq.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class LogManagerTruncationRecoveryTest {
    @TempDir Path tempDir;

    @Test
    void committedTruncationSurvivesManagerRestart() {
        LogManager first = new LogManager(tempDir);
        for (int i = 0; i < 6; i++) {
            first.append("orders", 0, null, new byte[]{(byte) i});
        }
        first.truncateTo("orders", 0, 3);
        first.close();

        try (LogManager reopened = new LogManager(tempDir)) {
            assertEquals(3, reopened.getOrCreate("orders", 0).highWatermark());
            assertEquals(List.of(0L, 1L, 2L), reopened.getOrCreate("orders", 0)
                    .read(0, 1024).stream().map(record -> record.offset()).toList());
        }
    }
}
