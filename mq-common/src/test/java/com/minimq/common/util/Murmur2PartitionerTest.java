package com.minimq.common.util;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class Murmur2PartitionerTest {

    @Test
    void sameKeySamePartition() {
        byte[] key = "user-1".getBytes(StandardCharsets.UTF_8);
        int p1 = Murmur2Partitioner.partition(key, 3);
        int p2 = Murmur2Partitioner.partition(key, 3);
        assertEquals(p1, p2);
        assertTrue(p1 >= 0 && p1 < 3);
    }

    @Test
    void rejectsNonPositivePartitionCount() {
        byte[] key = "user-1".getBytes(StandardCharsets.UTF_8);

        assertThrows(IllegalArgumentException.class, () -> Murmur2Partitioner.partition(key, 0));
        assertThrows(IllegalArgumentException.class, () -> Murmur2Partitioner.partition(key, -1));
    }
}
