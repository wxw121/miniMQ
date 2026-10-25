package com.minimq.common.acks;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MinInSyncReplicasTest {

    @Test
    void treatsMissingOrZeroAsDefault() {
        assertEquals(1, MinInSyncReplicas.effective(0));
        assertEquals(1, MinInSyncReplicas.effective(-1));
        assertEquals(2, MinInSyncReplicas.effective(2));
    }

    @Test
    void validatesAgainstReplicationFactor() {
        assertTrue(MinInSyncReplicas.isValid(1, 1));
        assertTrue(MinInSyncReplicas.isValid(2, 3));
        assertFalse(MinInSyncReplicas.isValid(0, 1));
        assertFalse(MinInSyncReplicas.isValid(3, 2));
        MinInSyncReplicas.requireValid(1, 1);
        MinInSyncReplicas.requireRange(MinInSyncReplicas.MAX);
        assertThrows(IllegalArgumentException.class, () -> MinInSyncReplicas.requireRange(0));
        assertThrows(IllegalArgumentException.class,
                () -> MinInSyncReplicas.requireValid(3, 2));
    }
}
