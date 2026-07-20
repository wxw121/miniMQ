package com.minimq.broker.metadata;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class EtcdClusterMetadataRepositoryTest {
    @Test
    void validatesClusterId() {
        assertEquals("/minimq/demo/metadata/snapshot",
                EtcdClusterMetadataRepository.snapshotKey("demo"));
        assertThrows(IllegalArgumentException.class,
                () -> EtcdClusterMetadataRepository.snapshotKey(" "));
    }
}
