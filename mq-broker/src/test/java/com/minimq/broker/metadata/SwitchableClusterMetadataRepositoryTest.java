package com.minimq.broker.metadata;

import com.minimq.common.error.ErrorCode;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class SwitchableClusterMetadataRepositoryTest {
    @Test
    void forwardsEveryOperationToCurrentDelegate() {
        ClusterMetadataRepository first = mock(ClusterMetadataRepository.class);
        ClusterMetadataRepository second = mock(ClusterMetadataRepository.class);
        ClusterMetadata metadata = new ClusterMetadata();
        VersionedClusterMetadata value = new VersionedClusterMetadata(metadata, 2);
        ClusterMetadataRepository.UpdateResult result =
                new ClusterMetadataRepository.UpdateResult(ErrorCode.NONE, value);
        when(first.read()).thenReturn(value);
        when(second.initialize(metadata)).thenReturn(result);
        when(second.compareAndSet(2, metadata)).thenReturn(result);
        SwitchableClusterMetadataRepository repository =
                new SwitchableClusterMetadataRepository(first);

        assertEquals(value, repository.read());
        repository.switchTo(second);
        assertEquals(result, repository.initialize(metadata));
        assertEquals(result, repository.compareAndSet(2, metadata));
        verify(first).read();
        verify(second).initialize(metadata);
        verify(second).compareAndSet(2, metadata);
        assertThrows(NullPointerException.class,
                () -> new SwitchableClusterMetadataRepository(null));
        assertThrows(NullPointerException.class, () -> repository.switchTo(null));
    }
}
