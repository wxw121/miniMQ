package com.minimq.broker.metadata;

import com.minimq.common.error.ErrorCode;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Modifier;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class SwitchableClusterMetadataRepositoryTest {
    @Test
    void deadlineOverloadsAreExplicitRepositoryContractMethods() throws Exception {
        assertTrue(Modifier.isAbstract(ClusterMetadataRepository.class.getMethod(
                "read", Duration.class).getModifiers()));
        assertTrue(Modifier.isAbstract(ClusterMetadataRepository.class.getMethod(
                "initialize", ClusterMetadata.class, Duration.class).getModifiers()));
        assertTrue(Modifier.isAbstract(ClusterMetadataRepository.class.getMethod(
                "compareAndSet", long.class, ClusterMetadata.class, Duration.class)
                .getModifiers()));
    }

    @Test
    void forwardsEveryOperationToCurrentDelegate() {
        ClusterMetadataRepository first = mock(ClusterMetadataRepository.class);
        ClusterMetadataRepository second = mock(ClusterMetadataRepository.class);
        Duration timeout = Duration.ofMillis(25L);
        ClusterMetadata metadata = new ClusterMetadata();
        VersionedClusterMetadata value = new VersionedClusterMetadata(metadata, 2);
        ClusterMetadataRepository.UpdateResult result =
                new ClusterMetadataRepository.UpdateResult(ErrorCode.NONE, value);
        when(first.read()).thenReturn(value);
        when(second.initialize(metadata)).thenReturn(result);
        when(second.compareAndSet(2, metadata)).thenReturn(result);
        when(first.read(timeout)).thenReturn(value);
        when(second.initialize(metadata, timeout)).thenReturn(result);
        when(second.compareAndSet(2, metadata, timeout)).thenReturn(result);
        SwitchableClusterMetadataRepository repository =
                new SwitchableClusterMetadataRepository(first);

        assertEquals(value, repository.read());
        assertEquals(value, repository.read(timeout));
        repository.switchTo(second);
        assertEquals(result, repository.initialize(metadata));
        assertEquals(result, repository.compareAndSet(2, metadata));
        assertEquals(result, repository.initialize(metadata, timeout));
        assertEquals(result, repository.compareAndSet(2, metadata, timeout));
        verify(first).read();
        verify(first).read(timeout);
        verify(second).initialize(metadata);
        verify(second).compareAndSet(2, metadata);
        verify(second).initialize(metadata, timeout);
        verify(second).compareAndSet(2, metadata, timeout);
        assertThrows(NullPointerException.class,
                () -> new SwitchableClusterMetadataRepository(null));
        assertThrows(NullPointerException.class, () -> repository.switchTo(null));
    }
}
