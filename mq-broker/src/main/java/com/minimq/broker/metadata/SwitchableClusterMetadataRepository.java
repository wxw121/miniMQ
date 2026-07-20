package com.minimq.broker.metadata;

import java.util.Objects;

/** Keeps a stable repository identity while Controller leadership changes its fenced backend. */
public final class SwitchableClusterMetadataRepository implements ClusterMetadataRepository {
    private volatile ClusterMetadataRepository delegate;

    public SwitchableClusterMetadataRepository(ClusterMetadataRepository initial) {
        delegate = Objects.requireNonNull(initial, "initial");
    }

    public void switchTo(ClusterMetadataRepository next) {
        delegate = Objects.requireNonNull(next, "next");
    }

    @Override
    public VersionedClusterMetadata read() {
        return delegate.read();
    }

    @Override
    public UpdateResult initialize(ClusterMetadata metadata) {
        return delegate.initialize(metadata);
    }

    @Override
    public UpdateResult compareAndSet(long expectedRevision, ClusterMetadata metadata) {
        return delegate.compareAndSet(expectedRevision, metadata);
    }
}
