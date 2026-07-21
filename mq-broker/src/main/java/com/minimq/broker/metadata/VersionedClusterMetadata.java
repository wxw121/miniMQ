package com.minimq.broker.metadata;

/** A cluster metadata snapshot paired with the repository revision used for CAS. */
public record VersionedClusterMetadata(ClusterMetadata metadata, long revision) {
    public VersionedClusterMetadata {
        if (metadata == null) {
            throw new IllegalArgumentException("metadata must not be null");
        }
        if (revision < 0) {
            throw new IllegalArgumentException("revision must be non-negative");
        }
    }
}
