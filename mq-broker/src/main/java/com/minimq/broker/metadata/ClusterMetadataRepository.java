package com.minimq.broker.metadata;

import com.minimq.common.error.ErrorCode;

import java.time.Duration;

/** Versioned persistence boundary for all shared cluster metadata writes. */
public interface ClusterMetadataRepository {
    VersionedClusterMetadata read();

    /** Reads metadata within the caller-provided operation budget. */
    VersionedClusterMetadata read(Duration timeout);

    UpdateResult initialize(ClusterMetadata metadata);

    /** Initializes metadata within the caller-provided operation budget. */
    UpdateResult initialize(ClusterMetadata metadata, Duration timeout);

    UpdateResult compareAndSet(long expectedRevision, ClusterMetadata metadata);

    /** Performs a revision CAS within the caller-provided operation budget. */
    UpdateResult compareAndSet(
            long expectedRevision, ClusterMetadata metadata, Duration timeout);

    record UpdateResult(ErrorCode errorCode, VersionedClusterMetadata value) {
        public UpdateResult {
            if (errorCode == null || value == null) {
                throw new IllegalArgumentException("errorCode and value must not be null");
            }
        }
    }
}
