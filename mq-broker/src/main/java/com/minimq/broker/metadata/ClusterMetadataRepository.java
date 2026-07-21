package com.minimq.broker.metadata;

import com.minimq.common.error.ErrorCode;

/** Versioned persistence boundary for all shared cluster metadata writes. */
public interface ClusterMetadataRepository {
    VersionedClusterMetadata read();

    UpdateResult initialize(ClusterMetadata metadata);

    UpdateResult compareAndSet(long expectedRevision, ClusterMetadata metadata);

    record UpdateResult(ErrorCode errorCode, VersionedClusterMetadata value) {
        public UpdateResult {
            if (errorCode == null || value == null) {
                throw new IllegalArgumentException("errorCode and value must not be null");
            }
        }
    }
}
