package com.minimq.storage;

import java.nio.file.Path;
import java.util.Objects;

public record FileRegionSlice(Path path, long position, long length) {
    public FileRegionSlice {
        Objects.requireNonNull(path, "path");
        if (position < 0) {
            throw new IllegalArgumentException("file region position must be non-negative");
        }
        if (length <= 0) {
            throw new IllegalArgumentException("file region length must be positive");
        }
    }
}
