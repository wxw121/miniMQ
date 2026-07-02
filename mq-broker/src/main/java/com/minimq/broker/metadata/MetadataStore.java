package com.minimq.broker.metadata;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class MetadataStore {
    private final Path metadataFile;
    private final ObjectMapper mapper = new ObjectMapper();

    public MetadataStore(Path dataDir) {
        this.metadataFile = dataDir.resolve("cluster-metadata.json");
    }

    public synchronized ClusterMetadata load() {
        if (!Files.exists(metadataFile)) {
            return new ClusterMetadata();
        }
        try {
            return mapper.readValue(metadataFile.toFile(), ClusterMetadata.class);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to load metadata", e);
        }
    }

    public synchronized void save(ClusterMetadata metadata) {
        try {
            Files.createDirectories(metadataFile.getParent());
            mapper.writerWithDefaultPrettyPrinter().writeValue(metadataFile.toFile(), metadata);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to save metadata", e);
        }
    }
}
