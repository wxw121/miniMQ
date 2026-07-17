package com.minimq.storage;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * Safely publishes and loads one partition's segment catalog.
 */
final class SegmentCatalogStore {
    private final Path catalogFile;
    private final ObjectMapper mapper = new ObjectMapper();

    SegmentCatalogStore(Path partitionDir) {
        this.catalogFile = partitionDir.resolve("catalog.json");
    }

    Optional<SegmentCatalog> load() throws IOException {
        if (!Files.exists(catalogFile)) {
            return Optional.empty();
        }
        if (Files.size(catalogFile) == 0) {
            throw new IOException("Corrupt segment catalog " + catalogFile + ": empty file");
        }
        try {
            return Optional.of(mapper.readValue(catalogFile.toFile(), SegmentCatalog.class));
        } catch (IOException | IllegalArgumentException e) {
            throw new IOException("Failed to load segment catalog " + catalogFile, e);
        }
    }

    void cleanupMaintenanceArtifacts() throws IOException {
        Path parent = catalogFile.getParent();
        if (parent == null || !Files.exists(parent)) {
            return;
        }
        try (Stream<Path> paths = Files.list(parent)) {
            for (Path path : paths.filter(this::isMaintenanceArtifact).toList()) {
                Files.deleteIfExists(path);
            }
        }
    }

    void save(SegmentCatalog catalog) {
        try {
            Path parent = catalogFile.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            Path tempFile = catalogFile.resolveSibling(catalogFile.getFileName() + ".tmp");
            mapper.writerWithDefaultPrettyPrinter().writeValue(tempFile.toFile(), catalog);
            forceFile(tempFile);
            try {
                Files.move(tempFile, catalogFile,
                        StandardCopyOption.ATOMIC_MOVE,
                        StandardCopyOption.REPLACE_EXISTING);
            } catch (IOException atomicFailure) {
                Files.move(tempFile, catalogFile, StandardCopyOption.REPLACE_EXISTING);
            }
            forceDirectory(parent);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to save segment catalog " + catalogFile, e);
        }
    }

    private boolean isMaintenanceArtifact(Path path) {
        String name = path.getFileName().toString();
        return name.equals(catalogFile.getFileName() + ".tmp")
                || name.endsWith(".cleaned.log")
                || name.endsWith(".cleaned.index")
                || name.endsWith(".obsolete.log")
                || name.endsWith(".obsolete.index");
    }

    private void forceFile(Path path) throws IOException {
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            channel.force(true);
        }
    }

    private void forceDirectory(Path directory) {
        if (directory == null) {
            return;
        }
        try (FileChannel channel = FileChannel.open(directory, StandardOpenOption.READ)) {
            channel.force(true);
        } catch (IOException ignored) {
            // Some platforms do not support forcing a directory channel.
        }
    }
}
