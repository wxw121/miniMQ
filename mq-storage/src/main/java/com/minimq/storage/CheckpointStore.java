package com.minimq.storage;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.time.Clock;
import java.util.Optional;

/**
 * Safe-write JSON checkpoint store for WAL replay positions.
 */
public class CheckpointStore {
    private final Path checkpointFile;
    private final Clock clock;
    private final ObjectMapper mapper = new ObjectMapper();

    public CheckpointStore(Path dataDir) {
        this(dataDir, Clock.systemUTC());
    }

    CheckpointStore(Path dataDir, Clock clock) {
        this.checkpointFile = dataDir.resolve("checkpoint").resolve("recovery-checkpoint.json");
        this.clock = clock;
    }

    public synchronized Optional<RecoveryCheckpoint> load() {
        if (!Files.exists(checkpointFile)) {
            return Optional.empty();
        }
        try {
            if (Files.size(checkpointFile) == 0) {
                return Optional.empty();
            }
            RecoveryCheckpoint checkpoint = mapper.readValue(checkpointFile.toFile(), RecoveryCheckpoint.class);
            validate(checkpoint);
            return Optional.of(checkpoint);
        } catch (IOException | IllegalArgumentException e) {
            return Optional.empty();
        }
    }

    public synchronized void save(RecoveryCheckpoint checkpoint) {
        try {
            validate(checkpoint);
            Path parent = checkpointFile.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            Path tempFile = checkpointFile.resolveSibling(checkpointFile.getFileName() + ".tmp");
            mapper.writerWithDefaultPrettyPrinter().writeValue(tempFile.toFile(), checkpoint);
            forceFile(tempFile);
            try {
                Files.move(tempFile, checkpointFile,
                        StandardCopyOption.ATOMIC_MOVE,
                        StandardCopyOption.REPLACE_EXISTING);
            } catch (IOException atomicFailure) {
                Files.move(tempFile, checkpointFile, StandardCopyOption.REPLACE_EXISTING);
            }
            forceDirectory(parent);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to save recovery checkpoint", e);
        }
    }

    public RecoveryCheckpoint checkpoint(
            long walPosition,
            java.util.Map<String, Long> highWatermarks,
            java.util.Map<String, java.util.Map<Long, Long>> scheduledDeliveries,
            java.util.Map<String, Long> visibleDeliveryWatermarks) {
        return new RecoveryCheckpoint(
                walPosition,
                highWatermarks,
                scheduledDeliveries,
                visibleDeliveryWatermarks,
                clock.millis());
    }

    public RecoveryCheckpoint checkpoint(
            long walPosition,
            java.util.Map<String, Long> highWatermarks,
            java.util.Map<String, java.util.Map<Long, Long>> scheduledDeliveries) {
        return checkpoint(walPosition, highWatermarks, scheduledDeliveries, java.util.Map.of());
    }

    public RecoveryCheckpoint checkpoint(
            long walPosition,
            java.util.Map<String, Long> highWatermarks) {
        return checkpoint(walPosition, highWatermarks, java.util.Map.of());
    }

    private void validate(RecoveryCheckpoint checkpoint) {
        if (checkpoint == null) {
            throw new IllegalArgumentException("checkpoint is required");
        }
        if (checkpoint.walPosition < 0) {
            throw new IllegalArgumentException("checkpoint walPosition must be non-negative");
        }
        if (checkpoint.partitionHighWatermarks == null) {
            throw new IllegalArgumentException("checkpoint partitionHighWatermarks is required");
        }
        if (checkpoint.scheduledDeliveries == null) {
            checkpoint.scheduledDeliveries = new java.util.HashMap<>();
        }
        if (checkpoint.visibleDeliveryWatermarks == null) {
            checkpoint.visibleDeliveryWatermarks = new java.util.HashMap<>();
        }
        checkpoint.partitionHighWatermarks.forEach((key, value) -> {
            if (key == null || key.isBlank()) {
                throw new IllegalArgumentException("checkpoint partition key is blank");
            }
            if (value == null || value < 0) {
                throw new IllegalArgumentException("checkpoint high watermark is invalid for " + key);
            }
        });
        checkpoint.scheduledDeliveries.forEach((partition, deliveries) -> {
            if (partition == null || partition.isBlank()) {
                throw new IllegalArgumentException("checkpoint scheduled partition key is blank");
            }
            if (deliveries == null) {
                throw new IllegalArgumentException("checkpoint scheduled deliveries are null for " + partition);
            }
            deliveries.forEach((offset, deliverAt) -> {
                if (offset == null || offset < 0 || deliverAt == null || deliverAt <= 0) {
                    throw new IllegalArgumentException(
                            "checkpoint scheduled delivery is invalid for " + partition);
                }
            });
        });
        checkpoint.visibleDeliveryWatermarks.forEach((partition, visibleWatermark) -> {
            if (partition == null || partition.isBlank()) {
                throw new IllegalArgumentException("checkpoint visible delivery partition key is blank");
            }
            if (visibleWatermark == null || visibleWatermark < 0) {
                throw new IllegalArgumentException(
                        "checkpoint visible delivery watermark is invalid for " + partition);
            }
        });
    }

    private void forceFile(Path file) throws IOException {
        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
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
