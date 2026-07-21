package com.minimq.broker.replication;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Base64;
import java.util.Optional;

/** Durable local proof of the highest Leader epoch accepted by this broker. */
public final class LeaderEpochStateStore {
    private final Path root;
    private final ObjectMapper mapper = new ObjectMapper();

    public LeaderEpochStateStore(Path root) {
        this.root = root;
    }

    public synchronized Optional<LeaderEpochState> load(String topic, int partitionId) {
        Path path = path(topic, partitionId);
        if (!Files.exists(path)) {
            return Optional.empty();
        }
        try {
            return Optional.of(mapper.readValue(path.toFile(), LeaderEpochState.class));
        } catch (IOException e) {
            throw new IllegalStateException("Failed to load Leader epoch state " + path, e);
        }
    }

    public synchronized void save(String topic, int partitionId, LeaderEpochState state) {
        Optional<LeaderEpochState> previous = load(topic, partitionId);
        if (previous.isPresent()) {
            LeaderEpochState existing = previous.get();
            boolean validReadyTransition = state.leaderEpoch() == existing.leaderEpoch()
                    && state.recoveryOffset() == existing.recoveryOffset()
                    && !existing.ready() && state.ready();
            if (state.leaderEpoch() < existing.leaderEpoch()
                    || (state.leaderEpoch() == existing.leaderEpoch()
                    && !state.equals(existing) && !validReadyTransition)) {
                throw new IllegalStateException("Refusing stale or conflicting Leader epoch state");
            }
            if (state.equals(existing)) {
                return;
            }
        }
        Path target = path(topic, partitionId);
        Path temporary = target.resolveSibling(target.getFileName() + ".tmp");
        try {
            Files.createDirectories(root);
            mapper.writeValue(temporary.toFile(), state);
            try (FileChannel channel = FileChannel.open(temporary, StandardOpenOption.READ)) {
                channel.force(true);
            }
            try {
                Files.move(temporary, target, StandardCopyOption.ATOMIC_MOVE,
                        StandardCopyOption.REPLACE_EXISTING);
            } catch (IOException atomicFailure) {
                Files.move(temporary, target, StandardCopyOption.REPLACE_EXISTING);
            }
        } catch (IOException e) {
            throw new IllegalStateException("Failed to save Leader epoch state " + target, e);
        }
    }

    private Path path(String topic, int partitionId) {
        if (topic == null || topic.isBlank() || partitionId < 0) {
            throw new IllegalArgumentException("invalid topic partition");
        }
        String encoded = Base64.getUrlEncoder().withoutPadding()
                .encodeToString(topic.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        return root.resolve(encoded + "-" + partitionId + ".json");
    }
}
