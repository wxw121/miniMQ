package com.minimq.broker.replication;

import com.minimq.broker.BrokerConfig;
import com.minimq.broker.BrokerAvailabilityGuard;
import com.minimq.broker.ControllerClient;
import com.minimq.broker.ControllerRole;
import com.minimq.broker.controller.ClusterController;
import com.minimq.broker.metadata.ClusterMetadata;
import com.minimq.broker.resilience.CircuitBreaker;
import com.minimq.common.error.ErrorCode;
import com.minimq.protocol.message.ProtocolMessages.UpdateReplicaStateV2Request;

import java.util.HashMap;
import java.util.Map;

public final class ReplicaStateReporter implements AutoCloseable {
    private final BrokerConfig config;
    private final ClusterController controller;
    private final BrokerAvailabilityGuard availabilityGuard;
    private final ControllerRole controllerRole;
    private final CircuitBreaker controllerCircuitBreaker;
    private final Map<ReplicaKey, ReplicaStateReport> confirmedReports = new HashMap<>();
    private ControllerClient client;

    public ReplicaStateReporter(BrokerConfig config, ClusterController controller) {
        this(config, controller, activeGuard(), ControllerRole.legacy(config));
    }

    public ReplicaStateReporter(
            BrokerConfig config,
            ClusterController controller,
            BrokerAvailabilityGuard availabilityGuard) {
        this(config, controller, availabilityGuard, ControllerRole.legacy(config));
    }

    public ReplicaStateReporter(
            BrokerConfig config,
            ClusterController controller,
            BrokerAvailabilityGuard availabilityGuard,
            ControllerRole controllerRole) {
        this(config, controller, availabilityGuard, controllerRole,
                new CircuitBreaker(3, 1_000, System::currentTimeMillis));
    }

    public ReplicaStateReporter(
            BrokerConfig config,
            ClusterController controller,
            BrokerAvailabilityGuard availabilityGuard,
            ControllerRole controllerRole,
            CircuitBreaker controllerCircuitBreaker) {
        this.config = config;
        this.controller = controller;
        this.availabilityGuard = availabilityGuard;
        this.controllerRole = controllerRole;
        this.controllerCircuitBreaker = controllerCircuitBreaker;
    }

    public synchronized ErrorCode report(
            String topicName, int partitionId, int brokerId, long logEndOffset) {
        return report(topicName, partitionId, brokerId, logEndOffset, logEndOffset, -1L);
    }

    public synchronized ErrorCode report(
            String topicName,
            int partitionId,
            int brokerId,
            long physicalLogEndOffset,
            long visibleLogEndOffset,
            long nextVisibleAtEpochMs) {
        if (!availabilityGuard.allowsDataPlane()) {
            return ErrorCode.BROKER_FENCED;
        }
        long brokerEpoch = brokerEpoch(brokerId);
        ClusterMetadata snapshot = controller.snapshot();
        long controllerEpoch = snapshot.controllerEpoch;
        long leaderEpoch = leaderEpoch(snapshot, topicName, partitionId);
        ReplicaStateReport report = new ReplicaStateReport(
                brokerEpoch,
                controllerEpoch,
                leaderEpoch,
                physicalLogEndOffset,
                visibleLogEndOffset,
                nextVisibleAtEpochMs);
        ReplicaKey key = new ReplicaKey(topicName, partitionId, brokerId);
        if (report.equals(confirmedReports.get(key))) {
            return ErrorCode.NONE;
        }
        if (controllerRole.isActive()) {
            ErrorCode result = controller.updateReplicaStateV2(
                    topicName,
                    partitionId,
                    brokerId,
                    brokerEpoch,
                    controllerEpoch,
                    leaderEpoch,
                    physicalLogEndOffset,
                    visibleLogEndOffset,
                    nextVisibleAtEpochMs);
            if (result == ErrorCode.NONE) {
                confirmedReports.put(key, report);
            }
            return result;
        }
        if (!controllerCircuitBreaker.allowRequest()) {
            return ErrorCode.CONTROLLER_NOT_READY;
        }
        try {
            controllerClient().updateReplicaStateV2(new UpdateReplicaStateV2Request(
                    config.clusterToken(),
                    topicName,
                    partitionId,
                    brokerId,
                    brokerEpoch,
                    controllerEpoch,
                    leaderEpoch,
                    physicalLogEndOffset,
                    visibleLogEndOffset,
                    nextVisibleAtEpochMs));
            confirmedReports.put(key, report);
            controllerCircuitBreaker.recordSuccess();
            return ErrorCode.NONE;
        } catch (InterruptedException failure) {
            resetClient();
            controllerCircuitBreaker.recordFailure();
            Thread.currentThread().interrupt();
            return ErrorCode.INVALID_REQUEST;
        } catch (ControllerClient.ControllerRequestException failure) {
            resetClient();
            controllerCircuitBreaker.recordFailure();
            return failure.errorCode();
        } catch (Exception failure) {
            resetClient();
            controllerCircuitBreaker.recordFailure();
            return ErrorCode.INVALID_REQUEST;
        }
    }

    private ControllerClient controllerClient() throws InterruptedException {
        if (client == null) {
            var identity = controllerRole.currentLeader();
            if (identity == null) {
                throw new IllegalStateException("Controller is not available");
            }
            client = new ControllerClient();
            client.connect(identity.host(), identity.port());
        }
        return client;
    }

    private void resetClient() {
        if (client == null) {
            return;
        }
        try {
            client.close();
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
        } finally {
            client = null;
        }
    }

    @Override
    public synchronized void close() throws InterruptedException {
        if (client != null) {
            client.close();
            client = null;
        }
    }

    private record ReplicaKey(String topicName, int partitionId, int brokerId) {
    }

    private record ReplicaStateReport(
            long brokerEpoch,
            long controllerEpoch,
            long leaderEpoch,
            long physicalLogEndOffset,
            long visibleLogEndOffset,
            long nextVisibleAtEpochMs) {
    }

    private long brokerEpoch(int brokerId) {
        var broker = controller.getBroker(brokerId);
        return broker == null || broker.brokerEpoch <= 0 ? 1L : broker.brokerEpoch;
    }

    private static long leaderEpoch(
            ClusterMetadata snapshot, String topicName, int partitionId) {
        ClusterMetadata.TopicEntry topic = snapshot.topics.get(topicName);
        if (topic == null) {
            return 0L;
        }
        return topic.partitions.stream()
                .filter(partition -> partition.partitionId == partitionId)
                .mapToLong(partition -> partition.leaderEpoch)
                .findFirst()
                .orElse(0L);
    }

    private static BrokerAvailabilityGuard activeGuard() {
        BrokerAvailabilityGuard guard = new BrokerAvailabilityGuard();
        guard.activate();
        return guard;
    }
}
