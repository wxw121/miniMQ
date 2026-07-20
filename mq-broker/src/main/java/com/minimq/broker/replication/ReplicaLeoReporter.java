package com.minimq.broker.replication;

import com.minimq.broker.BrokerConfig;
import com.minimq.broker.BrokerAvailabilityGuard;
import com.minimq.broker.ControllerClient;
import com.minimq.broker.ControllerRole;
import com.minimq.broker.controller.ClusterController;
import com.minimq.common.error.ErrorCode;
import com.minimq.protocol.message.ProtocolMessages.UpdateReplicaLeoRequest;

import java.util.HashMap;
import java.util.Map;

public final class ReplicaLeoReporter implements AutoCloseable {
    private final BrokerConfig config;
    private final ClusterController controller;
    private final BrokerAvailabilityGuard availabilityGuard;
    private final ControllerRole controllerRole;
    private final Map<ReplicaKey, Long> confirmedLeos = new HashMap<>();
    private ControllerClient client;

    public ReplicaLeoReporter(BrokerConfig config, ClusterController controller) {
        this(config, controller, activeGuard(), ControllerRole.legacy(config));
    }

    public ReplicaLeoReporter(
            BrokerConfig config,
            ClusterController controller,
            BrokerAvailabilityGuard availabilityGuard) {
        this(config, controller, availabilityGuard, ControllerRole.legacy(config));
    }

    public ReplicaLeoReporter(
            BrokerConfig config,
            ClusterController controller,
            BrokerAvailabilityGuard availabilityGuard,
            ControllerRole controllerRole) {
        this.config = config;
        this.controller = controller;
        this.availabilityGuard = availabilityGuard;
        this.controllerRole = controllerRole;
    }

    public synchronized ErrorCode report(
            String topicName, int partitionId, int brokerId, long logEndOffset) {
        if (!availabilityGuard.allowsDataPlane()) {
            return ErrorCode.BROKER_FENCED;
        }
        ReplicaKey key = new ReplicaKey(topicName, partitionId, brokerId);
        if (logEndOffset <= confirmedLeos.getOrDefault(key, -1L)) {
            return ErrorCode.NONE;
        }
        if (controllerRole.isActive()) {
            ErrorCode result = controller.updateReplicaLogEndOffset(
                    topicName, partitionId, brokerId, logEndOffset);
            if (result == ErrorCode.NONE) {
                confirmedLeos.put(key, logEndOffset);
            }
            return result;
        }
        try {
            var broker = controller.getBroker(brokerId);
            long brokerEpoch = broker == null || broker.brokerEpoch <= 0 ? 1L : broker.brokerEpoch;
            controllerClient().updateReplicaLeo(new UpdateReplicaLeoRequest(
                    topicName, partitionId, brokerId, brokerEpoch,
                    controller.snapshot().controllerEpoch, logEndOffset));
            confirmedLeos.put(key, logEndOffset);
            return ErrorCode.NONE;
        } catch (InterruptedException failure) {
            resetClient();
            Thread.currentThread().interrupt();
            return ErrorCode.INVALID_REQUEST;
        } catch (Exception failure) {
            resetClient();
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

    private static BrokerAvailabilityGuard activeGuard() {
        BrokerAvailabilityGuard guard = new BrokerAvailabilityGuard();
        guard.activate();
        return guard;
    }
}
