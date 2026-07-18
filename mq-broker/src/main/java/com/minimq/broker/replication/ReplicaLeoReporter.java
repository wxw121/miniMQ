package com.minimq.broker.replication;

import com.minimq.broker.BrokerConfig;
import com.minimq.broker.ControllerClient;
import com.minimq.broker.controller.ClusterController;
import com.minimq.common.error.ErrorCode;
import com.minimq.protocol.message.ProtocolMessages.UpdateReplicaLeoRequest;

public final class ReplicaLeoReporter implements AutoCloseable {
    private final BrokerConfig config;
    private final ClusterController controller;
    private ControllerClient client;

    public ReplicaLeoReporter(BrokerConfig config, ClusterController controller) {
        this.config = config;
        this.controller = controller;
    }

    public synchronized ErrorCode report(
            String topicName, int partitionId, int brokerId, long logEndOffset) {
        if (config.controller()) {
            return controller.updateReplicaLogEndOffset(
                    topicName, partitionId, brokerId, logEndOffset);
        }
        try {
            controllerClient().updateReplicaLeo(new UpdateReplicaLeoRequest(
                    topicName, partitionId, brokerId, logEndOffset));
            return ErrorCode.NONE;
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            return ErrorCode.INVALID_REQUEST;
        } catch (IllegalStateException failure) {
            return ErrorCode.INVALID_REQUEST;
        }
    }

    private ControllerClient controllerClient() throws InterruptedException {
        if (client == null) {
            client = new ControllerClient();
            client.connect(config.controllerHost(), config.controllerPort());
        }
        return client;
    }

    @Override
    public synchronized void close() throws InterruptedException {
        if (client != null) {
            client.close();
            client = null;
        }
    }
}
