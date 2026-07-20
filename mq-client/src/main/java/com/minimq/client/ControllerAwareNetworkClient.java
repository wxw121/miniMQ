package com.minimq.client;

import com.minimq.client.exception.MiniMqConnectionException;
import com.minimq.common.error.ErrorCode;
import com.minimq.protocol.ApiKeys;
import com.minimq.protocol.codec.MqDecoder;
import com.minimq.protocol.message.ProtocolMessages.DiscoverControllerRequest;
import com.minimq.protocol.message.ProtocolMessages.DiscoverControllerResponse;

/** Discovers the active Controller through any bootstrap Broker and retries one stale route. */
public final class ControllerAwareNetworkClient implements AutoCloseable {
    private final BootstrapServers bootstrapServers;
    private final String clientId;
    private NetworkClient controllerClient;
    private long controllerEpoch = -1L;

    public ControllerAwareNetworkClient(String bootstrapServers, String clientId) {
        this.bootstrapServers = BootstrapServers.parse(bootstrapServers);
        this.clientId = clientId;
    }

    public synchronized void connect() throws InterruptedException {
        rediscover();
    }

    public synchronized MqDecoder.DecodedResponse send(short apiKey, Object body)
            throws InterruptedException {
        if (controllerClient == null) {
            rediscover();
        }
        MqDecoder.DecodedResponse response = controllerClient.send(apiKey, body);
        if (isStaleController(response.header().errorCode())) {
            rediscover();
            response = controllerClient.send(apiKey, body);
        }
        return response;
    }

    public void ensureSuccess(MqDecoder.DecodedResponse response) {
        ensureSuccess(response, "Request");
    }

    public void ensureSuccess(MqDecoder.DecodedResponse response, String operation) {
        controllerClient.ensureSuccess(response, operation);
    }

    public synchronized long controllerEpoch() {
        return controllerEpoch;
    }

    private void rediscover() throws InterruptedException {
        closeCurrent();
        RuntimeException lastFailure = null;
        for (BootstrapServers.Endpoint endpoint : bootstrapServers.endpoints()) {
            try (NetworkClient bootstrap = new NetworkClient(clientId + "-bootstrap")) {
                bootstrap.connect(endpoint.host(), endpoint.port());
                MqDecoder.DecodedResponse decoded = bootstrap.send(
                        ApiKeys.DISCOVER_CONTROLLER, new DiscoverControllerRequest());
                bootstrap.ensureSuccess(decoded, "DISCOVER_CONTROLLER");
                DiscoverControllerResponse discovered = (DiscoverControllerResponse) decoded.body();
                if (discovered.controller().controllerEpoch() < controllerEpoch) {
                    continue;
                }
                NetworkClient next = new NetworkClient(clientId + "-controller");
                next.connect(discovered.controller().host(), discovered.controller().port());
                controllerClient = next;
                controllerEpoch = discovered.controller().controllerEpoch();
                return;
            } catch (RuntimeException failure) {
                lastFailure = failure;
            }
        }
        throw new MiniMqConnectionException(
                "Failed to discover Controller from all bootstrap servers", lastFailure);
    }

    private static boolean isStaleController(ErrorCode error) {
        return error == ErrorCode.NOT_CONTROLLER || error == ErrorCode.CONTROLLER_FENCED;
    }

    private void closeCurrent() throws InterruptedException {
        if (controllerClient != null) {
            controllerClient.close();
            controllerClient = null;
        }
    }

    @Override
    public synchronized void close() throws InterruptedException {
        closeCurrent();
    }
}
