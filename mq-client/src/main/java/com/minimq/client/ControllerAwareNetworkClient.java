package com.minimq.client;

import com.minimq.client.exception.MiniMqConnectionException;
import com.minimq.client.exception.MiniMqRequestTimeoutException;
import com.minimq.client.exception.MiniMqServerException;
import com.minimq.common.error.ErrorCode;
import com.minimq.protocol.ApiKeys;
import com.minimq.protocol.codec.MqDecoder;
import com.minimq.protocol.message.ProtocolMessages.DiscoverControllerRequest;
import com.minimq.protocol.message.ProtocolMessages.DiscoverControllerResponse;

import java.time.Duration;
import java.util.Objects;

/** Discovers the active Controller through any bootstrap Broker and retries one stale route. */
public final class ControllerAwareNetworkClient implements AutoCloseable {
    private final BootstrapServers bootstrapServers;
    private final String clientId;
    private final Duration requestTimeout;
    private final NetworkClientFactory clientFactory;
    private NetworkClient controllerClient;
    private long controllerEpoch = -1L;

    public ControllerAwareNetworkClient(String bootstrapServers, String clientId) {
        this(bootstrapServers, clientId, NetworkClient.DEFAULT_REQUEST_TIMEOUT);
    }

    public ControllerAwareNetworkClient(
            String bootstrapServers, String clientId, Duration requestTimeout) {
        this(bootstrapServers, clientId, requestTimeout, NetworkClient::new);
    }

    ControllerAwareNetworkClient(
            String bootstrapServers,
            String clientId,
            Duration requestTimeout,
            NetworkClientFactory clientFactory) {
        this.bootstrapServers = BootstrapServers.parse(bootstrapServers);
        this.clientId = clientId;
        this.requestTimeout = Objects.requireNonNull(requestTimeout, "requestTimeout");
        this.clientFactory = Objects.requireNonNull(clientFactory, "clientFactory");
        if (requestTimeout.isZero() || requestTimeout.isNegative()) {
            throw new IllegalArgumentException("requestTimeout must be positive");
        }
        NetworkClient.durationToNanos(requestTimeout);
    }

    public synchronized void connect() throws InterruptedException {
        rediscover(configuredDeadlineNanos());
    }

    public synchronized MqDecoder.DecodedResponse send(short apiKey, Object body)
            throws InterruptedException {
        return sendInternal(apiKey, body, configuredDeadlineNanos());
    }

    public synchronized MqDecoder.DecodedResponse send(
            short apiKey, Object body, Duration timeout) throws InterruptedException {
        Duration requestedTimeout = Objects.requireNonNull(timeout, "timeout");
        if (requestedTimeout.isNegative()) {
            throw new IllegalArgumentException("timeout must not be negative");
        }
        Duration effectiveTimeout = requestedTimeout.compareTo(requestTimeout) < 0
                ? requestedTimeout
                : requestTimeout;
        long deadlineNanos = NetworkClient.deadlineNanosFromNow(effectiveTimeout);
        return sendInternal(apiKey, body, deadlineNanos);
    }

    synchronized MqDecoder.DecodedResponse sendUntil(
            short apiKey, Object body, long deadlineNanos) throws InterruptedException {
        return sendInternal(apiKey, body, deadlineNanos);
    }

    private MqDecoder.DecodedResponse sendInternal(
            short apiKey, Object body, Long deadlineNanos) throws InterruptedException {
        for (int attempt = 0; attempt < 2; attempt++) {
            if (controllerClient == null) {
                rediscover(deadlineNanos);
            }
            Duration remainingTimeout = deadlineNanos == null
                    ? null
                    : remainingTimeout(deadlineNanos);
            if (remainingTimeout != null && remainingTimeout.isZero()) {
                throw new MiniMqRequestTimeoutException(
                        ApiKeys.nameOf(apiKey), remainingTimeout);
            }
            MqDecoder.DecodedResponse response;
            try {
                response = remainingTimeout == null
                        ? controllerClient.send(apiKey, body)
                        : controllerClient.send(apiKey, body, remainingTimeout);
            } catch (MiniMqConnectionException failure) {
                boolean cleanupInterrupted = invalidateCurrent(failure, deadlineNanos);
                if (attempt == 0
                        && !cleanupInterrupted
                        && hasRemainingTime(deadlineNanos)) {
                    rediscover(deadlineNanos);
                    continue;
                }
                throw failure;
            } catch (RuntimeException | Error failure) {
                invalidateCurrent(failure, deadlineNanos);
                throw failure;
            }
            if (isStaleController(response.header().errorCode()) && attempt == 0) {
                rediscover(deadlineNanos);
                continue;
            }
            return response;
        }
        throw new IllegalStateException("unreachable");
    }

    private static boolean hasRemainingTime(Long deadlineNanos) {
        return deadlineNanos == null || deadlineNanos - System.nanoTime() > 0L;
    }

    private static Duration remainingTimeout(long deadlineNanos) {
        long remainingNanos = deadlineNanos - System.nanoTime();
        return remainingNanos <= 0L
                ? Duration.ZERO
                : Duration.ofNanos(remainingNanos);
    }

    public void ensureSuccess(MqDecoder.DecodedResponse response) {
        ensureSuccess(response, "Request");
    }

    public void ensureSuccess(MqDecoder.DecodedResponse response, String operation) {
        if (response.header().errorCode() != ErrorCode.NONE) {
            throw new MiniMqServerException(response.header().errorCode(), operation);
        }
    }

    public synchronized long controllerEpoch() {
        return controllerEpoch;
    }

    private void rediscover() throws InterruptedException {
        rediscover(configuredDeadlineNanos());
    }

    private long configuredDeadlineNanos() {
        return NetworkClient.deadlineNanosFromNow(requestTimeout);
    }

    private void rediscover(Long deadlineNanos) throws InterruptedException {
        closeCurrent(deadlineNanos);
        RuntimeException lastFailure = null;
        for (BootstrapServers.Endpoint endpoint : bootstrapServers.endpoints()) {
            try {
                ensureTimeRemaining(deadlineNanos, "DISCOVER_CONTROLLER");
                DiscoverControllerResponse discovered = discoverController(
                        endpoint, deadlineNanos);
                if (discovered.controller().controllerEpoch() < controllerEpoch) {
                    continue;
                }
                NetworkClient next = clientFactory.create(
                        clientId + "-controller",
                        NetworkClient.RetryPolicy.fixedBackoff(1, 0),
                        requestTimeout);
                try {
                    connect(
                            next,
                            discovered.controller().host(),
                            discovered.controller().port(),
                            deadlineNanos,
                            "CONNECT_CONTROLLER");
                } catch (InterruptedException | RuntimeException | Error failure) {
                    suppressCloseFailure(next, failure, deadlineNanos);
                    throw failure;
                }
                controllerClient = next;
                controllerEpoch = discovered.controller().controllerEpoch();
                return;
            } catch (InterruptedException failure) {
                restoreCleanupInterruption(failure);
                throw failure;
            } catch (MiniMqRequestTimeoutException failure) {
                restoreCleanupInterruption(failure);
                throw failure;
            } catch (RuntimeException failure) {
                lastFailure = failure;
                if (restoreCleanupInterruption(failure)) {
                    break;
                }
            } catch (Error failure) {
                restoreCleanupInterruption(failure);
                throw failure;
            }
        }
        throw new MiniMqConnectionException(
                "Failed to discover Controller from all bootstrap servers", lastFailure);
    }

    private DiscoverControllerResponse discoverController(
            BootstrapServers.Endpoint endpoint, Long deadlineNanos)
            throws InterruptedException {
        NetworkClient bootstrap = clientFactory.create(
                clientId + "-bootstrap",
                NetworkClient.RetryPolicy.fixedBackoff(1, 0),
                requestTimeout);
        DiscoverControllerResponse discovered;
        try {
            connect(
                    bootstrap,
                    endpoint.host(),
                    endpoint.port(),
                    deadlineNanos,
                    "CONNECT_BOOTSTRAP");
            MqDecoder.DecodedResponse decoded = send(
                    bootstrap,
                    ApiKeys.DISCOVER_CONTROLLER,
                    new DiscoverControllerRequest(),
                    deadlineNanos);
            bootstrap.ensureSuccess(decoded, "DISCOVER_CONTROLLER");
            discovered = (DiscoverControllerResponse) decoded.body();
        } catch (InterruptedException | RuntimeException | Error failure) {
            suppressCloseFailure(bootstrap, failure, deadlineNanos);
            throw failure;
        }
        abortCandidate(bootstrap, deadlineNanos);
        return discovered;
    }

    private static void suppressCloseFailure(
            NetworkClient client, Throwable failure, Long deadlineNanos) {
        if (deadlineNanos == null) {
            NetworkClientCleanup.suppressCloseFailure(client, failure);
        } else {
            NetworkClientCleanup.suppressCloseFailure(
                    client, failure, remainingTimeout(deadlineNanos));
        }
    }

    private static void abortCandidate(NetworkClient client, Long deadlineNanos)
            throws InterruptedException {
        if (deadlineNanos == null) {
            client.abortCandidate();
        } else {
            client.abortCandidate(remainingTimeout(deadlineNanos));
        }
    }

    private static void connect(
            NetworkClient client,
            String host,
            int port,
            Long deadlineNanos,
            String operation) throws InterruptedException {
        if (deadlineNanos == null) {
            client.connect(host, port);
            return;
        }
        client.connect(host, port, requireRemainingTimeout(deadlineNanos, operation));
    }

    private static MqDecoder.DecodedResponse send(
            NetworkClient client,
            short apiKey,
            Object body,
            Long deadlineNanos) throws InterruptedException {
        if (deadlineNanos == null) {
            return client.send(apiKey, body);
        }
        return client.send(
                apiKey,
                body,
                requireRemainingTimeout(deadlineNanos, ApiKeys.nameOf(apiKey)));
    }

    private static void ensureTimeRemaining(Long deadlineNanos, String operation) {
        if (deadlineNanos != null) {
            requireRemainingTimeout(deadlineNanos, operation);
        }
    }

    private static Duration requireRemainingTimeout(long deadlineNanos, String operation) {
        Duration remaining = remainingTimeout(deadlineNanos);
        if (remaining.isZero()) {
            throw new MiniMqRequestTimeoutException(operation, remaining);
        }
        return remaining;
    }

    private static boolean restoreCleanupInterruption(Throwable failure) {
        boolean interrupted = NetworkClientCleanup.hasSuppressedInterruption(failure);
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
        return interrupted;
    }

    private boolean invalidateCurrent(Throwable failure) {
        return invalidateCurrent(failure, null);
    }

    private boolean invalidateCurrent(Throwable failure, Long deadlineNanos) {
        NetworkClient stale = controllerClient;
        controllerClient = null;
        if (stale == null) {
            return false;
        }
        boolean interrupted;
        if (deadlineNanos == null) {
            interrupted = NetworkClientCleanup.suppressCloseFailure(stale, failure);
        } else {
            interrupted = NetworkClientCleanup.suppressCloseFailure(
                    stale, failure, remainingTimeout(deadlineNanos));
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
        return interrupted;
    }

    private static boolean isStaleController(ErrorCode error) {
        return error == ErrorCode.NOT_CONTROLLER || error == ErrorCode.CONTROLLER_FENCED;
    }

    private void closeCurrent() throws InterruptedException {
        closeCurrent(null);
    }

    private void closeCurrent(Long deadlineNanos) throws InterruptedException {
        if (controllerClient != null) {
            NetworkClient stale = controllerClient;
            controllerClient = null;
            if (deadlineNanos == null) {
                stale.close();
            } else {
                stale.abortCandidate(remainingTimeout(deadlineNanos));
            }
        }
    }

    @Override
    public synchronized void close() throws InterruptedException {
        closeCurrent();
    }

    @FunctionalInterface
    interface NetworkClientFactory {
        NetworkClient create(
                String clientId,
                NetworkClient.RetryPolicy retryPolicy,
                Duration requestTimeout);
    }
}
