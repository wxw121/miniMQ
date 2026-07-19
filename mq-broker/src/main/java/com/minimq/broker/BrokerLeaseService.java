package com.minimq.broker;

import com.minimq.broker.controller.ClusterController;
import com.minimq.broker.metadata.ClusterMetadata.BrokerEntry;
import com.minimq.common.error.ErrorCode;
import com.minimq.protocol.message.ProtocolMessages.BrokerHeartbeatRequest;
import com.minimq.protocol.message.ProtocolMessages.RegisterBrokerRequest;
import com.minimq.protocol.message.ProtocolMessages.RegisterBrokerResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

public final class BrokerLeaseService implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(BrokerLeaseService.class);

    private final BrokerConfig config;
    private final ClusterController localController;
    private final BrokerAvailabilityGuard guard;
    private final Supplier<ControllerClient> clientFactory;
    private final LongSupplier nanoTime;
    private final ScheduledExecutorService scheduler;
    private final AtomicBoolean closed = new AtomicBoolean();
    private ControllerClient client;
    private long brokerEpoch = -1L;
    private long lastConfirmedRequestStartNanos = Long.MIN_VALUE;

    public BrokerLeaseService(
            BrokerConfig config,
            ClusterController localController,
            BrokerAvailabilityGuard guard) {
        this(
                config,
                localController,
                guard,
                ControllerClient::new,
                System::nanoTime,
                Executors.newSingleThreadScheduledExecutor(runnable -> {
                    Thread thread = new Thread(runnable, "broker-lease-" + config.brokerId());
                    thread.setDaemon(true);
                    return thread;
                }));
    }

    BrokerLeaseService(
            BrokerConfig config,
            ClusterController localController,
            BrokerAvailabilityGuard guard,
            Supplier<ControllerClient> clientFactory,
            LongSupplier nanoTime,
            ScheduledExecutorService scheduler) {
        this.config = config;
        this.localController = localController;
        this.guard = guard;
        this.clientFactory = clientFactory;
        this.nanoTime = nanoTime;
        this.scheduler = scheduler;
    }

    public synchronized void initialize() throws InterruptedException {
        if (closed.get()) {
            throw new IllegalStateException("Broker lease service is closed");
        }
        establishLease();
    }

    public void start() {
        scheduler.scheduleWithFixedDelay(
                this::runSafely,
                config.brokerHeartbeatIntervalMs(),
                config.brokerHeartbeatIntervalMs(),
                TimeUnit.MILLISECONDS);
    }

    synchronized void runOnce() {
        if (closed.get()) {
            return;
        }
        if (guard.state() != BrokerAvailabilityGuard.State.ACTIVE) {
            recover();
            return;
        }

        long requestStart = nanoTime.getAsLong();
        if (leaseTimedOutAt(requestStart)) {
            guard.fence();
            return;
        }
        try {
            heartbeat(
                    new BrokerHeartbeatRequest(config.brokerId(), brokerEpoch),
                    remainingSelfFenceTimeoutMs(requestStart));
            lastConfirmedRequestStartNanos = requestStart;
            fenceIfTimedOut();
        } catch (ControllerClient.ControllerRequestException failure) {
            if (failure.errorCode() == ErrorCode.BROKER_FENCED) {
                guard.fence();
            } else {
                fenceIfTimedOut();
            }
            resetClient();
        } catch (Exception failure) {
            fenceIfTimedOut();
            resetClient();
        }
    }

    long brokerEpoch() {
        return brokerEpoch;
    }

    private void runSafely() {
        try {
            runOnce();
        } catch (RuntimeException failure) {
            log.warn("Broker lease iteration failed for broker {}", config.brokerId(), failure);
        }
    }

    private void establishLease() throws InterruptedException {
        long requestStart = nanoTime.getAsLong();
        long confirmedRequestStart = requestStart;
        long actualLeaseTimeoutMs;
        if (config.controller()) {
            BrokerEntry existing = localController.getBroker(config.brokerId());
            if (existing != null && existing.online) {
                brokerEpoch = existing.brokerEpoch;
                ErrorCode result = localController.heartbeatBroker(
                        new BrokerHeartbeatRequest(config.brokerId(), brokerEpoch));
                ensureControllerSuccess(result);
                actualLeaseTimeoutMs = config.controllerBrokerLeaseTimeoutMs();
            } else {
                var registration = localController.registerBroker(registrationRequest());
                ensureControllerSuccess(registration.errorCode());
                RegisterBrokerResponse response = registration.response();
                brokerEpoch = response.brokerEpoch();
                actualLeaseTimeoutMs = response.leaseTimeoutMs();
            }
        } else {
            ControllerClient controllerClient = controllerClient();
            RegisterBrokerResponse response = controllerClient.registerBroker(registrationRequest());
            brokerEpoch = response.brokerEpoch();
            actualLeaseTimeoutMs = response.leaseTimeoutMs();
            MetadataSync.applyMetadata(localController, controllerClient.fetchMetadata());
        }

        if (config.brokerSelfFenceTimeoutMs() >= actualLeaseTimeoutMs) {
            throw new IllegalStateException(
                    "Broker self-fence timeout must be less than Controller lease timeout");
        }
        if (!config.controller()) {
            confirmedRequestStart = nanoTime.getAsLong();
            controllerClient().heartbeatBroker(
                    new BrokerHeartbeatRequest(config.brokerId(), brokerEpoch),
                    config.brokerSelfFenceTimeoutMs());
        }
        lastConfirmedRequestStartNanos = confirmedRequestStart;
        if (closed.get() || leaseTimedOut()) {
            guard.fence();
            throw new IllegalStateException("Broker lease expired during registration and metadata sync");
        }
        guard.activate();
    }

    private RegisterBrokerRequest registrationRequest() {
        return new RegisterBrokerRequest(config.brokerId(), config.host(), config.port());
    }

    private void heartbeat(BrokerHeartbeatRequest request, long responseTimeoutMs) throws InterruptedException {
        if (config.controller()) {
            ensureControllerSuccess(localController.heartbeatBroker(request));
        } else {
            controllerClient().heartbeatBroker(request, responseTimeoutMs);
        }
    }

    private void recover() {
        if (closed.get()) {
            return;
        }
        guard.startRecovering();
        try {
            establishLease();
        } catch (Exception failure) {
            resetClient();
            guard.fence();
        }
    }

    private void fenceIfTimedOut() {
        if (leaseTimedOutAt(nanoTime.getAsLong())) {
            guard.fence();
        }
    }

    private boolean leaseTimedOut() {
        return leaseTimedOutAt(nanoTime.getAsLong());
    }

    private boolean leaseTimedOutAt(long nowNanos) {
        if (lastConfirmedRequestStartNanos == Long.MIN_VALUE) {
            return true;
        }
        long elapsed = nowNanos - lastConfirmedRequestStartNanos;
        return elapsed >= TimeUnit.MILLISECONDS.toNanos(config.brokerSelfFenceTimeoutMs());
    }

    private long remainingSelfFenceTimeoutMs(long nowNanos) {
        long elapsed = nowNanos - lastConfirmedRequestStartNanos;
        long remainingNanos = TimeUnit.MILLISECONDS.toNanos(config.brokerSelfFenceTimeoutMs()) - elapsed;
        if (remainingNanos <= 0) {
            return 1L;
        }
        return TimeUnit.NANOSECONDS.toMillis(remainingNanos - 1L) + 1L;
    }

    private ControllerClient controllerClient() throws InterruptedException {
        if (client == null) {
            client = clientFactory.get();
            client.connect(config.controllerHost(), config.controllerPort());
        }
        return client;
    }

    private static void ensureControllerSuccess(ErrorCode errorCode) {
        if (errorCode != ErrorCode.NONE) {
            throw new ControllerClient.ControllerRequestException(errorCode);
        }
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
    public synchronized void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        guard.fence();
        scheduler.shutdownNow();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                log.warn("Broker lease scheduler did not terminate for broker {}", config.brokerId());
            }
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
        }
        resetClient();
    }
}
