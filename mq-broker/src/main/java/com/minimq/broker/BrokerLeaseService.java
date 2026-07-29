package com.minimq.broker;

import com.minimq.broker.audit.FailoverAuditEventType;
import com.minimq.broker.audit.FailoverAuditLog;
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
    private final ControllerRole controllerRole;
    private final FailoverAuditLog auditLog;
    private final Supplier<ControllerClient> clientFactory;
    private final LongSupplier nanoTime;
    private final ScheduledExecutorService scheduler;
    private final AtomicBoolean closed = new AtomicBoolean();
    private ControllerClient client;
    private long brokerEpoch = -1L;
    private long lastConfirmedRequestStartNanos = Long.MIN_VALUE;
    private boolean selfFenceRecorded;
    private boolean activationHeld;

    public BrokerLeaseService(
            BrokerConfig config,
            ClusterController localController,
            BrokerAvailabilityGuard guard) {
        this(
                config,
                localController,
                guard,
                ControllerRole.legacy(config),
                ControllerClient::new,
                System::nanoTime,
                Executors.newSingleThreadScheduledExecutor(runnable -> {
                    Thread thread = new Thread(runnable, "broker-lease-" + config.brokerId());
                    thread.setDaemon(true);
                    return thread;
                }));
    }

    public BrokerLeaseService(
            BrokerConfig config,
            ClusterController localController,
            BrokerAvailabilityGuard guard,
            ControllerRole controllerRole) {
        this(config, localController, guard, controllerRole, ControllerClient::new,
                System::nanoTime,
                Executors.newSingleThreadScheduledExecutor(runnable -> {
                    Thread thread = new Thread(runnable, "broker-lease-" + config.brokerId());
                    thread.setDaemon(true);
                    return thread;
                }));
    }

    public BrokerLeaseService(
            BrokerConfig config,
            ClusterController localController,
            BrokerAvailabilityGuard guard,
            FailoverAuditLog auditLog) {
        this(config, localController, guard, ControllerRole.legacy(config), auditLog);
    }

    public BrokerLeaseService(
            BrokerConfig config,
            ClusterController localController,
            BrokerAvailabilityGuard guard,
            ControllerRole controllerRole,
            FailoverAuditLog auditLog) {
        this(config, localController, guard, controllerRole, ControllerClient::new,
                System::nanoTime,
                Executors.newSingleThreadScheduledExecutor(runnable -> {
                    Thread thread = new Thread(runnable, "broker-lease-" + config.brokerId());
                    thread.setDaemon(true);
                    return thread;
                }),
                auditLog);
    }

    BrokerLeaseService(
            BrokerConfig config,
            ClusterController localController,
            BrokerAvailabilityGuard guard,
            Supplier<ControllerClient> clientFactory,
            LongSupplier nanoTime,
            ScheduledExecutorService scheduler) {
        this(config, localController, guard, ControllerRole.legacy(config),
                clientFactory, nanoTime, scheduler, null);
    }

    BrokerLeaseService(
            BrokerConfig config,
            ClusterController localController,
            BrokerAvailabilityGuard guard,
            ControllerRole controllerRole,
            Supplier<ControllerClient> clientFactory,
            LongSupplier nanoTime,
            ScheduledExecutorService scheduler) {
        this(config, localController, guard, controllerRole, clientFactory, nanoTime, scheduler, null);
    }

    BrokerLeaseService(
            BrokerConfig config,
            ClusterController localController,
            BrokerAvailabilityGuard guard,
            ControllerRole controllerRole,
            Supplier<ControllerClient> clientFactory,
            LongSupplier nanoTime,
            ScheduledExecutorService scheduler,
            FailoverAuditLog auditLog) {
        this.config = config;
        this.localController = localController;
        this.guard = guard;
        this.controllerRole = controllerRole;
        this.auditLog = auditLog;
        this.clientFactory = clientFactory;
        this.nanoTime = nanoTime;
        this.scheduler = scheduler;
    }

    public synchronized void initialize() throws InterruptedException {
        if (closed.get()) {
            throw new IllegalStateException("Broker lease service is closed");
        }
        try {
            establishLease();
        } catch (InterruptedException | RuntimeException | Error failure) {
            resetClientAfterFailure(failure);
            throw failure;
        }
    }

    public synchronized void holdActivation() {
        if (brokerEpoch >= 0L) {
            throw new IllegalStateException("Broker lease is already initialized");
        }
        activationHeld = true;
    }

    public synchronized void activateAfterStartupBarrier() {
        if (!activationHeld) {
            throw new IllegalStateException("Broker activation is not held");
        }
        if (guard.state() == BrokerAvailabilityGuard.State.FENCED) {
            throw new IllegalStateException("Broker was fenced before startup barrier completed");
        }
        if (leaseTimedOut()) {
            fenceSelf("lease expired before startup barrier completed");
            throw new IllegalStateException("Broker lease expired before startup barrier completed");
        }
        activationHeld = false;
        guard.activate();
        if (guard.state() != BrokerAvailabilityGuard.State.ACTIVE) {
            throw new IllegalStateException("Broker activation blocked by storage cleanup fence");
        }
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
        if (guard.hasStickyStorageFence()) {
            return;
        }
        if (activationHeld) {
            renewHeldLease();
            return;
        }
        if (guard.state() != BrokerAvailabilityGuard.State.ACTIVE) {
            recover();
            return;
        }

        long requestStart = nanoTime.getAsLong();
        if (leaseTimedOutAt(requestStart)) {
            fenceSelf("self-fence timeout before heartbeat");
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
                fenceSelf("controller fenced broker lease");
            } else {
                fenceIfTimedOut();
            }
            resetClient(remainingSelfFenceTimeoutMs(nanoTime.getAsLong()));
            fenceIfTimedOut();
        } catch (Exception failure) {
            fenceIfTimedOut();
            resetClient(remainingSelfFenceTimeoutMs(nanoTime.getAsLong()));
            fenceIfTimedOut();
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
        if (controllerRole.isActive()) {
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
            ControllerClient controllerClient = controllerClient(config.brokerSelfFenceTimeoutMs());
            RegisterBrokerResponse response = controllerClient.registerBroker(registrationRequest());
            brokerEpoch = response.brokerEpoch();
            actualLeaseTimeoutMs = response.leaseTimeoutMs();
            if (config.usesLegacyControllerMode()) {
                MetadataSync.refresh(
                        localController,
                        controllerClient,
                        deadlineNanos(requestStart, actualLeaseTimeoutMs),
                        nanoTime);
            }
        }

        if (config.brokerSelfFenceTimeoutMs() >= actualLeaseTimeoutMs) {
            throw new IllegalStateException(
                    "Broker self-fence timeout must be less than Controller lease timeout");
        }
        if (!controllerRole.isActive()) {
            confirmedRequestStart = nanoTime.getAsLong();
            controllerClient(config.brokerSelfFenceTimeoutMs()).heartbeatBroker(
                    new BrokerHeartbeatRequest(config.brokerId(), brokerEpoch),
                    config.brokerSelfFenceTimeoutMs());
        }
        lastConfirmedRequestStartNanos = confirmedRequestStart;
        if (closed.get() || leaseTimedOut()) {
            fenceSelf("lease expired during registration");
            throw new IllegalStateException("Broker lease expired during registration and metadata sync");
        }
        selfFenceRecorded = false;
        if (!activationHeld) {
            guard.activate();
        }
    }

    private void renewHeldLease() {
        long requestStart = nanoTime.getAsLong();
        if (leaseTimedOutAt(requestStart)) {
            fenceSelf("self-fence timeout before startup heartbeat");
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
                fenceSelf("controller fenced broker lease during startup");
            } else {
                fenceIfTimedOut();
            }
            resetClient(remainingSelfFenceTimeoutMs(nanoTime.getAsLong()));
            fenceIfTimedOut();
        } catch (Exception failure) {
            fenceIfTimedOut();
            resetClient(remainingSelfFenceTimeoutMs(nanoTime.getAsLong()));
            fenceIfTimedOut();
        }
    }

    private RegisterBrokerRequest registrationRequest() {
        return new RegisterBrokerRequest(config.brokerId(), config.host(), config.port());
    }

    private void heartbeat(BrokerHeartbeatRequest request, long responseTimeoutMs) throws InterruptedException {
        if (controllerRole.isActive()) {
            ensureControllerSuccess(localController.heartbeatBroker(request));
        } else {
            ControllerClient controllerClient = controllerClient(responseTimeoutMs);
            long remainingTimeoutMs = Math.min(
                    responseTimeoutMs,
                    remainingSelfFenceTimeoutMs(nanoTime.getAsLong()));
            controllerClient.heartbeatBroker(request, remainingTimeoutMs);
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
            fenceSelf("lease recovery failed");
        }
    }

    private void fenceIfTimedOut() {
        if (leaseTimedOutAt(nanoTime.getAsLong())) {
            fenceSelf("self-fence timeout after heartbeat");
        }
    }

    private void fenceSelf(String detail) {
        boolean alreadyFenced = guard.state() == BrokerAvailabilityGuard.State.FENCED;
        guard.fence();
        if (alreadyFenced || selfFenceRecorded) {
            return;
        }
        selfFenceRecorded = true;
        if (auditLog == null) {
            return;
        }
        try {
            auditLog.append(
                    FailoverAuditEventType.BROKER_SELF_FENCED,
                    config.brokerId(),
                    null,
                    null,
                    "active",
                    "fenced",
                    detail);
        } catch (RuntimeException ignored) {
            // Self-fencing remains authoritative even when the audit append fails.
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

    private static long deadlineNanos(long startNanos, long timeoutMs) {
        if (timeoutMs <= 0L) {
            return startNanos;
        }
        long timeoutNanos = TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        if (startNanos > Long.MAX_VALUE - timeoutNanos) {
            return Long.MAX_VALUE;
        }
        return startNanos + timeoutNanos;
    }

    private ControllerClient controllerClient(long connectTimeoutMs) throws InterruptedException {
        if (client == null) {
            var identity = controllerRole.currentLeader();
            if (identity == null) {
                throw new IllegalStateException("Controller is not available");
            }
            client = clientFactory.get();
            client.connect(identity.host(), identity.port(), connectTimeoutMs);
        }
        return client;
    }

    private static void ensureControllerSuccess(ErrorCode errorCode) {
        if (errorCode != ErrorCode.NONE) {
            throw new ControllerClient.ControllerRequestException(errorCode);
        }
    }

    private void resetClient() {
        resetClient(TimeUnit.SECONDS.toMillis(5));
    }

    private void resetClientAfterFailure(Throwable primaryFailure) {
        if (client == null) {
            return;
        }
        try {
            client.close(TimeUnit.SECONDS.toMillis(5));
        } catch (InterruptedException closeFailure) {
            primaryFailure.addSuppressed(closeFailure);
            Thread.currentThread().interrupt();
        } catch (RuntimeException | Error closeFailure) {
            primaryFailure.addSuppressed(closeFailure);
        } finally {
            client = null;
        }
    }

    private void resetClient(long closeTimeoutMs) {
        if (client == null) {
            return;
        }
        try {
            client.close(closeTimeoutMs);
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
