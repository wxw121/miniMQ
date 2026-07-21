package com.minimq.broker.coordination;

import java.util.Objects;

/** Small state machine around the blocking etcd campaign operation. */
public final class ControllerElectionService implements AutoCloseable {
    private final ControllerCoordinationStore store;
    private final int brokerId;
    private final String host;
    private final int port;
    private final int ttlSeconds;
    private volatile ControllerState state = ControllerState.FOLLOWER;
    private volatile ControllerCoordinationStore.Leadership leadership;

    public ControllerElectionService(ControllerCoordinationStore store, int brokerId,
                                     String host, int port, int ttlSeconds) {
        this.store = Objects.requireNonNull(store, "store");
        if (brokerId < 0 || host == null || host.isBlank() || port <= 0 || ttlSeconds <= 0) {
            throw new IllegalArgumentException("invalid election configuration");
        }
        this.brokerId = brokerId;
        this.host = host;
        this.port = port;
        this.ttlSeconds = ttlSeconds;
    }

    public synchronized ControllerCoordinationStore.Leadership campaign() {
        if (leadership != null) {
            throw new IllegalStateException("controller already owns a leadership session");
        }
        state = ControllerState.CAMPAIGNING;
        try {
            leadership = store.campaign(brokerId, host, port, ttlSeconds);
            state = ControllerState.RECOVERING;
            return leadership;
        } catch (RuntimeException e) {
            state = ControllerState.FENCED;
            throw e;
        }
    }

    public synchronized void activate() {
        if (state != ControllerState.RECOVERING || leadership == null || !leadership.valid()) {
            state = ControllerState.FENCED;
            throw new ControllerFencedException("leadership is not valid");
        }
        state = ControllerState.ACTIVE;
    }

    public ControllerState state() {
        if ((state == ControllerState.ACTIVE || state == ControllerState.RECOVERING)
                && leadership != null && !leadership.valid()) {
            state = ControllerState.FENCED;
        }
        return state;
    }

    public ControllerCoordinationStore.Leadership leadership() {
        return leadership;
    }

    @Override
    public synchronized void close() {
        if (leadership != null) {
            leadership.close();
            leadership = null;
        }
        state = ControllerState.FOLLOWER;
    }
}
