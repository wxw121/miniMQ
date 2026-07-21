package com.minimq.broker;

import com.minimq.common.error.ErrorCode;

import java.util.concurrent.atomic.AtomicReference;

public final class BrokerAvailabilityGuard {
    public enum State {
        STARTING,
        ACTIVE,
        STORAGE_DEGRADED,
        FENCED,
        RECOVERING
    }

    private final AtomicReference<State> state = new AtomicReference<>(State.STARTING);

    public State state() {
        return state.get();
    }

    public boolean allowsDataPlane() {
        return state() == State.ACTIVE;
    }

    public boolean allowsProduce() {
        return state() == State.ACTIVE;
    }

    public boolean allowsFetch() {
        State current = state();
        return current == State.ACTIVE || current == State.STORAGE_DEGRADED;
    }

    public boolean allowsMaintenanceRun() {
        return state() == State.ACTIVE;
    }

    public boolean allowsMaintenanceRead() {
        return true;
    }

    public ErrorCode rejectionError() {
        return state() == State.STORAGE_DEGRADED ? ErrorCode.MAINTENANCE_BUSY : ErrorCode.BROKER_FENCED;
    }

    public void activate() {
        state.set(State.ACTIVE);
    }

    public void fence() {
        state.set(State.FENCED);
    }

    public void degradeStorage() {
        state.set(State.STORAGE_DEGRADED);
    }

    public void startRecovering() {
        state.set(State.RECOVERING);
    }
}
