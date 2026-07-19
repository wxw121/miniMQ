package com.minimq.broker;

import com.minimq.common.error.ErrorCode;

import java.util.concurrent.atomic.AtomicReference;

public final class BrokerAvailabilityGuard {
    public enum State {
        STARTING,
        ACTIVE,
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

    public boolean allowsMaintenanceRun() {
        return state() == State.ACTIVE;
    }

    public boolean allowsMaintenanceRead() {
        return true;
    }

    public ErrorCode rejectionError() {
        return ErrorCode.BROKER_FENCED;
    }

    public void activate() {
        state.set(State.ACTIVE);
    }

    public void fence() {
        state.set(State.FENCED);
    }

    public void startRecovering() {
        state.set(State.RECOVERING);
    }
}
