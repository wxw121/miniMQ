package com.minimq.broker;

import com.minimq.common.error.ErrorCode;

public final class BrokerAvailabilityGuard {
    public enum State {
        STARTING,
        ACTIVE,
        STORAGE_DEGRADED,
        FENCED,
        RECOVERING
    }

    private volatile State state = State.STARTING;
    private boolean stickyStorageFence;

    public State state() {
        return state;
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

    public synchronized void activate() {
        if (!stickyStorageFence) {
            state = State.ACTIVE;
        }
    }

    public synchronized void fence() {
        state = State.FENCED;
    }

    public synchronized void fenceStorageCleanup() {
        stickyStorageFence = true;
        state = State.FENCED;
    }

    public synchronized boolean hasStickyStorageFence() {
        return stickyStorageFence;
    }

    public synchronized void degradeStorage() {
        if (!stickyStorageFence) {
            state = State.STORAGE_DEGRADED;
        }
    }

    public synchronized void startRecovering() {
        if (!stickyStorageFence) {
            state = State.RECOVERING;
        }
    }
}
