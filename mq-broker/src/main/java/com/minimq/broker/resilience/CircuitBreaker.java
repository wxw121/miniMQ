package com.minimq.broker.resilience;

import java.util.function.LongSupplier;

public final class CircuitBreaker {
    public enum State {
        CLOSED,
        OPEN,
        HALF_OPEN
    }

    private final int failureThreshold;
    private final long openDurationMs;
    private final LongSupplier currentTimeMs;
    private State state = State.CLOSED;
    private int consecutiveFailures;
    private long openedAtMs = -1L;

    public CircuitBreaker(int failureThreshold, long openDurationMs, LongSupplier currentTimeMs) {
        if (failureThreshold <= 0) {
            throw new IllegalArgumentException("failureThreshold must be positive");
        }
        if (openDurationMs <= 0) {
            throw new IllegalArgumentException("openDurationMs must be positive");
        }
        this.failureThreshold = failureThreshold;
        this.openDurationMs = openDurationMs;
        this.currentTimeMs = currentTimeMs;
    }

    public synchronized boolean allowRequest() {
        if (state != State.OPEN) {
            return true;
        }
        if (currentTimeMs.getAsLong() - openedAtMs >= openDurationMs) {
            state = State.HALF_OPEN;
            return true;
        }
        return false;
    }

    public synchronized void recordSuccess() {
        consecutiveFailures = 0;
        state = State.CLOSED;
        openedAtMs = -1L;
    }

    public synchronized void recordFailure() {
        if (state == State.HALF_OPEN) {
            open();
            return;
        }
        consecutiveFailures++;
        if (consecutiveFailures >= failureThreshold) {
            open();
        }
    }

    public synchronized State state() {
        if (state == State.OPEN && currentTimeMs.getAsLong() - openedAtMs >= openDurationMs) {
            state = State.HALF_OPEN;
        }
        return state;
    }

    private void open() {
        state = State.OPEN;
        openedAtMs = currentTimeMs.getAsLong();
    }
}
