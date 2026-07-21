package com.minimq.broker.resilience;

import java.util.concurrent.atomic.AtomicInteger;

public final class RequestThrottler {
    private final int maxInFlightRequests;
    private final AtomicInteger inFlight = new AtomicInteger();

    public RequestThrottler(int maxInFlightRequests) {
        if (maxInFlightRequests < 0) {
            throw new IllegalArgumentException("maxInFlightRequests must be non-negative");
        }
        this.maxInFlightRequests = maxInFlightRequests;
    }

    public ThrottleResult tryAcquire(short apiKey) {
        if (maxInFlightRequests == 0) {
            return ThrottleResult.disabledAllowed();
        }
        while (true) {
            int current = inFlight.get();
            if (current >= maxInFlightRequests) {
                return ThrottleResult.rejected();
            }
            if (inFlight.compareAndSet(current, current + 1)) {
                return ThrottleResult.accepted(this::release);
            }
        }
    }

    public int inFlight() {
        return inFlight.get();
    }

    private void release() {
        inFlight.updateAndGet(current -> Math.max(0, current - 1));
    }
}
