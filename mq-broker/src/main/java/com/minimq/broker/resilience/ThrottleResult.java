package com.minimq.broker.resilience;

import java.util.concurrent.atomic.AtomicBoolean;

public final class ThrottleResult {
    private static final ThrottleResult REJECTED = new ThrottleResult(false, () -> { });
    private static final ThrottleResult DISABLED_ALLOWED = new ThrottleResult(true, () -> { });

    private final boolean allowed;
    private final Runnable release;
    private final AtomicBoolean released = new AtomicBoolean();

    private ThrottleResult(boolean allowed, Runnable release) {
        this.allowed = allowed;
        this.release = release;
    }

    static ThrottleResult rejected() {
        return REJECTED;
    }

    static ThrottleResult disabledAllowed() {
        return DISABLED_ALLOWED;
    }

    static ThrottleResult accepted(Runnable release) {
        return new ThrottleResult(true, release);
    }

    public boolean allowed() {
        return allowed;
    }

    public void release() {
        if (allowed && released.compareAndSet(false, true)) {
            release.run();
        }
    }
}
