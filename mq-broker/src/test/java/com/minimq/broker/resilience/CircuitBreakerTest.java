package com.minimq.broker.resilience;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CircuitBreakerTest {

    @Test
    void opensAfterFailureThresholdAndRejectsRequestsUntilCooldownExpires() {
        MutableClock clock = new MutableClock();
        CircuitBreaker breaker = new CircuitBreaker(2, 100, clock::now);

        assertTrue(breaker.allowRequest());
        breaker.recordFailure();
        assertEquals(CircuitBreaker.State.CLOSED, breaker.state());

        breaker.recordFailure();
        assertEquals(CircuitBreaker.State.OPEN, breaker.state());
        assertFalse(breaker.allowRequest());

        clock.advance(100);
        assertTrue(breaker.allowRequest());
        assertEquals(CircuitBreaker.State.HALF_OPEN, breaker.state());

        breaker.recordSuccess();
        assertEquals(CircuitBreaker.State.CLOSED, breaker.state());
        assertTrue(breaker.allowRequest());
    }

    @Test
    void halfOpenFailureReopensForAnotherCooldownWindow() {
        MutableClock clock = new MutableClock();
        CircuitBreaker breaker = new CircuitBreaker(1, 50, clock::now);

        breaker.recordFailure();
        clock.advance(50);
        assertTrue(breaker.allowRequest());
        breaker.recordFailure();

        assertEquals(CircuitBreaker.State.OPEN, breaker.state());
        assertFalse(breaker.allowRequest());
        clock.advance(49);
        assertFalse(breaker.allowRequest());
        clock.advance(1);
        assertTrue(breaker.allowRequest());
    }

    @Test
    void rejectsNonPositiveConfiguration() {
        MutableClock clock = new MutableClock();

        assertThrows(IllegalArgumentException.class, () -> new CircuitBreaker(0, 1, clock::now));
        assertThrows(IllegalArgumentException.class, () -> new CircuitBreaker(1, 0, clock::now));
    }

    @Test
    void stateTransitionsToHalfOpenAfterCooldown() {
        MutableClock clock = new MutableClock();
        CircuitBreaker breaker = new CircuitBreaker(1, 50, clock::now);

        breaker.recordFailure();
        clock.advance(50);

        assertEquals(CircuitBreaker.State.HALF_OPEN, breaker.state());
    }

    private static final class MutableClock {
        private long now;

        long now() {
            return now;
        }

        void advance(long millis) {
            now += millis;
        }
    }
}
