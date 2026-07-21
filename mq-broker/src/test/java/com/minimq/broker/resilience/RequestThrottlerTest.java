package com.minimq.broker.resilience;

import com.minimq.protocol.ApiKeys;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RequestThrottlerTest {

    @Test
    void disabledLimitAlwaysAllowsRequests() {
        RequestThrottler throttler = new RequestThrottler(0);

        ThrottleResult first = throttler.tryAcquire(ApiKeys.PRODUCE);
        ThrottleResult second = throttler.tryAcquire(ApiKeys.FETCH);

        assertTrue(first.allowed());
        assertTrue(second.allowed());
        assertEquals(0, throttler.inFlight());
    }

    @Test
    void rejectsRequestsAboveMaxInFlightUntilAnAcceptedRequestReleases() {
        RequestThrottler throttler = new RequestThrottler(1);

        ThrottleResult first = throttler.tryAcquire(ApiKeys.PRODUCE);
        ThrottleResult rejected = throttler.tryAcquire(ApiKeys.FETCH);

        assertTrue(first.allowed());
        assertFalse(rejected.allowed());
        assertEquals(1, throttler.inFlight());

        first.release();
        first.release();

        assertEquals(0, throttler.inFlight());
        assertTrue(throttler.tryAcquire(ApiKeys.FETCH).allowed());
    }
}
