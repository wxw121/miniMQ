package com.minimq.broker;

import com.minimq.common.error.ErrorCode;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BrokerAvailabilityGuardTest {

    @Test
    void onlyActiveStateAllowsDataAndMaintenanceMutation() {
        BrokerAvailabilityGuard guard = new BrokerAvailabilityGuard();

        assertEquals(BrokerAvailabilityGuard.State.STARTING, guard.state());
        assertFalse(guard.allowsDataPlane());
        assertFalse(guard.allowsMaintenanceRun());
        assertTrue(guard.allowsMaintenanceRead());

        guard.activate();
        assertEquals(BrokerAvailabilityGuard.State.ACTIVE, guard.state());
        assertTrue(guard.allowsDataPlane());
        assertTrue(guard.allowsMaintenanceRun());

        guard.fence();
        assertEquals(BrokerAvailabilityGuard.State.FENCED, guard.state());
        assertEquals(ErrorCode.BROKER_FENCED, guard.rejectionError());
        assertFalse(guard.allowsDataPlane());

        guard.startRecovering();
        assertEquals(BrokerAvailabilityGuard.State.RECOVERING, guard.state());
        assertFalse(guard.allowsDataPlane());

        guard.activate();
        assertTrue(guard.allowsDataPlane());
    }
}
