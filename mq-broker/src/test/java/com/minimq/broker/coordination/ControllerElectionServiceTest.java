package com.minimq.broker.coordination;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

class ControllerElectionServiceTest {
    @Test
    void becomesRecoveringThenActiveAndClosesLeadership() {
        AtomicBoolean closed = new AtomicBoolean();
        ControllerIdentity identity = new ControllerIdentity(1, "127.0.0.1", 9001, 7, "session");
        ControllerCoordinationStore store = new ControllerCoordinationStore() {
            @Override public Leadership campaign(int brokerId, String host, int port, int ttlSeconds) {
                return new Leadership() {
                    @Override public ControllerIdentity identity() { return identity; }
                    @Override public byte[] identityBytes() { return new byte[]{1}; }
                    @Override public boolean valid() { return !closed.get(); }
                    @Override public void close() { closed.set(true); }
                };
            }
            @Override public ControllerIdentity currentLeader() { return identity; }
            @Override public void close() { }
        };

        ControllerElectionService service = new ControllerElectionService(store, 1, "127.0.0.1", 9001, 3);
        ControllerCoordinationStore.Leadership leadership = service.campaign();
        assertEquals(ControllerState.RECOVERING, service.state());
        assertEquals(7, leadership.identity().controllerEpoch());
        service.activate();
        assertEquals(ControllerState.ACTIVE, service.state());
        service.close();
        assertEquals(ControllerState.FOLLOWER, service.state());
        assertTrue(closed.get());
    }

    @Test
    void failedCampaignFencesCandidate() {
        ControllerCoordinationStore store = new ControllerCoordinationStore() {
            @Override public Leadership campaign(int brokerId, String host, int port, int ttlSeconds) {
                throw new ControllerFencedException("lost lease");
            }
            @Override public ControllerIdentity currentLeader() { return null; }
            @Override public void close() { }
        };
        ControllerElectionService service = new ControllerElectionService(store, 1, "host", 9001, 3);
        assertThrows(ControllerFencedException.class, service::campaign);
        assertEquals(ControllerState.FENCED, service.state());
    }
}
