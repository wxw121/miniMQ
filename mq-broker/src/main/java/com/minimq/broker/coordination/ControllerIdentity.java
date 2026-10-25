package com.minimq.broker.coordination;

/** Stable, lease-bound identity of the elected controller. */
public record ControllerIdentity(
        int brokerId,
        String host,
        int port,
        long controllerEpoch,
        String sessionId) {
    public ControllerIdentity {
        if (brokerId < 0 || host == null || host.isBlank() || port <= 0 || port > 65535
                || controllerEpoch < 0 || sessionId == null || sessionId.isBlank()) {
            throw new IllegalArgumentException("invalid controller identity");
        }
    }
}
