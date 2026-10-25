package com.minimq.broker;

import com.minimq.broker.coordination.ControllerIdentity;

/** Dynamic view of Controller leadership used by request fencing and discovery. */
public interface ControllerRole {
    boolean isActive();

    ControllerIdentity currentLeader();

    static ControllerRole legacy(BrokerConfig config) {
        return new ControllerRole() {
            @Override
            public boolean isActive() {
                return config.controller();
            }

            @Override
            public ControllerIdentity currentLeader() {
                int brokerId = config.controller() ? config.brokerId() : 0;
                String host = config.controller() ? config.host() : config.controllerHost();
                int port = config.controller() ? config.port() : config.controllerPort();
                return new ControllerIdentity(brokerId, host, port, 0L, "legacy");
            }
        };
    }
}
