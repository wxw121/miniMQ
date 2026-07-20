package com.minimq.broker.coordination;

public enum ControllerState {
    FOLLOWER,
    CAMPAIGNING,
    RECOVERING,
    ACTIVE,
    FENCED
}
