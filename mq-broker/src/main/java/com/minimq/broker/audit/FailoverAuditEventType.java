package com.minimq.broker.audit;

/** State transitions that are useful when diagnosing a failover. */
public enum FailoverAuditEventType {
    CONTROLLER_TAKEOVER,
    BROKER_LEASE_EXPIRED,
    BROKER_SELF_FENCED,
    LEADER_CHANGED,
    ISR_SHRUNK,
    ISR_EXPANDED,
    STALE_EPOCH_REJECTED,
    LEADER_READY,
    LEADER_NOT_READY
}
