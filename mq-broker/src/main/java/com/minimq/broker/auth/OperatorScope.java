package com.minimq.broker.auth;

import java.util.Locale;

public enum OperatorScope {
    STATUS,
    TOPIC_ADMIN,
    MAINTENANCE,
    BROKER_OFFLINE;

    public static OperatorScope parse(String value) {
        try {
            return OperatorScope.valueOf(value.trim().toUpperCase(Locale.ROOT));
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("Unknown operator scope: " + value, e);
        }
    }
}
