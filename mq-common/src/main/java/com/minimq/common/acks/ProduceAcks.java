package com.minimq.common.acks;

/**
 * Producer acknowledgement policy. Only Leader-local and current-ISR waits are supported.
 */
public enum ProduceAcks {
    ALL((short) -1),
    LEADER((short) 1);

    public static final int DEFAULT_TIMEOUT_MS = 5_000;
    public static final int MAX_TIMEOUT_MS = 60_000;

    private final short code;

    ProduceAcks(short code) {
        this.code = code;
    }

    public short code() {
        return code;
    }

    public static ProduceAcks fromCode(short code) {
        return switch (code) {
            case -1 -> ALL;
            case 1 -> LEADER;
            default -> throw new IllegalArgumentException("unsupported acks: " + code);
        };
    }

    public static void requireValid(short code) {
        fromCode(code);
    }

    public static void requireTimeoutMs(int ackTimeoutMs) {
        if (ackTimeoutMs < 1 || ackTimeoutMs > MAX_TIMEOUT_MS) {
            throw new IllegalArgumentException(
                    "ackTimeoutMs must be in 1.." + MAX_TIMEOUT_MS + ": " + ackTimeoutMs);
        }
    }
}
