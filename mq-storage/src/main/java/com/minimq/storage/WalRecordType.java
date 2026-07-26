package com.minimq.storage;

import java.io.IOException;

/**
 * WAL record type identifiers.
 */
public enum WalRecordType {
    DATA_APPEND((byte) 1),
    CHECKPOINT_MARKER((byte) 2),
    CONTROL_MARKER((byte) 3),
    TRUNCATE_BEGIN((byte) 4),
    TRUNCATE_COMMIT((byte) 5),
    TOPIC_DELETE_BEGIN((byte) 6),
    TOPIC_DELETE_COMMIT((byte) 7),
    SCHEDULED_DATA_APPEND((byte) 8);

    private final byte id;

    WalRecordType(byte id) {
        this.id = id;
    }

    public byte id() {
        return id;
    }

    static WalRecordType fromId(byte id, long recordStart) throws IOException {
        for (WalRecordType type : values()) {
            if (type.id == id) {
                return type;
            }
        }
        throw new IOException("Corrupt WAL at byte " + recordStart + ": unknown record type " + id);
    }
}
