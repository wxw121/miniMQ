package com.minimq.protocol;

public final class ApiKeys {
    public static final short CREATE_TOPIC = 0;
    public static final short METADATA = 1;
    public static final short PRODUCE = 2;
    public static final short FETCH = 3;
    public static final short JOIN_GROUP = 4;
    public static final short SYNC_GROUP = 5;
    public static final short HEARTBEAT = 6;
    public static final short OFFSET_COMMIT = 7;
    public static final short OFFSET_FETCH = 8;
    public static final short REGISTER_BROKER = 9;

    private ApiKeys() {
    }
}
