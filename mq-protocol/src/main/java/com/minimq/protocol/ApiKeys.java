package com.minimq.protocol;

/**
 * MiniMQ 协议 API 编号表，客户端和 Broker 必须保持完全一致。
 */
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
    public static final short REPLICA_FETCH = 10;
    public static final short MARK_BROKER_OFFLINE = 11;
    public static final short UPDATE_REPLICA_LEO = 12;
    public static final short MAINTENANCE_REPORT = 13;
    public static final short MAINTENANCE_RUN = 14;
    public static final short MAINTENANCE_METRICS = 15;
    public static final short BROKER_HEARTBEAT = 16;
    public static final short DISCOVER_CONTROLLER = 17;
    public static final short LEADER_READY = 18;
    public static final short RUNTIME_METRICS = 19;
    public static final short OPS_STATUS = 20;
    public static final short GROUP_STATUS = 21;
    public static final short LIST_TOPICS = 22;
    public static final short DESCRIBE_TOPIC = 23;
    public static final short DELETE_TOPIC = 24;
    public static final short INCREASE_PARTITIONS = 25;
    public static final short FAILOVER_AUDIT = 26;
    public static final short AUTH_STATUS = 27;
    public static final short CONFIGURE_TOPIC_RELIABILITY = 28;
    public static final short MARK_BROKER_OFFLINE_ADMIN = 29;
    public static final short CREATE_TOPIC_ADMIN = 30;
    public static final short PRODUCE_V2 = 31;
    public static final short REPLICA_FETCH_V2 = 32;
    public static final short TOPIC_DELETE_ACK = 33;
    public static final short FETCH_V2 = 34;
    public static final short METADATA_V2 = 35;
    public static final short RUNTIME_METRICS_V2 = 36;
    public static final short UPDATE_REPLICA_STATE_V2 = 37;
    public static final short LEAVE_GROUP = 38;

    private static final String[] NAMES = {
            "CREATE_TOPIC",
            "METADATA",
            "PRODUCE",
            "FETCH",
            "JOIN_GROUP",
            "SYNC_GROUP",
            "HEARTBEAT",
            "OFFSET_COMMIT",
            "OFFSET_FETCH",
            "REGISTER_BROKER",
            "REPLICA_FETCH",
            "MARK_BROKER_OFFLINE",
            "UPDATE_REPLICA_LEO",
            "MAINTENANCE_REPORT",
            "MAINTENANCE_RUN",
            "MAINTENANCE_METRICS",
            "BROKER_HEARTBEAT",
            "DISCOVER_CONTROLLER",
            "LEADER_READY",
            "RUNTIME_METRICS",
            "OPS_STATUS",
            "GROUP_STATUS",
            "LIST_TOPICS",
            "DESCRIBE_TOPIC",
            "DELETE_TOPIC",
            "INCREASE_PARTITIONS",
            "FAILOVER_AUDIT",
            "AUTH_STATUS",
            "CONFIGURE_TOPIC_RELIABILITY",
            "MARK_BROKER_OFFLINE_ADMIN",
            "CREATE_TOPIC_ADMIN",
            "PRODUCE_V2",
            "REPLICA_FETCH_V2",
            "TOPIC_DELETE_ACK",
            "FETCH_V2",
            "METADATA_V2",
            "RUNTIME_METRICS_V2",
            "UPDATE_REPLICA_STATE_V2",
            "LEAVE_GROUP"
    };

    public static String nameOf(short apiKey) {
        return apiKey >= 0 && apiKey < NAMES.length
                ? NAMES[apiKey]
                : "apiKey=" + apiKey;
    }

    private ApiKeys() {
    }
}
