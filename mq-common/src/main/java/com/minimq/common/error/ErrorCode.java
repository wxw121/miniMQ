package com.minimq.common.error;

/**
 * 协议级错误码枚举，统一表示 Broker 处理请求后的成功或失败原因。
 */
public enum ErrorCode {
    NONE(0),
    UNKNOWN_TOPIC(1),
    UNKNOWN_PARTITION(2),
    NOT_LEADER(3),
    INVALID_REQUEST(4),
    GROUP_NOT_FOUND(5),
    REBALANCE_IN_PROGRESS(6),
    UNAUTHORIZED(7),
    MAINTENANCE_BUSY(8),
    BROKER_FENCED(9),
    BROKER_ID_IN_USE(10),
    CONTROLLER_FENCED(11),
    NOT_CONTROLLER(12),
    STALE_LEADER_EPOCH(13),
    LEADER_NOT_READY(14),
    CONTROLLER_NOT_READY(15),
    THROTTLED(16);

    public final short code;

    ErrorCode(int code) {
        this.code = (short) code;
    }

    public static ErrorCode fromCode(short code) {
        for (ErrorCode value : values()) {
            if (value.code == code) {
                return value;
            }
        }
        return INVALID_REQUEST;
    }
}
