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
    REBALANCE_IN_PROGRESS(6);

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
