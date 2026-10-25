package com.minimq.client.exception;

import com.minimq.common.error.ErrorCode;

public class MiniMqServerException extends MiniMqClientException {
    private final ErrorCode errorCode;

    public MiniMqServerException(ErrorCode errorCode) {
        super("Request failed: " + errorCode);
        this.errorCode = errorCode;
    }

    public MiniMqServerException(ErrorCode errorCode, String operation) {
        super(operation + " failed: " + errorCode);
        this.errorCode = errorCode;
    }

    public ErrorCode errorCode() {
        return errorCode;
    }
}
