package com.minimq.protocol.message;

import com.minimq.common.error.ErrorCode;

public record MqResponseHeader(int correlationId, ErrorCode errorCode) {
}
