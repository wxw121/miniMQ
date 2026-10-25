package com.minimq.protocol.message;

import com.minimq.common.error.ErrorCode;

/**
 * 响应头模型，携带 correlationId 与统一错误码。
 */
public record MqResponseHeader(int correlationId, ErrorCode errorCode) {
}
