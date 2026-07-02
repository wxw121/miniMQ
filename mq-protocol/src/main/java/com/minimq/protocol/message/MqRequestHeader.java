package com.minimq.protocol.message;

/**
 * 请求头模型，携带 API 编号、correlationId 和 clientId。
 */
public record MqRequestHeader(short apiKey, int correlationId, String clientId) {
}
