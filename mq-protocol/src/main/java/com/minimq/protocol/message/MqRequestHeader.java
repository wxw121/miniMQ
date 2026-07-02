package com.minimq.protocol.message;

public record MqRequestHeader(short apiKey, int correlationId, String clientId) {
}
