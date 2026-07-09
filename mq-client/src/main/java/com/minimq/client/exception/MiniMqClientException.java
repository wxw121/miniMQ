package com.minimq.client.exception;

public class MiniMqClientException extends IllegalStateException {
    public MiniMqClientException(String message) {
        super(message);
    }

    public MiniMqClientException(String message, Throwable cause) {
        super(message, cause);
    }
}
