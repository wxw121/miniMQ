package com.minimq.client.exception;

import java.time.Duration;

public class MiniMqRequestTimeoutException extends MiniMqClientException {
    public MiniMqRequestTimeoutException(String apiName, Duration timeout) {
        super("Request timeout for " + apiName + " after " + timeout.toMillis() + " ms");
    }
}
