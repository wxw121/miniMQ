package com.minimq.broker.coordination;

public class ControllerFencedException extends RuntimeException {
    public ControllerFencedException(String message) {
        super(message);
    }

    public ControllerFencedException(String message, Throwable cause) {
        super(message, cause);
    }
}
