package com.example.orderconsumer.service;

/**
 * The event is permanently invalid - retrying will never fix it.
 * The error handler is configured NOT to retry this exception and sends the record
 * straight to the dead-letter topic.
 */
public class InvalidOrderException extends RuntimeException {

    public InvalidOrderException(String message) {
        super(message);
    }
}
