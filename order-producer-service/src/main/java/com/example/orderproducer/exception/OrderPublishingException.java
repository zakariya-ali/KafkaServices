package com.example.orderproducer.exception;

/**
 * Thrown when an order event could not be confirmed by Kafka
 * (broker down, timeout, serialization problem...). Mapped to HTTP 503.
 */
public class OrderPublishingException extends RuntimeException {

    public OrderPublishingException(String message, Throwable cause) {
        super(message, cause);
    }
}
