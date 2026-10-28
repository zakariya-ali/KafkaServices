package com.example.orderproducer.dto;

/**
 * Body returned to the client once Kafka has acknowledged the event.
 * Partition and offset prove where Kafka stored the message.
 */
public record OrderResponse(
        String orderId,
        String status,
        String topic,
        int partition,
        long offset
) {
}
