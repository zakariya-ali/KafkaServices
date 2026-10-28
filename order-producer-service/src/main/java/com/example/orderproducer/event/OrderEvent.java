package com.example.orderproducer.event;

import com.example.orderproducer.dto.OrderRequest;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * The message published to the {@code order-events} topic.
 *
 * <p>This JSON shape is the <b>contract</b> between the Producer and the Consumer. The Consumer
 * has its own copy of this record (in its own package) - the two services share no code,
 * only the JSON field names. Example on the wire:
 *
 * <pre>
 * {"eventId":"6f1c...","orderId":"ORD-101","productName":"Laptop",
 *  "quantity":2,"price":799.99,"createdAt":"2026-10-28T10:15:30Z"}
 * </pre>
 *
 * @param eventId   unique id of this event (useful for tracing and de-duplication)
 * @param createdAt when the producer created the event (serialised as ISO-8601)
 */
public record OrderEvent(
        String eventId,
        String orderId,
        String productName,
        int quantity,
        BigDecimal price,
        Instant createdAt
) {

    public static OrderEvent from(OrderRequest request) {
        return new OrderEvent(
                UUID.randomUUID().toString(),
                request.orderId(),
                request.productName(),
                request.quantity(),
                request.price(),
                Instant.now()
        );
    }
}
