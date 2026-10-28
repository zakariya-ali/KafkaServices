package com.example.orderconsumer.event;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * The Consumer's own copy of the order event contract.
 *
 * <p>It lives in a different package from the Producer's class - and that's fine, because the
 * Producer sends plain JSON without Java type headers. Deserialization only cares that the JSON
 * field names match these component names.
 *
 * <p>{@code @JsonIgnoreProperties(ignoreUnknown = true)} makes the consumer tolerant: if the
 * Producer later adds a new field, older consumers keep working instead of failing.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record OrderEvent(
        String eventId,
        String orderId,
        String productName,
        int quantity,
        BigDecimal price,
        Instant createdAt
) {
}
