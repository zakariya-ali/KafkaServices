package com.example.orderconsumer.service;

import com.example.orderconsumer.event.OrderEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Business logic for an incoming order. Kept separate from {@code OrderConsumer} so that it can
 * be unit-tested without Kafka, and so the listener class only deals with "messaging" concerns.
 *
 * <p>Kafka guarantees <b>at-least-once</b> delivery with this setup, so the same event can arrive
 * twice (e.g. the consumer crashes after processing but before committing the offset).
 * We make processing <b>idempotent</b> by remembering which eventIds were already handled.
 * The in-memory set is a teaching simplification: it is lost on restart and not shared between
 * instances. A real service would store processed ids in a database (unique constraint).
 */
@Service
public class OrderProcessingService {

    private static final Logger log = LoggerFactory.getLogger(OrderProcessingService.class);

    private final Set<String> processedEventIds = ConcurrentHashMap.newKeySet();

    /**
     * @return {@code true} if the order was processed, {@code false} if it was a duplicate
     * @throws InvalidOrderException if the event can never be processed
     */
    public boolean process(OrderEvent event) {
        validate(event);

        String dedupKey = event.eventId() != null ? event.eventId() : event.orderId();
        if (!processedEventIds.add(dedupKey)) {
            log.warn("Duplicate event {} for order {} - skipping", dedupKey, event.orderId());
            return false;
        }

        BigDecimal total = event.price().multiply(BigDecimal.valueOf(event.quantity()));
        log.info("Processing order {}: {} x {} @ {} = total {}",
                event.orderId(), event.quantity(), event.productName(), event.price(), total);

        // Real work would go here: save to a database, reserve stock, call a payment service...

        log.info("Order {} processed successfully", event.orderId());
        return true;
    }

    private static void validate(OrderEvent event) {
        if (event == null) {
            throw new InvalidOrderException("Order event is null");
        }
        if (event.orderId() == null || event.orderId().isBlank()) {
            throw new InvalidOrderException("orderId is missing");
        }
        if (event.quantity() <= 0) {
            throw new InvalidOrderException("quantity must be > 0 for order " + event.orderId());
        }
        if (event.price() == null || event.price().signum() <= 0) {
            throw new InvalidOrderException("price must be > 0 for order " + event.orderId());
        }
    }
}
