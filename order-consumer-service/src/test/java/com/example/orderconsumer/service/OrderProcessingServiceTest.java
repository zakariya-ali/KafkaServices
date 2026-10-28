package com.example.orderconsumer.service;

import com.example.orderconsumer.event.OrderEvent;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OrderProcessingServiceTest {

    private final OrderProcessingService service = new OrderProcessingService();

    @Test
    void processesValidOrder() {
        assertThat(service.process(event("evt-1", "ORD-101", 2, "799.99"))).isTrue();
    }

    @Test
    void skipsDuplicateEvent() {
        OrderEvent event = event("evt-1", "ORD-101", 2, "799.99");

        assertThat(service.process(event)).isTrue();
        assertThat(service.process(event)).isFalse();
    }

    @Test
    void sameOrderWithDifferentEventIdIsProcessedAgain() {
        assertThat(service.process(event("evt-1", "ORD-101", 2, "799.99"))).isTrue();
        assertThat(service.process(event("evt-2", "ORD-101", 2, "799.99"))).isTrue();
    }

    @Test
    void rejectsMissingOrderId() {
        assertThatThrownBy(() -> service.process(event("evt-1", " ", 1, "10")))
                .isInstanceOf(InvalidOrderException.class)
                .hasMessageContaining("orderId");
    }

    @Test
    void rejectsNonPositiveQuantity() {
        assertThatThrownBy(() -> service.process(event("evt-1", "ORD-1", 0, "10")))
                .isInstanceOf(InvalidOrderException.class)
                .hasMessageContaining("quantity");
    }

    @Test
    void rejectsNonPositivePrice() {
        assertThatThrownBy(() -> service.process(event("evt-1", "ORD-1", 1, "-5")))
                .isInstanceOf(InvalidOrderException.class)
                .hasMessageContaining("price");
    }

    @Test
    void rejectsNullEvent() {
        assertThatThrownBy(() -> service.process(null)).isInstanceOf(InvalidOrderException.class);
    }

    private static OrderEvent event(String eventId, String orderId, int quantity, String price) {
        return new OrderEvent(eventId, orderId, "Laptop", quantity, new BigDecimal(price), Instant.now());
    }
}
