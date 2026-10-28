package com.example.orderconsumer.consumer;

import com.example.orderconsumer.event.OrderEvent;
import com.example.orderconsumer.service.InvalidOrderException;
import com.example.orderconsumer.service.OrderProcessingService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OrderConsumerTest {

    @Mock
    private OrderProcessingService orderProcessingService;

    @InjectMocks
    private OrderConsumer orderConsumer;

    @Test
    void delegatesEventToProcessingService() {
        OrderEvent event = new OrderEvent("evt-1", "ORD-101", "Laptop", 2, new BigDecimal("799.99"), Instant.now());

        orderConsumer.onOrderEvent(new ConsumerRecord<>("order-events", 0, 5L, "ORD-101", event));

        verify(orderProcessingService).process(event);
    }

    @Test
    void propagatesProcessingFailureSoTheErrorHandlerCanRetryOrDeadLetter() {
        OrderEvent event = new OrderEvent("evt-1", "ORD-101", "Laptop", 0, BigDecimal.ONE, Instant.now());
        when(orderProcessingService.process(event)).thenThrow(new InvalidOrderException("bad"));

        assertThatThrownBy(() -> orderConsumer.onOrderEvent(
                new ConsumerRecord<>("order-events", 0, 6L, "ORD-101", event)))
                .isInstanceOf(InvalidOrderException.class);
    }
}
