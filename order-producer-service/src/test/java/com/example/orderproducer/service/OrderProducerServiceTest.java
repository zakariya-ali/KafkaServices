package com.example.orderproducer.service;

import com.example.orderproducer.event.OrderEvent;
import com.example.orderproducer.exception.OrderPublishingException;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Pure unit test: no Spring context, no Kafka. KafkaTemplate is a Mockito mock and we control
 * the CompletableFuture it returns to simulate success and failure.
 */
@ExtendWith(MockitoExtension.class)
class OrderProducerServiceTest {

    private static final String TOPIC = "order-events";

    @Mock
    private KafkaTemplate<String, OrderEvent> kafkaTemplate;

    private OrderProducerService service;

    @BeforeEach
    void setUp() {
        service = new OrderProducerService(kafkaTemplate, TOPIC, Duration.ofSeconds(1));
    }

    @Test
    void publishSendsEventWithOrderIdAsKeyAndReturnsResult() {
        OrderEvent event = sampleEvent();
        SendResult<String, OrderEvent> sendResult = new SendResult<>(
                new ProducerRecord<>(TOPIC, event.orderId(), event),
                new RecordMetadata(new TopicPartition(TOPIC, 1), 42, 0, 0L, 0, 0));
        when(kafkaTemplate.send(TOPIC, "ORD-101", event))
                .thenReturn(CompletableFuture.completedFuture(sendResult));

        SendResult<String, OrderEvent> result = service.publish(event);

        verify(kafkaTemplate).send(TOPIC, "ORD-101", event);
        assertThat(result.getRecordMetadata().partition()).isEqualTo(1);
        assertThat(result.getRecordMetadata().offset()).isEqualTo(42);
    }

    @Test
    void publishThrowsWhenKafkaFails() {
        OrderEvent event = sampleEvent();
        when(kafkaTemplate.send(TOPIC, "ORD-101", event))
                .thenReturn(CompletableFuture.failedFuture(new TimeoutException("broker not available")));

        assertThatThrownBy(() -> service.publish(event))
                .isInstanceOf(OrderPublishingException.class)
                .hasMessageContaining("ORD-101")
                .hasRootCauseInstanceOf(TimeoutException.class);
    }

    @Test
    void publishThrowsWhenKafkaNeverAnswers() {
        OrderEvent event = sampleEvent();
        when(kafkaTemplate.send(TOPIC, "ORD-101", event)).thenReturn(new CompletableFuture<>());

        assertThatThrownBy(() -> service.publish(event))
                .isInstanceOf(OrderPublishingException.class)
                .hasMessageContaining("Timed out");
    }

    private static OrderEvent sampleEvent() {
        return new OrderEvent("evt-1", "ORD-101", "Laptop", 2, new BigDecimal("799.99"), Instant.now());
    }
}
