package com.example.orderconsumer;

import com.example.orderconsumer.event.OrderEvent;
import com.example.orderconsumer.service.OrderProcessingService;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

/**
 * End-to-end test against a REAL Kafka broker started in Docker by Testcontainers.
 *
 * <p>We publish plain JSON strings - exactly what the Producer service sends (no Java type
 * headers) - and check that the listener deserializes and processes them.
 * Skipped automatically when Docker is not available.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@Testcontainers(disabledWithoutDocker = true)
class OrderConsumerIntegrationTest {

    @Container
    @ServiceConnection // Spring Boot points spring.kafka.bootstrap-servers at this container
    static KafkaContainer kafka = new KafkaContainer("apache/kafka:3.9.1");

    @MockitoSpyBean // real bean, but Mockito records its calls so we can verify them
    private OrderProcessingService orderProcessingService;

    @Test
    void consumesPlainJsonPublishedWithoutTypeHeaders() {
        send("ORD-IT-1", """
                {"eventId":"evt-it-1","orderId":"ORD-IT-1","productName":"Laptop",
                 "quantity":2,"price":799.99,"createdAt":"2026-10-28T10:15:30Z"}
                """);

        verify(orderProcessingService, timeout(20_000)).process(argThat((OrderEvent e) ->
                e.orderId().equals("ORD-IT-1")
                        && e.quantity() == 2
                        && e.price().compareTo(new BigDecimal("799.99")) == 0));
    }

    @Test
    void sendsPoisonMessageToDeadLetterTopic() {
        send("ORD-BAD", "this is not json");

        try (Consumer<String, String> dltConsumer = dltConsumer()) {
            dltConsumer.subscribe(List.of("order-events.DLT"));
            ConsumerRecord<String, String> record =
                    KafkaTestUtils.getSingleRecord(dltConsumer, "order-events.DLT", Duration.ofSeconds(30));
            assertThat(record.key()).isEqualTo("ORD-BAD");
            assertThat(record.value()).isEqualTo("this is not json");
        }
    }

    private static void send(String key, String json) {
        Map<String, Object> props = Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            producer.send(new ProducerRecord<>("order-events", key, json));
        }
    }

    private static Consumer<String, String> dltConsumer() {
        return new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "dlt-test",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class));
    }
}
