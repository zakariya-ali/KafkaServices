package com.example.orderproducer.service;

import com.example.orderproducer.event.OrderEvent;
import com.example.orderproducer.exception.OrderPublishingException;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Publishes {@link OrderEvent}s to Kafka.
 *
 * <p>{@code KafkaTemplate.send(...)} is <b>asynchronous</b>: it puts the record in an in-memory
 * buffer and immediately returns a {@link CompletableFuture}. The future completes later, when the
 * broker answers. A message is only "successfully published" when that future completes
 * <i>without</i> an exception - with {@code acks=all} that means the leader and all in-sync
 * replicas have written it to their log.
 *
 * <p>Because the HTTP client deserves an honest answer, we wait (with a timeout) for the broker's
 * acknowledgement before returning. If it fails we throw {@link OrderPublishingException}, which
 * the {@code GlobalExceptionHandler} turns into HTTP 503 instead of a misleading success.
 */
@Service
public class OrderProducerService {

    private static final Logger log = LoggerFactory.getLogger(OrderProducerService.class);

    private final KafkaTemplate<String, OrderEvent> kafkaTemplate;
    private final String topic;
    private final Duration sendTimeout;

    // Constructor injection: Spring passes in the KafkaTemplate bean and the configured values.
    // Fields can be final, and tests can create the class with plain `new`.
    public OrderProducerService(KafkaTemplate<String, OrderEvent> kafkaTemplate,
                                @Value("${app.kafka.topic}") String topic,
                                @Value("${app.kafka.send-timeout:10s}") Duration sendTimeout) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
        this.sendTimeout = sendTimeout;
    }

    public SendResult<String, OrderEvent> publish(OrderEvent event) {
        // orderId is the message KEY: Kafka hashes the key to choose a partition, so every event
        // for the same order lands in the same partition and is consumed in order.
        CompletableFuture<SendResult<String, OrderEvent>> future =
                kafkaTemplate.send(topic, event.orderId(), event);

        // Non-blocking callback: runs on the Kafka producer I/O thread when the broker answers.
        future.whenComplete((result, ex) -> {
            if (ex == null) {
                RecordMetadata metadata = result.getRecordMetadata();
                log.info("Published order {} to topic={} partition={} offset={}",
                        event.orderId(), metadata.topic(), metadata.partition(), metadata.offset());
            } else {
                log.error("Failed to publish order {}: {}", event.orderId(), ex.getMessage());
            }
        });

        try {
            // Block the HTTP thread until Kafka confirms (or we give up).
            return future.get(sendTimeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new OrderPublishingException("Interrupted while publishing order " + event.orderId(), e);
        } catch (ExecutionException e) {
            throw new OrderPublishingException("Kafka rejected order " + event.orderId(), e.getCause());
        } catch (TimeoutException e) {
            throw new OrderPublishingException("Timed out publishing order " + event.orderId(), e);
        }
    }
}
