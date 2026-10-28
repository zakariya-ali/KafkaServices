package com.example.orderconsumer.consumer;

import com.example.orderconsumer.event.OrderEvent;
import com.example.orderconsumer.service.OrderProcessingService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Kafka entry point of the Consumer - the messaging equivalent of a REST controller.
 *
 * <p>Nobody calls {@link #onOrderEvent} directly. At startup Spring creates a
 * <i>listener container</i> for this method: a background thread that continuously polls Kafka.
 * For each record it receives, the container deserializes the value into an {@link OrderEvent}
 * and invokes this method. When the method returns normally, the offset is committed
 * ("I have handled everything up to here"). If it throws, the {@code DefaultErrorHandler}
 * configured in {@code KafkaConsumerConfig} decides whether to retry or dead-letter the record.
 */
@Component
public class OrderConsumer {

    private static final Logger log = LoggerFactory.getLogger(OrderConsumer.class);

    private final OrderProcessingService orderProcessingService;

    public OrderConsumer(OrderProcessingService orderProcessingService) {
        this.orderProcessingService = orderProcessingService;
    }

    @KafkaListener(topics = "${app.kafka.topic}", groupId = "${spring.kafka.consumer.group-id}")
    public void onOrderEvent(ConsumerRecord<String, OrderEvent> record) {
        log.info("Received order event key={} partition={} offset={}",
                record.key(), record.partition(), record.offset());

        orderProcessingService.process(record.value());
    }
}
