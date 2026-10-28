package com.example.orderproducer.controller;

import com.example.orderproducer.dto.OrderRequest;
import com.example.orderproducer.dto.OrderResponse;
import com.example.orderproducer.event.OrderEvent;
import com.example.orderproducer.service.OrderProducerService;
import jakarta.validation.Valid;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.support.SendResult;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * HTTP entry point of the Producer.
 *
 * <p>Returns <b>202 Accepted</b> rather than 201 Created: the order has been accepted and safely
 * stored in Kafka, but it has not been <i>processed</i> yet - that happens later in the Consumer.
 * Nothing has been "created" at a URL the client could GET.
 */
@RestController
@RequestMapping("/api/orders")
public class OrderController {

    private static final Logger log = LoggerFactory.getLogger(OrderController.class);

    private final OrderProducerService orderProducerService;

    public OrderController(OrderProducerService orderProducerService) {
        this.orderProducerService = orderProducerService;
    }

    /**
     * {@code @RequestBody}: Jackson converts the JSON body into an {@link OrderRequest}.
     * {@code @Valid}: the annotations on OrderRequest are checked BEFORE this method runs;
     * failures throw MethodArgumentNotValidException → 400 (see GlobalExceptionHandler).
     */
    @PostMapping
    public ResponseEntity<OrderResponse> createOrder(@Valid @RequestBody OrderRequest request) {
        log.info("Received order request {}", request.orderId());

        OrderEvent event = OrderEvent.from(request);
        SendResult<String, OrderEvent> result = orderProducerService.publish(event);
        RecordMetadata metadata = result.getRecordMetadata();

        OrderResponse body = new OrderResponse(
                event.orderId(), "ACCEPTED", metadata.topic(), metadata.partition(), metadata.offset());
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(body);
    }
}
