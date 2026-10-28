package com.example.orderproducer.controller;

import com.example.orderproducer.event.OrderEvent;
import com.example.orderproducer.exception.OrderPublishingException;
import com.example.orderproducer.service.OrderProducerService;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.http.MediaType;
import org.springframework.kafka.support.SendResult;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Web-layer slice test: starts only Spring MVC (controller, advice, Jackson, validation),
 * not Kafka. The service is replaced by a Mockito mock via {@code @MockitoBean}.
 */
@WebMvcTest(OrderController.class)
class OrderControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private OrderProducerService orderProducerService;

    @Test
    void validOrderReturns202() throws Exception {
        when(orderProducerService.publish(any())).thenAnswer(invocation -> {
            OrderEvent event = invocation.getArgument(0);
            return new SendResult<>(new ProducerRecord<>("order-events", event.orderId(), event),
                    new RecordMetadata(new TopicPartition("order-events", 2), 7, 0, 0L, 0, 0));
        });

        mockMvc.perform(post("/api/orders").contentType(MediaType.APPLICATION_JSON).content("""
                        {"orderId":"ORD-101","productName":"Laptop","quantity":2,"price":799.99}
                        """))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.orderId").value("ORD-101"))
                .andExpect(jsonPath("$.status").value("ACCEPTED"))
                .andExpect(jsonPath("$.partition").value(2))
                .andExpect(jsonPath("$.offset").value(7));
    }

    @Test
    void missingOrderIdReturns400() throws Exception {
        mockMvc.perform(post("/api/orders").contentType(MediaType.APPLICATION_JSON).content("""
                        {"productName":"Laptop","quantity":2,"price":799.99}
                        """))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors.orderId").value("orderId must not be blank"));

        verify(orderProducerService, never()).publish(any());
    }

    @Test
    void negativeQuantityReturns400() throws Exception {
        mockMvc.perform(post("/api/orders").contentType(MediaType.APPLICATION_JSON).content("""
                        {"orderId":"ORD-102","productName":"Laptop","quantity":-1,"price":799.99}
                        """))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors.quantity").value("quantity must be greater than zero"));
    }

    @Test
    void zeroPriceReturns400() throws Exception {
        mockMvc.perform(post("/api/orders").contentType(MediaType.APPLICATION_JSON).content("""
                        {"orderId":"ORD-103","productName":"Laptop","quantity":1,"price":0}
                        """))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors.price").value("price must be greater than zero"));
    }

    @Test
    void nonNumericPriceReturns400() throws Exception {
        mockMvc.perform(post("/api/orders").contentType(MediaType.APPLICATION_JSON).content("""
                        {"orderId":"ORD-104","productName":"Laptop","quantity":1,"price":"abc"}
                        """))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.title").value("Malformed request"));
    }

    @Test
    void kafkaFailureReturns503() throws Exception {
        when(orderProducerService.publish(any()))
                .thenThrow(new OrderPublishingException("Kafka down", new RuntimeException()));

        mockMvc.perform(post("/api/orders").contentType(MediaType.APPLICATION_JSON).content("""
                        {"orderId":"ORD-105","productName":"Laptop","quantity":1,"price":10}
                        """))
                .andExpect(status().isServiceUnavailable())
                .andExpect(jsonPath("$.title").value("Messaging system unavailable"));
    }
}
