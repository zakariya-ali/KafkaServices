package com.example.orderconsumer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Entry point of the Order Consumer microservice.
 *
 * <p>Spring Boot sees spring-kafka on the classpath, enables {@code @KafkaListener} processing
 * and creates a listener container for every {@code @KafkaListener} method it finds.
 * Those containers start polling Kafka as soon as the application context is ready.
 */
@SpringBootApplication
public class OrderConsumerApplication {

    public static void main(String[] args) {
        SpringApplication.run(OrderConsumerApplication.class, args);
    }
}
