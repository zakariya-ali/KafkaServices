package com.example.orderproducer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Entry point of the Order Producer microservice.
 *
 * <p>{@code @SpringBootApplication} = {@code @Configuration} + {@code @EnableAutoConfiguration}
 * + {@code @ComponentScan}. Spring scans this package (and sub-packages) for
 * {@code @RestController}, {@code @Service}, {@code @Configuration} classes and creates beans
 * from them. Auto-configuration sees spring-kafka on the classpath and builds a
 * {@code KafkaTemplate} from the {@code spring.kafka.*} properties in application.yml.
 */
@SpringBootApplication
public class OrderProducerApplication {

    public static void main(String[] args) {
        SpringApplication.run(OrderProducerApplication.class, args);
    }
}
