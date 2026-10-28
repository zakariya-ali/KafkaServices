package com.example.orderproducer.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Kafka configuration that cannot be expressed in application.yml.
 *
 * <p>The KafkaTemplate itself is auto-configured by Spring Boot from {@code spring.kafka.producer.*}.
 * Here we only declare the topic: Spring Boot's {@code KafkaAdmin} finds every {@link NewTopic}
 * bean at startup and creates the topic if it does not exist (it never deletes or shrinks one).
 */
@Configuration
public class KafkaProducerConfig {

    @Bean
    public NewTopic orderEventsTopic(@Value("${app.kafka.topic}") String topic,
                                     @Value("${app.kafka.partitions:3}") int partitions) {
        return TopicBuilder.name(topic)
                .partitions(partitions)   // 3 partitions → up to 3 consumers can share the work
                .replicas(1)              // single broker in dev; use 3 in production
                .build();
    }
}
