package com.example.orderconsumer.config;

import com.example.orderconsumer.service.InvalidOrderException;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.DelegatingByTypeSerializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Error handling for the {@code @KafkaListener}.
 *
 * <p>Deserialization settings live in application.yml. This class defines what happens when
 * processing a record fails:
 * <ol>
 *   <li>Retry the record up to 2 more times, 1 second apart (transient failures, e.g. a DB blip).</li>
 *   <li>If it still fails, publish it to the dead-letter topic {@code order-events.DLT}
 *       and move on, so one "poison" message cannot block the partition forever.</li>
 *   <li>Some exceptions (bad data) are never retried - they go to the DLT immediately.</li>
 * </ol>
 * Spring Boot automatically applies a {@code CommonErrorHandler} bean to the default listener
 * container factory, so nothing else needs to be wired.
 */
@Configuration
public class KafkaConsumerConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerConfig.class);

    /**
     * The consumer owns its dead-letter topic. It needs the same number of partitions as the
     * source topic, because the recoverer writes a failed record to the same partition number.
     */
    @Bean
    public NewTopic orderEventsDeadLetterTopic(@Value("${app.kafka.topic}") String topic,
                                               @Value("${app.kafka.partitions:3}") int partitions) {
        return TopicBuilder.name(topic + ".DLT").partitions(partitions).replicas(1).build();
    }

    @Bean
    public DefaultErrorHandler kafkaErrorHandler(KafkaTemplate<Object, Object> deadLetterKafkaTemplate) {
        // Failed record → "<original-topic>.DLT", same partition as the original record.
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(deadLetterKafkaTemplate,
                (record, ex) -> new TopicPartition(record.topic() + ".DLT", record.partition()));

        DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, new FixedBackOff(1000L, 2L));
        // Deserialization errors are already non-retryable by default; add our own "bad data" exception.
        errorHandler.addNotRetryableExceptions(InvalidOrderException.class, IllegalArgumentException.class);
        errorHandler.setRetryListeners((record, ex, attempt) ->
                log.warn("Retry attempt {} for record key={} offset={}: {}",
                        attempt, record.key(), record.offset(), ex.getMessage()));
        return errorHandler;
    }

    /**
     * KafkaTemplate used only to write failed records to the DLT.
     * A record that failed deserialization is still raw {@code byte[]}; a record that failed
     * processing is an {@code OrderEvent}. The delegating serializer picks the right one by type.
     */
    @Bean
    public KafkaTemplate<Object, Object> deadLetterKafkaTemplate(ProducerFactory<?, ?> bootProducerFactory) {
        // Reuse Spring Boot's producer settings (bootstrap servers, acks...) and only swap serializers.
        Map<String, Object> props = new HashMap<>(bootProducerFactory.getConfigurationProperties());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        JsonSerializer<Object> jsonSerializer = new JsonSerializer<>();
        jsonSerializer.setAddTypeInfo(false);

        Map<Class<?>, Serializer<?>> delegates = new LinkedHashMap<>();
        delegates.put(byte[].class, new ByteArraySerializer());
        delegates.put(Object.class, jsonSerializer);

        DefaultKafkaProducerFactory<Object, Object> factory = new DefaultKafkaProducerFactory<>(props);
        factory.setValueSerializer(new DelegatingByTypeSerializer(delegates, true));
        return new KafkaTemplate<>(factory);
    }
}
