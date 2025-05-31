package com.zensar.data.replication.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;

/**
 * MINIMAL Kafka configuration that only sets manual acknowledgment.
 * All other properties come from application.properties via Spring Boot auto-configuration.
 * This is the CLEANEST approach.
 */
@Configuration
@EnableKafka
public class KafkaManualAckConfig {

    /**
     * Override only the listener container factory to enable manual acknowledgment.
     * Spring Boot will handle all other configuration from application.properties.
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(
            ConsumerFactory<String, String> consumerFactory) {

        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        // Use Spring Boot's auto-configured ConsumerFactory
        factory.setConsumerFactory(consumerFactory);

        // Only override the acknowledgment mode
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);

        System.out.println("=== Kafka Manual Acknowledgment Enabled ===");
        System.out.println("Acknowledgment Mode: " + factory.getContainerProperties().getAckMode());

        return factory;
    }
}

/*
 * ADVANTAGES OF THIS APPROACH:
 *
 * 1. Spring Boot handles ALL configuration from application.properties
 * 2. No risk of overriding or missing properties
 * 3. All your security properties from application.properties will be used
 * 4. Much simpler and less error-prone
 * 5. Easier to debug - just check application.properties
 *
 * YOUR APPLICATION.PROPERTIES SHOULD CONTAIN:
 *
 * spring.kafka.bootstrap-servers=${KAFKA_BOOTSTRAP_SERVERS}
 * spring.kafka.consumer.group-id=${KAFKA_GROUP_ID}
 * spring.kafka.consumer.enable-auto-commit=false
 * spring.kafka.properties.security.protocol=${KAFKA_SECURITY_PROTOCOL}
 * spring.kafka.properties.sasl.mechanism=${KAFKA_SASL_MECHANISM}
 * spring.kafka.properties.sasl.login.callback.handler.class=${KAFKA_SASL_LOGIN_CALLBACK_HANDLER_CLASS}
 * spring.kafka.properties.sasl.jaas.config=${KAFKA_SASL_JAAS_CONFIG}
 *
 * This way:
 * - Spring Boot reads and applies ALL properties automatically
 * - We only override what we need (manual acknowledgment)
 * - No more "default values" issues
 */