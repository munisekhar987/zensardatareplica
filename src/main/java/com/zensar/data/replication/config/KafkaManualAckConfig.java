package com.zensar.data.replication.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;

import java.util.HashMap;
import java.util.Map;

/**
 * Kafka configuration for manual acknowledgment with Google Cloud Managed Kafka support.
 * This configuration includes security settings for Google Cloud Managed Kafka.
 */
@Configuration
@EnableKafka
public class KafkaManualAckConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.consumer.group-id}")
    private String groupId;

    @Value("${spring.kafka.consumer.auto-offset-reset:earliest}")
    private String autoOffsetReset;

    // Google Cloud Managed Kafka Security Properties
    @Value("${spring.kafka.properties.security.protocol:SASL_SSL}")
    private String securityProtocol;

    @Value("${spring.kafka.properties.sasl.mechanism:OAUTHBEARER}")
    private String saslMechanism;

    @Value("${spring.kafka.properties.sasl.login.callback.handler.class:com.google.cloud.hosted.kafka.auth.GcpLoginCallbackHandler}")
    private String loginCallbackHandlerClass;

    @Value("${spring.kafka.properties.sasl.jaas.config:org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;}")
    private String saslJaasConfig;

    // Optional: Additional performance and reliability settings
    @Value("${spring.kafka.consumer.max-poll-records:500}")
    private int maxPollRecords;

    @Value("${spring.kafka.consumer.session-timeout-ms:30000}")
    private int sessionTimeoutMs;

    @Value("${spring.kafka.consumer.heartbeat-interval-ms:3000}")
    private int heartbeatIntervalMs;

    @Value("${spring.kafka.consumer.fetch-min-size:1}")
    private int fetchMinSize;

    @Value("${spring.kafka.consumer.fetch-max-wait:500}")
    private int fetchMaxWait;

    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        Map<String, Object> configProps = new HashMap<>();

        // Basic Kafka Consumer Configuration
        configProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        configProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
        configProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false); // Manual acknowledgment

        // Google Cloud Managed Kafka Security Configuration
        configProps.put("security.protocol", securityProtocol);
        configProps.put("sasl.mechanism", saslMechanism);
        configProps.put("sasl.login.callback.handler.class", loginCallbackHandlerClass);
        configProps.put("sasl.jaas.config", saslJaasConfig);

        // Performance and reliability settings
        configProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);
        configProps.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, sessionTimeoutMs);
        configProps.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, heartbeatIntervalMs);
        configProps.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, fetchMinSize);
        configProps.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, fetchMaxWait);

        // Additional settings for production reliability
        configProps.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300000); // 5 minutes
        configProps.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 60000); // 1 minute
        configProps.put(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG, 1000); // 1 second
        configProps.put(ConsumerConfig.RECONNECT_BACKOFF_MS_CONFIG, 1000); // 1 second
        configProps.put(ConsumerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG, 10000); // 10 seconds

        // For exactly-once semantics (if needed)
        configProps.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        return new DefaultKafkaConsumerFactory<>(configProps);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(consumerFactory());

        // CRITICAL: Set acknowledgment mode to MANUAL_IMMEDIATE for manual acknowledgment
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);

        // Error handling configuration
        factory.setCommonErrorHandler(new org.springframework.kafka.listener.DefaultErrorHandler(
                (exception, data) -> {
                    // Log the error - the consumer will handle acknowledgment decision
                    System.err.println("Error in Kafka listener: " + exception.toString());
                },
                new org.springframework.util.backoff.FixedBackOff(1000L, 3L) // 1 second delay, 3 retries
        ));

        // Concurrency settings - adjust based on your needs and topic partitions
        factory.setConcurrency(1); // Start with 1 for data consistency, increase if needed

        // Container properties for better monitoring and debugging
        factory.getContainerProperties().setLogContainerConfig(true);
        factory.getContainerProperties().setMissingTopicsFatal(false);

        return factory;
    }
}