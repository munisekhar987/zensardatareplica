package com.zensar.data.replication.service;

import com.zensar.data.replication.model.TopicTableMapping;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import java.util.HashMap;
import java.util.Map;

/**
 * Service for managing mappings between Kafka topics and database tables.
 */
@Service
public class TopicMappingService {
    private static final Logger logger = LoggerFactory.getLogger(TopicMappingService.class);

    @Value("#{${cdc.topic.table-mappings}}")
    private Map<String, String> configuredMappings;

    private final Map<String, TopicTableMapping> topicMappings = new HashMap<>();

    @PostConstruct
    public void init() {
        if (configuredMappings != null) {
            for (Map.Entry<String, String> entry : configuredMappings.entrySet()) {
                String topic = entry.getKey();
                String tableName = entry.getValue();

                TopicTableMapping mapping = new TopicTableMapping(topic, tableName);
                topicMappings.put(topic.toLowerCase(), mapping);

                logger.info("Configured mapping: Topic '{}' -> Table '{}'", topic, tableName);
            }
        }
    }

    /**
     * Get the table mapping for a Kafka topic
     * @param topic the Kafka topic name
     * @return the table mapping or null if not found
     */
    public TopicTableMapping getMappingForTopic(String topic) {
        // Try an exact match first
        TopicTableMapping mapping = topicMappings.get(topic.toLowerCase());

        if (mapping != null) {
            return mapping;
        }

        // If no exact match, try a case-insensitive search
        for (Map.Entry<String, TopicTableMapping> entry : topicMappings.entrySet()) {
            if (entry.getKey().equalsIgnoreCase(topic)) {
                return entry.getValue();
            }
        }

        return null;
    }

    /**
     * Add a new topic-table mapping (useful for dynamic configuration)
     * @param topic the Kafka topic name
     * @param tableName the database table name
     */
    public void addMapping(String topic, String tableName) {
        TopicTableMapping mapping = new TopicTableMapping(topic, tableName);
        topicMappings.put(topic.toLowerCase(), mapping);
        logger.info("Added new mapping: Topic '{}' -> Table '{}'", topic, tableName);
    }
}