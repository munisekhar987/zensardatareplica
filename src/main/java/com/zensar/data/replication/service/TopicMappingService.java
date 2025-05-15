package com.zensar.data.replication.service;

import com.zensar.data.replication.model.TopicTableMapping;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Service for managing mappings between Kafka topics and database tables.
 */
@Service
public class TopicMappingService {
    private static final Logger logger = LoggerFactory.getLogger(TopicMappingService.class);

    // Map to store topic->table mappings
    private final Map<String, TopicTableMapping> topicMappings = new HashMap<>();

    // Configuration for topic-table mappings
    @Value("#{${cdc.topic.table-mappings}}")
    private Map<String, String> configuredMappings;

    @PostConstruct
    public void initialize() {
        if (configuredMappings == null || configuredMappings.isEmpty()) {
            logger.warn("No topic-table mappings configured");
            return;
        }

        // Load mappings from configuration
        logger.info("Initializing topic to table mappings...");
        for (Map.Entry<String, String> entry : configuredMappings.entrySet()) {
            String topic = entry.getKey();
            String tableName = entry.getValue();

            // Store mapping
            TopicTableMapping mapping = new TopicTableMapping(topic, tableName);
            topicMappings.put(topic, mapping);

            logger.info("Added mapping: {} -> {}", topic, tableName);
        }

        logger.info("Topic mapping initialization complete, loaded {} mappings", topicMappings.size());
    }

    /**
     * Get the table mapping for a specific topic.
     *
     * @param topic Kafka topic name
     * @return TopicTableMapping or null if not found
     */
    public TopicTableMapping getMappingForTopic(String topic) {
        return topicMappings.get(topic);
    }

    /**
     * Get all configured topic-table mappings.
     *
     * @return List of all topic-table mappings
     */
    public List<TopicTableMapping> getAllMappings() {
        return new ArrayList<>(topicMappings.values());
    }

    /**
     * Add or update a mapping at runtime.
     *
     * @param topic Kafka topic name
     * @param tableName Database table name
     * @return The created/updated mapping
     */
    public TopicTableMapping addMapping(String topic, String tableName) {
        TopicTableMapping mapping = new TopicTableMapping(topic, tableName);
        topicMappings.put(topic, mapping);
        logger.info("Added/updated mapping: {} -> {}", topic, tableName);
        return mapping;
    }

    /**
     * Remove a mapping.
     *
     * @param topic Kafka topic name
     * @return True if mapping was removed, false if it didn't exist
     */
    public boolean removeMapping(String topic) {
        TopicTableMapping removed = topicMappings.remove(topic);
        if (removed != null) {
            logger.info("Removed mapping for topic: {}", topic);
            return true;
        }
        return false;
    }
}