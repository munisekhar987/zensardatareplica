package com.zensar.data.replication.consumer;

import com.zensar.data.replication.model.CdcEvent;
import com.zensar.data.replication.model.FieldTypeInfo;
import com.zensar.data.replication.model.TopicTableMapping;
import com.zensar.data.replication.service.SchemaParserService;
import com.zensar.data.replication.service.SqlExecutionService;
import com.zensar.data.replication.service.TopicMappingService;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.Map;

/**
 * Main service for consuming CDC events from Kafka and coordinating processing.
 * Enhanced to dynamically handle multiple topics and tables using configurable mappings.
 */
@Service
public class CdcConsumerService {
    private static final Logger logger = LoggerFactory.getLogger(CdcConsumerService.class);

    private final ObjectMapper objectMapper;
    private final SchemaParserService schemaParser;
    private final SqlExecutionService sqlExecutionService;
    private final TopicMappingService topicMappingService;

    @Autowired
    public CdcConsumerService(
            ObjectMapper objectMapper,
            SchemaParserService schemaParser,
            SqlExecutionService sqlExecutionService,
            TopicMappingService topicMappingService) {
        this.objectMapper = objectMapper;
        this.schemaParser = schemaParser;
        this.sqlExecutionService = sqlExecutionService;
        this.topicMappingService = topicMappingService;
    }

    /**
     * Consume CDC events from Kafka, using a comma-separated list of topics.
     */
    @KafkaListener(topics = "#{'${cdc.kafka.topics}'.split(',')}")
    public void consume(ConsumerRecord<String, String> message) {
        try {
            String topic = message.topic();
            String eventMessage = message.value();
            logger.info("CDC Event Received from topic {}", topic);
            logger.debug("Event payload: {}", eventMessage);

            if (eventMessage == null) {
                logger.warn("Skipping as empty event from topic: {}", topic);
                return;
            }

            // Parse the event message
            JsonNode rootNode = objectMapper.readTree(eventMessage);

            // Get table mapping for this topic
            TopicTableMapping mapping = topicMappingService.getMappingForTopic(topic);
            if (mapping == null) {
                logger.warn("No table mapping configured for topic: {}", topic);

                // Try to extract table name from source metadata as fallback
                String tableName = extractTableNameFromEventSource(rootNode);
                if (tableName != null) {
                    logger.info("Using table name extracted from source metadata: {}", tableName);
                    mapping = new TopicTableMapping(topic, tableName);
                } else {
                    logger.error("Cannot process event - unable to determine target table for topic: {}", topic);
                    return;
                }
            }

            // Create CDC event with the resolved table name
            CdcEvent cdcEvent = parseEvent(rootNode, topic, mapping.getTableName());
            if (cdcEvent == null) {
                logger.warn("Failed to parse CDC event from topic: {}", topic);
                return;
            }

            // Extract schema information
            Map<String, FieldTypeInfo> fieldTypeMap = schemaParser.buildFieldTypeMap(rootNode.get("schema"));

            // Process the event based on operation type
            processEvent(cdcEvent, fieldTypeMap);

        } catch (Exception e) {
            logger.error("Error processing Kafka message: {}", e.getMessage(), e);
        }
    }

    /**
     * Extract table name from Debezium source metadata if available.
     */
    private String extractTableNameFromEventSource(JsonNode rootNode) {
        try {
            if (rootNode != null && rootNode.has("payload") &&
                    rootNode.get("payload").has("source") &&
                    rootNode.get("payload").get("source").has("table")) {

                String tableName = rootNode.get("payload").get("source").get("table").asText();
                if (tableName != null && !tableName.isEmpty()) {
                    return tableName.toUpperCase();
                }
            }
        } catch (Exception e) {
            logger.warn("Error extracting table name from source metadata: {}", e.getMessage());
        }
        return null;
    }

    /**
     * Parse CDC event from JSON.
     */
    private CdcEvent parseEvent(JsonNode rootNode, String topic, String tableName) {
        if (rootNode == null || !rootNode.has("payload")) {
            logger.warn("Invalid CDC event format - missing payload");
            return null;
        }

        JsonNode payloadNode = rootNode.get("payload");
        String operation = payloadNode.has("op") ? payloadNode.get("op").asText() : "";
        JsonNode beforeNode = payloadNode.has("before") ? payloadNode.get("before") : null;
        JsonNode afterNode = payloadNode.has("after") ? payloadNode.get("after") : null;
        JsonNode sourceNode = payloadNode.has("source") ? payloadNode.get("source") : null;

        // Double-check table name from source if available
        if (sourceNode != null && sourceNode.has("table")) {
            String sourceTable = sourceNode.get("table").asText();
            if (sourceTable != null && !sourceTable.isEmpty()) {
                if (!sourceTable.equalsIgnoreCase(tableName)) {
                    logger.info("Table name from mapping '{}' differs from source metadata '{}', using source metadata",
                            tableName, sourceTable);
                    tableName = sourceTable.toUpperCase();
                }
            }
        }

        logger.info("Parsed CDC event with operation: {} for table: {}", operation, tableName);
        return new CdcEvent(topic, tableName, operation, beforeNode, afterNode, sourceNode);
    }

    /**
     * Process CDC event based on operation type.
     * Handles insert, update, delete, and read (snapshot) operations.
     */
    private void processEvent(CdcEvent event, Map<String, FieldTypeInfo> fieldTypeMap) {
        try {
            if (event.isInsert()) {
                if (event.getAfterNode() != null && !event.getAfterNode().isNull()) {
                    logger.info("Processing INSERT operation for table: {}", event.getTableName());
                    sqlExecutionService.executeStandardInsert(event, fieldTypeMap);
                }
            } else if (event.isUpdate()) {
                if (event.getAfterNode() != null && !event.getAfterNode().isNull()) {
                    logger.info("Processing UPDATE operation for table: {}", event.getTableName());
                    sqlExecutionService.executeUpdate(event, fieldTypeMap);
                }
            } else if (event.isDelete()) {
                if (event.getBeforeNode() != null && !event.getBeforeNode().isNull()) {
                    logger.info("Processing DELETE operation for table: {}", event.getTableName());
                    sqlExecutionService.executeDelete(event, fieldTypeMap);
                }
            } else if (event.isRead()) {
                if (event.getAfterNode() != null && !event.getAfterNode().isNull()) {
                    logger.info("Processing READ operation (snapshot) for table: {}", event.getTableName());
                    sqlExecutionService.executeStandardInsert(event, fieldTypeMap);
                }
            } else {
                logger.warn("Skipping operation type: {}", event.getOperation());
            }
        } catch (Exception e) {
            logger.error("Error processing CDC event: {}", e.getMessage(), e);
        }
    }
}