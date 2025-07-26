package com.zensar.data.replication.consumer;

import com.zensar.data.replication.model.CdcEvent;
import com.zensar.data.replication.model.FieldTypeInfo;
import com.zensar.data.replication.model.TopicTableMapping;
import com.zensar.data.replication.service.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Main service for consuming CDC events from Kafka and coordinating processing.
 * Enhanced to dynamically handle multiple topics and tables using configurable mappings.
 * Added support for UDT column handling by fetching complete rows from PostgreSQL.
 * Enhanced with case-insensitive field handling and column-specific UDT conversion.
 */
@Service
public class CdcConsumerService {
    private static final Logger logger = LoggerFactory.getLogger(CdcConsumerService.class);

    private final ObjectMapper objectMapper;
    private final SchemaParserService schemaParser;
    private final SqlExecutionService sqlExecutionService;
    private final TopicMappingService topicMappingService;
    private final PostgresSourceService postgresSourceService;
    @Autowired
    private KafkaMetrics metrics;

    @Autowired
    public CdcConsumerService(
            ObjectMapper objectMapper,
            SchemaParserService schemaParser,
            SqlExecutionService sqlExecutionService,
            TopicMappingService topicMappingService,
            PostgresSourceService postgresSourceService) {
        this.objectMapper = objectMapper;
        this.schemaParser = schemaParser;
        this.sqlExecutionService = sqlExecutionService;
        this.topicMappingService = topicMappingService;
        this.postgresSourceService = postgresSourceService;
    }

    /**
     * Consume CDC events from Kafka, using a comma-separated list of topics.
     */
    @KafkaListener(topics = "#{'${cdc.kafka.topics}'.split(',')}")
    public void consume(ConsumerRecord<String, String> message, Acknowledgment acknowledgment) {
        long startTime = System.currentTimeMillis();
        int maxRetries = 1;
        int retryCount = 0;

        while (retryCount <= maxRetries) {
            try {
                String topic = message.topic();
                String eventMessage = message.value();

                if (retryCount > 0) {
                    logger.warn("Retrying message processing for topic {} (attempt {}/{})",
                            topic, retryCount + 1, maxRetries + 1);
                    metrics.recordRetry();
                }

                logger.info("CDC Event Received from topic {}", topic);
                logger.debug("Event payload: {}", eventMessage);

                if (eventMessage == null) {
                    logger.warn("Skipping as empty event from topic: {}", topic);
                    acknowledgment.acknowledge();
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
                        acknowledgment.acknowledge();
                        return;
                    }
                }

                // Create CDC event with the resolved table name
                CdcEvent cdcEvent = parseEvent(rootNode, topic, mapping.getTableName());
                if (cdcEvent == null) {
                    logger.warn("Failed to parse CDC event from topic: {}", topic);
                    acknowledgment.acknowledge();
                    return;
                }

                // Extract schema information
                Map<String, FieldTypeInfo> fieldTypeMap = schemaParser.buildFieldTypeMap(rootNode.get("schema"));

                // Check if this table has UDT columns that need to be fetched from PostgreSQL
                if (postgresSourceService.isUdtTable(cdcEvent.getTableName())) {
                    logger.info("Table {} has UDT columns. Fetching complete row data from PostgreSQL.",
                            cdcEvent.getTableName());

                    // Enhance the CDC event with complete data from PostgreSQL
                    enhanceCdcEventWithPostgresData(cdcEvent);
                }

                // Process the event based on operation type
                processEvent(cdcEvent, fieldTypeMap);

                // Record successful processing with timing
                long processingTime = System.currentTimeMillis() - startTime;
                metrics.recordProcessing(processingTime);

                // Only acknowledge after successful processing
                logger.debug("Successfully processed event from topic: {}. Acknowledging message.", topic);
                acknowledgment.acknowledge();

                return; // Success - exit retry loop

            } catch (Exception e) {
                retryCount++;

                if (retryCount <= maxRetries) {
                    logger.warn("Error processing Kafka message from topic {} (attempt {}/{}): {}. Retrying...",
                            message.topic(), retryCount, maxRetries + 1, e.getMessage());

                    try {
                        Thread.sleep(1000); // 1 second delay
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        logger.error("Retry interrupted for topic: {}", message.topic());
                        break;
                    }
                } else {
                    // Record failure and stop consumer
                    metrics.recordFailure();
                    metrics.stop();

                    logger.error("CRITICAL ERROR: Failed to process Kafka message from topic {} after {} attempts: {}",
                            message.topic(), maxRetries + 1, e.getMessage(), e);
                    logger.error("STOPPING CONSUMER: Cannot proceed with further message processing.");

                    throw new RuntimeException("Critical error processing Kafka message from topic: " +
                            message.topic() + " after " + (maxRetries + 1) + " attempts. Consumer stopped.", e);
                }
            }
        }
    }

    /**
     * Enhance the CDC event with complete data from PostgreSQL, including UDT columns.
     * This method enriches the afterNode and beforeNode with data from PostgreSQL.
     * Enhanced with case-insensitive field name handling and column-specific UDT conversion.
     */
    private void enhanceCdcEventWithPostgresData(CdcEvent cdcEvent) {
        try {
            // Fetch the complete row data from PostgreSQL
            Map<String, Object> completeRowData = postgresSourceService.fetchCompleteRowData(cdcEvent);

            if (completeRowData.isEmpty()) {
                logger.warn("No data fetched from PostgreSQL for table {}", cdcEvent.getTableName());
                return;
            }

            // Enhance afterNode for INSERT and UPDATE operations
            if ((cdcEvent.isInsert() || cdcEvent.isUpdate() || cdcEvent.isRead()) &&
                    cdcEvent.getAfterNode() != null) {
                enhanceJsonNode((ObjectNode) cdcEvent.getAfterNode(), completeRowData);
                logger.info("Enhanced afterNode with PostgreSQL data for {} operation", cdcEvent.getOperation());
            }

            // Enhance beforeNode for DELETE operations
            // This is optional since the row might already be deleted in PostgreSQL
            if (cdcEvent.isDelete() && cdcEvent.getBeforeNode() != null && !completeRowData.isEmpty()) {
                enhanceJsonNode((ObjectNode) cdcEvent.getBeforeNode(), completeRowData);
                logger.info("Enhanced beforeNode with PostgreSQL data for DELETE operation");
            }
        } catch (Exception e) {
            logger.error("Error enhancing CDC event with PostgreSQL data: {}", e.getMessage(), e);
        }
    }

    /**
     * Enhance a JSON node with data from PostgreSQL
     * Enhanced with case-insensitive field name handling and column-specific UDT conversion.
     */
    private void enhanceJsonNode(ObjectNode node, Map<String, Object> postgresData) {
        // Create a deep copy to avoid concurrent modification
        Map<String, Object> data = new HashMap<>(postgresData);

        // Add all columns from PostgreSQL data to the node
        for (Map.Entry<String, Object> entry : data.entrySet()) {
            String columnName = entry.getKey();
            Object value = entry.getValue();

            // Skip null values
            if (value == null) {
                continue;
            }

            // Skip if the column already exists in the node (case-insensitive check)
            if (hasFieldCaseInsensitive(node, columnName)) {
                logger.debug("Column {} already exists in the node (case-insensitive match). Not overwriting.", columnName);
                continue;
            }

            // **KEY ADDITION**: Handle UDT values that were already converted by PostgresSourceService
            if (value instanceof String && isUdtConstructorString((String) value)) {
                // This is already a converted UDT constructor string from PostgreSQL
                logger.debug("Adding UDT constructor value for column {}: {}", columnName, value);
                node.put(columnName, (String) value);
                continue;
            }

            // Convert Java types to appropriate JsonNode types
            if (value instanceof String) {
                node.put(columnName, (String) value);
            } else if (value instanceof Integer) {
                node.put(columnName, (Integer) value);
            } else if (value instanceof Long) {
                node.put(columnName, (Long) value);
            } else if (value instanceof Double) {
                node.put(columnName, (Double) value);
            } else if (value instanceof Boolean) {
                node.put(columnName, (Boolean) value);
            } else if (value instanceof java.sql.Timestamp) {
                // Convert to milliseconds since epoch
                java.sql.Timestamp ts = (java.sql.Timestamp) value;
                node.put(columnName, ts.getTime());
            } else if (value instanceof java.sql.Date) {
                // Convert to milliseconds since epoch
                java.sql.Date date = (java.sql.Date) value;
                node.put(columnName, date.getTime());
            } else {
                // For other types, convert to string
                node.put(columnName, value.toString());
            }
        }

        logger.debug("Enhanced node with {} columns from PostgreSQL", data.size());
    }

    /**
     * Check if a string appears to be a UDT constructor string.
     */
    private boolean isUdtConstructorString(String value) {
        if (value == null || value.trim().isEmpty()) {
            return false;
        }

        return value.startsWith("TRANSP.HANDOFF_ROUTING_ROUTE_NO(") ||
                value.startsWith("TRANSP.HANDOFF_ROADNET_ROUTE_NO(") ||
                value.startsWith("HANDOFF_ROUTING_ROUTE_NO(") ||
                value.startsWith("HANDOFF_ROADNET_ROUTE_NO(");
    }

    /**
     * Check if a field exists in a JsonNode with case-insensitive matching
     *
     * @param node The JsonNode to check
     * @param fieldName The field name to look for (case-insensitive)
     * @return true if the field exists, false otherwise
     */
    private boolean hasFieldCaseInsensitive(JsonNode node, String fieldName) {
        // Case 1: Exact match
        if (node.has(fieldName)) {
            return true;
        }

        // Case 2: Case-insensitive match
        Iterator<String> fieldNames = node.fieldNames();
        while (fieldNames.hasNext()) {
            String name = fieldNames.next();
            if (name.equalsIgnoreCase(fieldName)) {
                return true;
            }
        }

        return false;
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