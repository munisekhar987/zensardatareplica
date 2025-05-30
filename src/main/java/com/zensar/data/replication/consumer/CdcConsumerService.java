package com.zensar.data.replication.consumer;

import com.zensar.data.replication.model.CdcEvent;
import com.zensar.data.replication.model.FieldTypeInfo;
import com.zensar.data.replication.model.TopicTableMapping;
import com.zensar.data.replication.service.PostgresSourceService;
import com.zensar.data.replication.service.SchemaParserService;
import com.zensar.data.replication.service.SqlExecutionService;
import com.zensar.data.replication.service.TopicMappingService;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Main service for consuming CDC events from Kafka and coordinating processing.
 * Uses Spring Boot's auto-configuration with manual acknowledgment enabled via properties.
 *
 * Required application.properties:
 * spring.kafka.consumer.enable-auto-commit=false
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
     * Consume CDC events from Kafka with manual acknowledgment.
     * Manual acknowledgment is enabled via application.properties:
     * spring.kafka.consumer.enable-auto-commit=false
     */
    @KafkaListener(topics = "#{'${cdc.kafka.topics}'.split(',')}")
    public void consume(
            ConsumerRecord<String, String> message,
            Acknowledgment acknowledgment,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset) {

        long startTime = System.currentTimeMillis();
        boolean processed = false;

        try {
            String eventMessage = message.value();
            logger.info("CDC Event Received from topic: {}, partition: {}, offset: {}",
                    topic, partition, offset);
            logger.debug("Event payload: {}", eventMessage);

            if (eventMessage == null || eventMessage.trim().isEmpty()) {
                logger.warn("Skipping empty event from topic: {}, partition: {}, offset: {}",
                        topic, partition, offset);
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
                    // Acknowledge even failed events to prevent infinite retry
                    acknowledgment.acknowledge();
                    return;
                }
            }

            // Create CDC event with the resolved table name
            CdcEvent cdcEvent = parseEvent(rootNode, topic, mapping.getTableName());
            if (cdcEvent == null) {
                logger.warn("Failed to parse CDC event from topic: {}", topic);
                // Acknowledge malformed events to prevent infinite retry
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

            // If we reach here, processing was successful
            processed = true;

            // Acknowledge successful processing
            acknowledgment.acknowledge();

            long processingTime = System.currentTimeMillis() - startTime;
            logger.info("Successfully processed CDC event for table: {} in {}ms (topic: {}, partition: {}, offset: {})",
                    cdcEvent.getTableName(), processingTime, topic, partition, offset);

        } catch (Exception e) {
            long processingTime = System.currentTimeMillis() - startTime;
            logger.error("Error processing Kafka message from topic: {}, partition: {}, offset: {} after {}ms: {}",
                    topic, partition, offset, processingTime, e.getMessage(), e);

            // For critical errors, we might want to acknowledge to prevent infinite retry
            // depending on your error handling strategy
            if (shouldAcknowledgeOnError(e)) {
                acknowledgment.acknowledge();
                logger.warn("Acknowledged failed message to prevent infinite retry");
            }
            // If not acknowledged, the message will be retried based on Kafka consumer configuration
        } finally {
            if (!processed) {
                logger.warn("CDC event processing completed unsuccessfully for topic: {}, partition: {}, offset: {}",
                        topic, partition, offset);
            }
        }
    }

    /**
     * Determine if we should acknowledge a message even when processing fails.
     * This prevents infinite retry loops for certain types of errors.
     */
    private boolean shouldAcknowledgeOnError(Exception exception) {
        // Acknowledge for parsing errors, configuration errors, etc.
        // These are unlikely to be resolved by retrying
        if (exception instanceof com.fasterxml.jackson.core.JsonParseException ||
                exception instanceof com.fasterxml.jackson.databind.JsonMappingException ||
                exception instanceof IllegalArgumentException) {
            return true;
        }

        // Don't acknowledge for SQL errors, connection issues, etc.
        // These might be resolved by retrying
        if (exception instanceof java.sql.SQLException ||
                exception instanceof java.sql.SQLTransientException ||
                exception instanceof java.net.ConnectException) {
            return false;
        }

        // For other exceptions, acknowledge after a certain number of attempts
        // This would require implementing retry counting logic
        return false;
    }

    // ... rest of the methods remain the same as in the previous implementation

    private void enhanceCdcEventWithPostgresData(CdcEvent cdcEvent) {
        try {
            Map<String, Object> completeRowData = postgresSourceService.fetchCompleteRowData(cdcEvent);

            if (completeRowData.isEmpty()) {
                logger.warn("No data fetched from PostgreSQL for table {}", cdcEvent.getTableName());
                return;
            }

            if ((cdcEvent.isInsert() || cdcEvent.isUpdate() || cdcEvent.isRead()) &&
                    cdcEvent.getAfterNode() != null) {
                enhanceJsonNode((ObjectNode) cdcEvent.getAfterNode(), completeRowData);
                logger.info("Enhanced afterNode with PostgreSQL data for {} operation", cdcEvent.getOperation());
            }

            if (cdcEvent.isDelete() && cdcEvent.getBeforeNode() != null && !completeRowData.isEmpty()) {
                enhanceJsonNode((ObjectNode) cdcEvent.getBeforeNode(), completeRowData);
                logger.info("Enhanced beforeNode with PostgreSQL data for DELETE operation");
            }
        } catch (Exception e) {
            logger.error("Error enhancing CDC event with PostgreSQL data: {}", e.getMessage(), e);
        }
    }

    private void enhanceJsonNode(ObjectNode node, Map<String, Object> postgresData) {
        Map<String, Object> data = new HashMap<>(postgresData);

        for (Map.Entry<String, Object> entry : data.entrySet()) {
            String columnName = entry.getKey();
            Object value = entry.getValue();

            if (value == null) {
                continue;
            }

            if (hasFieldCaseInsensitive(node, columnName)) {
                logger.debug("Column {} already exists in the node (case-insensitive match). Not overwriting.", columnName);
                continue;
            }

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
                java.sql.Timestamp ts = (java.sql.Timestamp) value;
                node.put(columnName, ts.getTime());
            } else if (value instanceof java.sql.Date) {
                java.sql.Date date = (java.sql.Date) value;
                node.put(columnName, date.getTime());
            } else {
                node.put(columnName, value.toString());
            }
        }

        logger.debug("Enhanced node with {} columns from PostgreSQL", data.size());
    }

    private boolean hasFieldCaseInsensitive(JsonNode node, String fieldName) {
        if (node.has(fieldName)) {
            return true;
        }

        Iterator<String> fieldNames = node.fieldNames();
        while (fieldNames.hasNext()) {
            String name = fieldNames.next();
            if (name.equalsIgnoreCase(fieldName)) {
                return true;
            }
        }

        return false;
    }

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
            throw e; // Re-throw to trigger retry logic
        }
    }
}