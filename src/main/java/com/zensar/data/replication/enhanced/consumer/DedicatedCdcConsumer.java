package com.zensar.data.replication.enhanced.consumer;

import com.zensar.data.replication.enhanced.pool.PrimaryKeyHasherService;
import com.zensar.data.replication.model.CdcEvent;
import com.zensar.data.replication.model.FieldTypeInfo;
import com.zensar.data.replication.model.TopicTableMapping;
import com.zensar.data.replication.service.PostgresSourceService;
import com.zensar.data.replication.service.SchemaParserService;
import com.zensar.data.replication.service.TopicMappingService;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Dedicated CDC Consumer for single topic processing with enhanced architecture:
 * Consumer(1) → Hashers(4) → Workers(16) → Connection Pool
 */
@Service
public class DedicatedCdcConsumer {
    private static final Logger logger = LoggerFactory.getLogger(DedicatedCdcConsumer.class);

    @Value("${cdc.enhanced.topic}")
    private String dedicatedTopic;

    @Value("${cdc.enhanced.consumer.group-id}")
    private String consumerGroupId;

    @Value("${cdc.enhanced.consumer.batch-size:2000}")
    private int batchSize;

    @Value("${cdc.enhanced.consumer.poll-timeout-ms:1000}")
    private long pollTimeoutMs;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private SchemaParserService schemaParser;

    @Autowired
    private TopicMappingService topicMappingService;

    @Autowired
    private PostgresSourceService postgresSourceService;

    @Autowired
    private PrimaryKeyHasherService hasherService;

    @Autowired
    private ConsumerFactory<String, String> consumerFactory;

    private KafkaConsumer<String, String> consumer;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private Thread consumerThread;

    // Metrics
    private final AtomicLong totalBatchesProcessed = new AtomicLong(0);
    private final AtomicLong totalEventsProcessed = new AtomicLong(0);
    private final AtomicLong totalFailedBatches = new AtomicLong(0);

    @PostConstruct
    public void init() {
        logger.info("Initializing Dedicated CDC Consumer");
        logger.info("Topic: {}, Consumer Group: {}, Batch Size: {}",
                dedicatedTopic, consumerGroupId, batchSize);

        startConsumer();
    }

    private void startConsumer() {
        if (running.compareAndSet(false, true)) {
            Properties props = new Properties();
            props.putAll(consumerFactory.getConfigurationProperties());
            props.put("group.id", consumerGroupId);

            consumer = new KafkaConsumer<>(props);
            consumer.subscribe(Collections.singletonList(dedicatedTopic));

            consumerThread = new Thread(this::consumeLoop, "Enhanced-CDC-Consumer");
            consumerThread.setDaemon(false);
            consumerThread.start();

            logger.info("Dedicated CDC Consumer started for topic: {}", dedicatedTopic);
        }
    }

    private void consumeLoop() {
        logger.info("Starting dedicated CDC consumer loop for topic: {}", dedicatedTopic);

        List<EnhancedCdcEvent> currentBatch = new ArrayList<>();
        Map<TopicPartition, Long> batchOffsets = new HashMap<>();

        while (running.get() && !Thread.currentThread().isInterrupted()) {
            try {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(pollTimeoutMs));

                if (records.isEmpty()) {
                    if (!currentBatch.isEmpty()) {
                        logger.debug("Processing partial batch of {} events due to timeout", currentBatch.size());
                        boolean success = processBatch(currentBatch, batchOffsets);
                        if (success) {
                            commitOffsets(batchOffsets);
                        }
                        currentBatch.clear();
                        batchOffsets.clear();
                    }
                    continue;
                }

                for (ConsumerRecord<String, String> record : records) {
                    try {
                        EnhancedCdcEvent event = parseRecord(record);
                        if (event != null) {
                            currentBatch.add(event);
                            TopicPartition tp = new TopicPartition(record.topic(), record.partition());
                            batchOffsets.put(tp, record.offset() + 1);
                        }
                    } catch (Exception e) {
                        logger.error("Error parsing record from offset {}", record.offset(), e);
                    }
                }

                if (currentBatch.size() >= batchSize) {
                    logger.debug("Processing full batch of {} events", currentBatch.size());
                    boolean success = processBatch(currentBatch, batchOffsets);
                    if (success) {
                        commitOffsets(batchOffsets);
                    } else {
                        logger.error("Batch processing failed, will not commit offsets");
                        totalFailedBatches.incrementAndGet();
                    }
                    currentBatch.clear();
                    batchOffsets.clear();
                }

            } catch (Exception e) {
                logger.error("Error in consumer loop", e);
                currentBatch.clear();
                batchOffsets.clear();

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }

        if (!currentBatch.isEmpty()) {
            logger.info("Processing final batch of {} events", currentBatch.size());
            try {
                boolean success = processBatch(currentBatch, batchOffsets);
                if (success) {
                    commitOffsets(batchOffsets);
                }
            } catch (Exception e) {
                logger.error("Error processing final batch", e);
            }
        }

        logger.info("Dedicated CDC consumer loop stopped");
    }

    private EnhancedCdcEvent parseRecord(ConsumerRecord<String, String> record) {
        try {
            String eventMessage = record.value();
            if (eventMessage == null || eventMessage.trim().isEmpty()) {
                return null;
            }

            JsonNode rootNode = objectMapper.readTree(eventMessage);

            TopicTableMapping mapping = topicMappingService.getMappingForTopic(dedicatedTopic);
            if (mapping == null) {
                String tableName = extractTableNameFromEventSource(rootNode);
                if (tableName != null) {
                    mapping = new TopicTableMapping(dedicatedTopic, tableName);
                } else {
                    logger.warn("Cannot determine target table for event");
                    return null;
                }
            }

            CdcEvent cdcEvent = parseEvent(rootNode, dedicatedTopic, mapping.getTableName());
            if (cdcEvent == null) {
                return null;
            }

            Map<String, FieldTypeInfo> fieldTypeMap = schemaParser.buildFieldTypeMap(rootNode.get("schema"));

            if (postgresSourceService.isUdtTable(cdcEvent.getTableName())) {
                enhanceCdcEventWithPostgresData(cdcEvent);
            }

            return new EnhancedCdcEvent(cdcEvent, fieldTypeMap);

        } catch (Exception e) {
            logger.error("Error parsing record", e);
            return null;
        }
    }

    private boolean processBatch(List<EnhancedCdcEvent> batch, Map<TopicPartition, Long> offsets) {
        if (batch.isEmpty()) {
            return true;
        }

        logger.info("Processing batch of {} events", batch.size());
        totalBatchesProcessed.incrementAndGet();

        try {
            boolean success = hasherService.submitBatch(batch);

            if (success) {
                totalEventsProcessed.addAndGet(batch.size());
                logger.debug("Successfully submitted batch to hasher service");
                return true;
            } else {
                logger.error("Failed to submit batch to hasher service");
                return false;
            }

        } catch (Exception e) {
            logger.error("Error processing batch", e);
            return false;
        }
    }

    private void commitOffsets(Map<TopicPartition, Long> offsets) {
        if (offsets.isEmpty()) {
            return;
        }

        try {
            Map<TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata> offsetsToCommit = new HashMap<>();
            for (Map.Entry<TopicPartition, Long> entry : offsets.entrySet()) {
                offsetsToCommit.put(entry.getKey(),
                        new org.apache.kafka.clients.consumer.OffsetAndMetadata(entry.getValue()));
            }

            consumer.commitSync(offsetsToCommit);
            logger.debug("Successfully committed offsets for {} partitions", offsets.size());

        } catch (Exception e) {
            logger.error("Error committing offsets", e);
            throw new RuntimeException("Failed to commit offsets", e);
        }
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
            logger.debug("Error extracting table name", e);
        }
        return null;
    }

    private CdcEvent parseEvent(JsonNode rootNode, String topic, String tableName) {
        if (rootNode == null || !rootNode.has("payload")) {
            return null;
        }

        JsonNode payloadNode = rootNode.get("payload");
        String operation = payloadNode.has("op") ? payloadNode.get("op").asText() : "";
        JsonNode beforeNode = payloadNode.has("before") ? payloadNode.get("before") : null;
        JsonNode afterNode = payloadNode.has("after") ? payloadNode.get("after") : null;
        JsonNode sourceNode = payloadNode.has("source") ? payloadNode.get("source") : null;

        return new CdcEvent(topic, tableName, operation, beforeNode, afterNode, sourceNode);
    }

    private void enhanceCdcEventWithPostgresData(CdcEvent cdcEvent) {
        try {
            Map<String, Object> completeRowData = postgresSourceService.fetchCompleteRowData(cdcEvent);

            if (!completeRowData.isEmpty()) {
                if ((cdcEvent.isInsert() || cdcEvent.isUpdate() || cdcEvent.isRead()) &&
                        cdcEvent.getAfterNode() != null) {
                    enhanceJsonNode((ObjectNode) cdcEvent.getAfterNode(), completeRowData);
                }

                if (cdcEvent.isDelete() && cdcEvent.getBeforeNode() != null) {
                    enhanceJsonNode((ObjectNode) cdcEvent.getBeforeNode(), completeRowData);
                }
            }
        } catch (Exception e) {
            logger.error("Error enhancing CDC event", e);
        }
    }

    private void enhanceJsonNode(ObjectNode node, Map<String, Object> postgresData) {
        for (Map.Entry<String, Object> entry : postgresData.entrySet()) {
            String columnName = entry.getKey();
            Object value = entry.getValue();

            if (value == null || hasFieldCaseInsensitive(node, columnName)) {
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
                node.put(columnName, ((java.sql.Timestamp) value).getTime());
            } else {
                node.put(columnName, value.toString());
            }
        }
    }

    private boolean hasFieldCaseInsensitive(JsonNode node, String fieldName) {
        if (node.has(fieldName)) {
            return true;
        }

        Iterator<String> fieldNames = node.fieldNames();
        while (fieldNames.hasNext()) {
            if (fieldNames.next().equalsIgnoreCase(fieldName)) {
                return true;
            }
        }
        return false;
    }

    public Map<String, Object> getMetrics() {
        Map<String, Object> metrics = new HashMap<>();
        metrics.put("running", running.get());
        metrics.put("dedicatedTopic", dedicatedTopic);
        metrics.put("consumerGroupId", consumerGroupId);
        metrics.put("totalBatchesProcessed", totalBatchesProcessed.get());
        metrics.put("totalEventsProcessed", totalEventsProcessed.get());
        metrics.put("totalFailedBatches", totalFailedBatches.get());
        metrics.put("batchSize", batchSize);
        return metrics;
    }

    @PreDestroy
    public void shutdown() {
        logger.info("Shutting down Dedicated CDC Consumer");

        running.set(false);

        if (consumerThread != null) {
            try {
                consumerThread.interrupt();
                consumerThread.join(30000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        if (consumer != null) {
            try {
                consumer.close(Duration.ofSeconds(10));
            } catch (Exception e) {
                logger.warn("Error closing consumer", e);
            }
        }

        logger.info("Dedicated CDC Consumer shutdown complete");
    }

    public static class EnhancedCdcEvent {
        private final CdcEvent event;
        private final Map<String, FieldTypeInfo> fieldTypeMap;

        public EnhancedCdcEvent(CdcEvent event, Map<String, FieldTypeInfo> fieldTypeMap) {
            this.event = event;
            this.fieldTypeMap = fieldTypeMap;
        }

        public CdcEvent getEvent() {
            return event;
        }

        public Map<String, FieldTypeInfo> getFieldTypeMap() {
            return fieldTypeMap;
        }
    }
}