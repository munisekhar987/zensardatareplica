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
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Simplified CDC Consumer with count-based offset management.
 * Simple approach: Track batch size vs completed count, commit when they match.
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

    @Value("${cdc.enhanced.hasher.thread-count:4}")
    private int hasherThreadCount;

    @Value("${cdc.enhanced.worker.thread-count:16}")
    private int workerThreadCount;

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

    // Simple batch tracking with retry logic
    private final Map<String, PendingBatch> pendingBatches = new ConcurrentHashMap<>();
    private final Map<String, FailedBatch> failedBatches = new ConcurrentHashMap<>();
    private final ScheduledExecutorService offsetCommitter = Executors.newSingleThreadScheduledExecutor(
            r -> new Thread(r, "Offset-Committer"));

    // Failure tracking
    private final List<String> criticalFailureReasons = new ArrayList<>();
    private volatile boolean consumerStopped = false;

    @Value("${cdc.enhanced.retry.max-attempts:2}")
    private int maxRetryAttempts;

    // Enhanced Metrics
    private final AtomicLong totalBatchesProcessed = new AtomicLong(0);
    private final AtomicLong totalEventsProcessed = new AtomicLong(0);
    private final AtomicLong totalFailedBatches = new AtomicLong(0);
    private final AtomicLong totalCommittedBatches = new AtomicLong(0);
    private final AtomicLong batchCounter = new AtomicLong(0);
    private volatile long lastProgressLog = System.currentTimeMillis();

    @PostConstruct
    public void init() {
        logger.info("=== SIMPLIFIED CDC CONSUMER STARTING ===");
        logger.info("Topic: {}", dedicatedTopic);
        logger.info("Consumer Group: {}", consumerGroupId);
        logger.info("Batch Size: {}", batchSize);
        logger.info("Poll Timeout: {}ms", pollTimeoutMs);
        logger.info("Architecture: 1 Consumer → {} Hashers → {} Workers",
                hasherThreadCount, workerThreadCount);
        logger.info("========================================");

        startConsumer();
        // Remove async offset committer - we're doing sync commits now

        logger.info("=== CDC CONSUMER READY FOR PROCESSING ===");
    }

    private void startConsumer() {
        if (running.compareAndSet(false, true)) {
            Properties props = new Properties();
            props.putAll(consumerFactory.getConfigurationProperties());
            props.put("group.id", consumerGroupId);
            props.put("enable.auto.commit", "false"); // Manual offset management

            consumer = new KafkaConsumer<>(props);
            consumer.subscribe(Collections.singletonList(dedicatedTopic));

            consumerThread = new Thread(this::consumeLoop, "Enhanced-CDC-Consumer");
            consumerThread.setDaemon(false);
            consumerThread.start();

            logger.info("CDC Consumer thread started for topic: {}", dedicatedTopic);
        }
    }

    private void startOffsetCommitter() {
        // Check for completed batches every 5 seconds
        offsetCommitter.scheduleAtFixedRate(this::checkAndCommitCompletedBatches, 5, 5, TimeUnit.SECONDS);
    }

    private void consumeLoop() {
        logger.info("Starting CDC consumer loop for topic: {}", dedicatedTopic);

        List<BatchRecord> currentBatch = new ArrayList<>();

        while (running.get() && !Thread.currentThread().isInterrupted() && !consumerStopped) {
            try {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(pollTimeoutMs));

                if (records.isEmpty()) {
                    if (!currentBatch.isEmpty()) {
                        logger.debug("Processing partial batch of {} events due to timeout", currentBatch.size());
                        processBatch(new ArrayList<>(currentBatch));
                        currentBatch.clear();
                    }
                    continue;
                }

                // Commit any pending offsets (thread-safe)
                commitPendingOffsets();

                for (ConsumerRecord<String, String> record : records) {
                    try {
                        EnhancedCdcEvent event = parseRecord(record);
                        if (event != null) {
                            BatchRecord batchRecord = new BatchRecord(event, record.topic(),
                                    record.partition(), record.offset());
                            currentBatch.add(batchRecord);
                        }
                    } catch (Exception e) {
                        logger.error("Error parsing record from offset {}", record.offset(), e);
                    }
                }

                if (currentBatch.size() >= batchSize) {
                    long batchNum = batchCounter.incrementAndGet();

                    if (batchNum % 10 == 0 || currentBatch.size() > 500) {
                        logger.info("CONSUMER: Processing batch #{} with {} events, total processed: {}",
                                batchNum, currentBatch.size(), totalEventsProcessed.get());
                    } else {
                        logger.debug("Processing batch #{} of {} events", batchNum, currentBatch.size());
                    }

                    processBatch(new ArrayList<>(currentBatch));
                    currentBatch.clear();
                }

            } catch (Exception e) {
                logger.error("Error in consumer loop", e);
                currentBatch.clear();

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }

        // Process final batch
        if (!currentBatch.isEmpty() && !consumerStopped) {
            logger.info("Processing final batch of {} events", currentBatch.size());
            try {
                processBatch(currentBatch);
            } catch (Exception e) {
                logger.error("Error processing final batch", e);
            }
        }

        if (consumerStopped) {
            logger.error("=== CDC CONSUMER STOPPED DUE TO CRITICAL FAILURES ===");
            for (String reason : criticalFailureReasons) {
                logger.error("CRITICAL FAILURE: {}", reason);
            }
            logger.error("========================================================");
        }

        logger.info("CDC consumer loop stopped");
    }

    private void commitPendingOffsets() {
        if (pendingOffsetCommits.isEmpty()) {
            return;
        }

        try {
            Map<TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata> toCommit;
            synchronized (this) {
                toCommit = new HashMap<>(pendingOffsetCommits);
                pendingOffsetCommits.clear();
            }

            if (!toCommit.isEmpty()) {
                consumer.commitSync(toCommit);
                logger.debug("Successfully committed {} pending offsets", toCommit.size());
            }
        } catch (Exception e) {
            logger.error("Error committing pending offsets", e);
            // Re-add failed offsets for retry
            synchronized (this) {
                pendingOffsetCommits.putAll(pendingOffsetCommits);
            }
        }
    }

    private void processBatch(List<BatchRecord> batchRecords) {
        if (batchRecords.isEmpty()) {
            return;
        }

        String batchId = UUID.randomUUID().toString();
        totalBatchesProcessed.incrementAndGet();

        // Create pending batch tracker
        PendingBatch pendingBatch = new PendingBatch(batchId, batchRecords);
        pendingBatches.put(batchId, pendingBatch);

        // Extract events for processing
        List<EnhancedCdcEvent> events = batchRecords.stream()
                .map(BatchRecord::getEvent)
                .collect(ArrayList::new, ArrayList::add, ArrayList::addAll);

        // Log large batches
        if (events.size() > 100) {
            logger.info("CONSUMER: Processing batch: {} events (ID: {})", events.size(), batchId);
        } else {
            logger.debug("Submitting batch of {} events to hasher service (ID: {})", events.size(), batchId);
        }

        try {
            // Submit to hasher service - simple submission without complex callbacks
            boolean submitted = hasherService.submitBatch(events, batchId);

            if (!submitted) {
                logger.error("Failed to submit batch to hasher service (ID: {})", batchId);
                pendingBatches.remove(batchId);
                totalFailedBatches.incrementAndGet();
                return; // Don't process this batch
            }

            // ✅ CRITICAL: WAIT for batch completion before returning
            logger.info("WAITING for batch {} completion ({} events)", batchId, events.size());

            boolean batchCompleted = waitForBatchCompletion(batchId, events.size());

            if (batchCompleted) {
                // ✅ Batch completed successfully - commit offsets
                commitBatchOffsetsSync(pendingBatch);
                totalCommittedBatches.incrementAndGet();
                totalEventsProcessed.addAndGet(events.size());
                logger.info("BATCH COMPLETED & COMMITTED: {} ({} events)", batchId, events.size());
            } else {
                // ❌ Batch failed or timed out - DO NOT commit offsets
                logger.error("BATCH FAILED OR TIMED OUT: {} ({} events) - NO COMMIT", batchId, events.size());
                totalFailedBatches.incrementAndGet();
            }

        } catch (Exception e) {
            logger.error("Error processing batch (ID: {})", batchId, e);
            totalFailedBatches.incrementAndGet();
        } finally {
            // Clean up
            pendingBatches.remove(batchId);
        }
    }

    /**
     * BLOCKING method that waits for batch completion
     */
    private boolean waitForBatchCompletion(String batchId, int expectedEventCount) {
        long startTime = System.currentTimeMillis();
        long timeoutMs = 60000; // 60 seconds timeout

        while (System.currentTimeMillis() - startTime < timeoutMs) {
            PendingBatch batch = pendingBatches.get(batchId);
            if (batch == null) {
                return false; // Batch disappeared
            }

            int completed = batch.getCompletedCount();
            int failed = batch.getFailedCount();
            int total = completed + failed;

            if (total >= expectedEventCount) {
                // All events processed
                if (failed == 0) {
                    logger.info("BATCH SUCCESS: {} - {}/{} events completed", batchId, completed, expectedEventCount);
                    return true;
                } else {
                    logger.warn("BATCH PARTIAL FAILURE: {} - {}/{} completed, {} failed",
                            batchId, completed, expectedEventCount, failed);
                    return false;
                }
            }

            // Log progress every 5 seconds
            if ((System.currentTimeMillis() - startTime) % 5000 < 100) {
                logger.info("WAITING: {} - {}/{} events completed", batchId, total, expectedEventCount);
            }

            try {
                Thread.sleep(100); // Check every 100ms
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }

        // Timeout
        logger.error("TIMEOUT waiting for batch completion: {}", batchId);
        return false;
    }

    /**
     * Synchronous offset commit (thread-safe)
     */
    private void commitBatchOffsetsSync(PendingBatch batch) {
        try {
            Map<TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata> offsetsToCommit = new HashMap<>();

            for (BatchRecord record : batch.getBatchRecords()) {
                TopicPartition tp = new TopicPartition(record.getTopic(), record.getPartition());
                long nextOffset = record.getOffset() + 1;
                offsetsToCommit.put(tp, new org.apache.kafka.clients.consumer.OffsetAndMetadata(nextOffset));
            }

            // Commit from consumer thread (thread-safe)
            consumer.commitSync(offsetsToCommit);
            logger.info("COMMITTED offsets for batch {} ({} partitions)",
                    batch.getBatchId(), offsetsToCommit.size());

        } catch (Exception e) {
            logger.error("ERROR committing offsets for batch {}", batch.getBatchId(), e);
            throw new RuntimeException("Failed to commit offsets", e);
        }
    }

    /**
     * Called by worker when a SQL statement is completed
     */
    public void onStatementCompleted(String batchId, boolean success, String failureReason, String tableName, String operation) {
        PendingBatch pendingBatch = pendingBatches.get(batchId);
        if (pendingBatch != null) {
            if (success) {
                pendingBatch.incrementCompletedCount();
            } else {
                pendingBatch.incrementFailedCount();
                // Track failure details
                pendingBatch.addFailureDetail(tableName, operation, failureReason);
            }
        }
    }

    /**
     * Overloaded method for backward compatibility
     */
    public void onStatementCompleted(String batchId, boolean success) {
        onStatementCompleted(batchId, success, null, null, null);
    }

    private void checkAndCommitCompletedBatches() {
        if (!running.get() || consumer == null || consumerStopped) {
            return;
        }

        List<String> completedBatchIds = new ArrayList<>();

        for (Map.Entry<String, PendingBatch> entry : pendingBatches.entrySet()) {
            String batchId = entry.getKey();
            PendingBatch batch = entry.getValue();

            int totalEvents = batch.getTotalEvents();
            int completedEvents = batch.getCompletedCount();
            int failedEvents = batch.getFailedCount();

            // Check if batch is complete (all events processed)
            if (completedEvents + failedEvents >= totalEvents) {
                completedBatchIds.add(batchId);

                if (failedEvents == 0) {
                    // All events succeeded - commit offsets
                    commitBatchOffsets(batch);
                    totalCommittedBatches.incrementAndGet();
                    totalEventsProcessed.addAndGet(completedEvents);

                    logger.debug("Batch {} completed successfully: {}/{} events",
                            batchId, completedEvents, totalEvents);
                } else {
                    // Some events failed - handle retry logic
                    handleFailedBatch(batch);
                }
            }
        }

        // Remove completed batches
        for (String batchId : completedBatchIds) {
            pendingBatches.remove(batchId);
        }

        // Log progress
        if (!completedBatchIds.isEmpty()) {
            long now = System.currentTimeMillis();
            if (now - lastProgressLog > 120000) { // Every 2 minutes
                logger.info("CONSUMER PROGRESS: {} batches committed, {} events processed, {} failed batches, {} pending",
                        totalCommittedBatches.get(), totalEventsProcessed.get(),
                        totalFailedBatches.get(), pendingBatches.size());
                lastProgressLog = now;
            }
        }
    }

    private void handleFailedBatch(PendingBatch batch) {
        String batchId = batch.getBatchId();
        int failedEvents = batch.getFailedCount();
        int totalEvents = batch.getTotalEvents();
        int completedEvents = batch.getCompletedCount();

        logger.warn("Batch {} failed: {}/{} events succeeded, {} failed",
                batchId, completedEvents, totalEvents, failedEvents);

        // Check if this batch has already been retried
        FailedBatch existingFailure = failedBatches.get(batchId);

        if (existingFailure == null) {
            // First failure - add to failed batches for retry
            FailedBatch failedBatch = new FailedBatch(batch, 1);
            failedBatches.put(batchId, failedBatch);

            logger.info("RETRY: Batch {} will be retried (attempt 1/{}) - {} failures: {}",
                    batchId, maxRetryAttempts, failedEvents, batch.getFailureDetails());

            // Retry the batch
            retryBatch(batch);

        } else if (existingFailure.getRetryCount() < maxRetryAttempts) {
            // Retry again
            existingFailure.incrementRetryCount();

            logger.warn("RETRY: Batch {} will be retried (attempt {}/{}) - {} failures: {}",
                    batchId, existingFailure.getRetryCount(), maxRetryAttempts, failedEvents, batch.getFailureDetails());

            retryBatch(batch);

        } else {
            // Max retries exceeded - stop consumer
            logger.error("CRITICAL: Batch {} exceeded max retries ({}) - STOPPING CONSUMER",
                    batchId, maxRetryAttempts);

            // Log detailed failure information
            logCriticalFailure(batch);

            // Stop the consumer
            stopConsumerDueToCriticalFailure(batch);
        }

        totalFailedBatches.incrementAndGet();
    }

    private void retryBatch(PendingBatch failedBatch) {
        try {
            // Reset counters for retry
            failedBatch.resetCounters();

            // Create new batch ID for retry
            String retryBatchId = failedBatch.getBatchId() + "-retry-" + System.currentTimeMillis();

            // Update batch ID
            failedBatch.setBatchId(retryBatchId);

            // Move from pending to new retry batch
            pendingBatches.put(retryBatchId, failedBatch);

            // Extract events for reprocessing
            List<EnhancedCdcEvent> events = failedBatch.getBatchRecords().stream()
                    .map(BatchRecord::getEvent)
                    .collect(ArrayList::new, ArrayList::add, ArrayList::addAll);

            logger.info("RETRY: Resubmitting batch {} with {} events", retryBatchId, events.size());

            // Resubmit to hasher service
            boolean submitted = hasherService.submitBatch(events, retryBatchId);

            if (!submitted) {
                logger.error("RETRY FAILED: Could not resubmit batch {}", retryBatchId);
                pendingBatches.remove(retryBatchId);
            }

        } catch (Exception e) {
            logger.error("Error during batch retry", e);
        }
    }

    private void logCriticalFailure(PendingBatch batch) {
        StringBuilder failureLog = new StringBuilder();
        failureLog.append("\n=== CRITICAL FAILURE DETAILS ===\n");
        failureLog.append("Batch ID: ").append(batch.getBatchId()).append("\n");
        failureLog.append("Total Events: ").append(batch.getTotalEvents()).append("\n");
        failureLog.append("Failed Events: ").append(batch.getFailedCount()).append("\n");
        failureLog.append("Completed Events: ").append(batch.getCompletedCount()).append("\n");
        failureLog.append("Max Retry Attempts: ").append(maxRetryAttempts).append("\n");
        failureLog.append("\nFAILED STATEMENTS:\n");

        List<String> failureDetails = batch.getFailureDetails();
        for (int i = 0; i < failureDetails.size(); i++) {
            failureLog.append("  ").append(i + 1).append(". ").append(failureDetails.get(i)).append("\n");
        }

        failureLog.append("=================================");

        logger.error(failureLog.toString());

        // Store for shutdown message
        String criticalReason = String.format("Batch %s failed %d times with %d statement failures",
                batch.getBatchId(), maxRetryAttempts, batch.getFailedCount());
        criticalFailureReasons.add(criticalReason);
    }

    private void stopConsumerDueToCriticalFailure(PendingBatch batch) {
        consumerStopped = true;
        running.set(false);

        logger.error("=== STOPPING CDC CONSUMER DUE TO CRITICAL FAILURES ===");
        logger.error("Consumer will stop processing new events.");
        logger.error("Manual intervention required to resolve failures.");
        logger.error("Check the failure details above and fix the root cause.");
        logger.error("======================================================");
    }

    private void commitBatchOffsets(PendingBatch batch) {
        try {
            // Use a queue to pass commit requests to the consumer thread
            synchronized (this) {
                Map<TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata> offsetsToCommit = new HashMap<>();

                for (BatchRecord record : batch.getBatchRecords()) {
                    TopicPartition tp = new TopicPartition(record.getTopic(), record.getPartition());
                    long nextOffset = record.getOffset() + 1;
                    offsetsToCommit.put(tp, new org.apache.kafka.clients.consumer.OffsetAndMetadata(nextOffset));
                }

                // Store offsets to commit - will be committed by consumer thread
                pendingOffsetCommits.putAll(offsetsToCommit);

                logger.debug("Queued offsets for commit - batch {} with {} partitions",
                        batch.getBatchId(), offsetsToCommit.size());
            }

        } catch (Exception e) {
            logger.error("Error queuing offsets for batch {}", batch.getBatchId(), e);
            throw new RuntimeException("Failed to queue offsets for commit", e);
        }
    }

    // Add this field at the top of the class
    private final Map<TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata> pendingOffsetCommits = new ConcurrentHashMap<>();

    // ... (keep all the existing parsing methods unchanged) ...

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

    @Scheduled(fixedRate = 300000) // Every 5 minutes
    public void logHealthStatus() {
        if (running.get()) {
            logger.info("CDC HEALTH CHECK - Batches: {}, Events: {}, Committed: {}, Failed: {}, Pending: {}, Running: {}",
                    totalBatchesProcessed.get(),
                    totalEventsProcessed.get(),
                    totalCommittedBatches.get(),
                    totalFailedBatches.get(),
                    pendingBatches.size(),
                    running.get());
        }
    }

    public Map<String, Object> getMetrics() {
        Map<String, Object> metrics = new HashMap<>();
        metrics.put("running", running.get());
        metrics.put("dedicatedTopic", dedicatedTopic);
        metrics.put("consumerGroupId", consumerGroupId);
        metrics.put("totalBatchesProcessed", totalBatchesProcessed.get());
        metrics.put("totalEventsProcessed", totalEventsProcessed.get());
        metrics.put("totalCommittedBatches", totalCommittedBatches.get());
        metrics.put("totalFailedBatches", totalFailedBatches.get());
        metrics.put("pendingBatches", pendingBatches.size());
        metrics.put("batchSize", batchSize);
        return metrics;
    }

    @PreDestroy
    public void shutdown() {
        logger.info("Shutting down CDC Consumer");

        running.set(false);

        // Wait for pending batches to complete
        if (!pendingBatches.isEmpty()) {
            logger.info("Waiting for {} pending batches to complete", pendingBatches.size());
            try {
                Thread.sleep(10000); // Wait 10 seconds for completion
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        // Final offset commit attempt
        checkAndCommitCompletedBatches();

        if (offsetCommitter != null) {
            offsetCommitter.shutdown();
        }

        if (consumerThread != null) {
            try {
                consumerThread.interrupt();
                consumerThread.join(30000);
                logger.info("Consumer thread stopped successfully");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.warn("Consumer thread shutdown interrupted");
            }
        }

        if (consumer != null) {
            try {
                consumer.close(Duration.ofSeconds(10));
                logger.info("Kafka consumer closed successfully");
            } catch (Exception e) {
                logger.warn("Error closing consumer", e);
            }
        }

        logger.info("=== CDC CONSUMER SHUTDOWN COMPLETE ===");
    }

    // Helper classes
    public static class EnhancedCdcEvent {
        private final CdcEvent event;
        private final Map<String, FieldTypeInfo> fieldTypeMap;

        public EnhancedCdcEvent(CdcEvent event, Map<String, FieldTypeInfo> fieldTypeMap) {
            this.event = event;
            this.fieldTypeMap = fieldTypeMap;
        }

        public CdcEvent getEvent() { return event; }
        public Map<String, FieldTypeInfo> getFieldTypeMap() { return fieldTypeMap; }
    }

    private static class BatchRecord {
        private final EnhancedCdcEvent event;
        private final String topic;
        private final int partition;
        private final long offset;

        public BatchRecord(EnhancedCdcEvent event, String topic, int partition, long offset) {
            this.event = event;
            this.topic = topic;
            this.partition = partition;
            this.offset = offset;
        }

        public EnhancedCdcEvent getEvent() { return event; }
        public String getTopic() { return topic; }
        public int getPartition() { return partition; }
        public long getOffset() { return offset; }
    }

    private static class PendingBatch {
        private String batchId;
        private final List<BatchRecord> batchRecords;
        private final AtomicLong completedCount = new AtomicLong(0);
        private final AtomicLong failedCount = new AtomicLong(0);
        private final List<String> failureDetails = new ArrayList<>();

        public PendingBatch(String batchId, List<BatchRecord> batchRecords) {
            this.batchId = batchId;
            this.batchRecords = new ArrayList<>(batchRecords);
        }

        public String getBatchId() { return batchId; }
        public void setBatchId(String batchId) { this.batchId = batchId; }
        public List<BatchRecord> getBatchRecords() { return batchRecords; }
        public int getTotalEvents() { return batchRecords.size(); }
        public int getCompletedCount() { return (int) completedCount.get(); }
        public int getFailedCount() { return (int) failedCount.get(); }
        public List<String> getFailureDetails() { return new ArrayList<>(failureDetails); }

        public void incrementCompletedCount() { completedCount.incrementAndGet(); }
        public void incrementFailedCount() { failedCount.incrementAndGet(); }

        public void addFailureDetail(String tableName, String operation, String reason) {
            if (tableName != null && operation != null && reason != null) {
                String detail = String.format("Table: %s, Operation: %s, Reason: %s", tableName, operation, reason);
                synchronized (failureDetails) {
                    failureDetails.add(detail);
                }
            }
        }

        public void resetCounters() {
            completedCount.set(0);
            failedCount.set(0);
            synchronized (failureDetails) {
                failureDetails.clear();
            }
        }
    }

    private static class FailedBatch {
        private final PendingBatch originalBatch;
        private int retryCount;

        public FailedBatch(PendingBatch originalBatch, int retryCount) {
            this.originalBatch = originalBatch;
            this.retryCount = retryCount;
        }

        public PendingBatch getOriginalBatch() { return originalBatch; }
        public int getRetryCount() { return retryCount; }
        public void incrementRetryCount() { this.retryCount++; }
    }
}