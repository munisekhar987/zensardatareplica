package com.zensar.data.replication.enhanced.pool;

import com.zensar.data.replication.enhanced.consumer.DedicatedCdcConsumer.EnhancedCdcEvent;
import com.zensar.data.replication.enhanced.worker.WorkerPoolService;
import com.zensar.data.replication.model.CdcEvent;
import com.zensar.data.replication.model.FieldTypeInfo;
import com.zensar.data.replication.service.SqlExecutionService;
import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Primary Key Hasher Service with configurable threads that hash events to configurable partitions
 * based on primary key values for consistent ordering.
 */
@Service
public class PrimaryKeyHasherService {
    private static final Logger logger = LoggerFactory.getLogger(PrimaryKeyHasherService.class);

    @Value("${cdc.enhanced.hasher.thread-count:4}")
    private int hasherThreadCount;

    @Value("${cdc.enhanced.worker.thread-count:16}")
    private int totalPartitions;

    @Value("${cdc.enhanced.hasher.queue-size:10000}")
    private int hasherQueueSize;

    @Autowired
    private SqlExecutionService sqlExecutionService;

    @Autowired
    private WorkerPoolService workerPoolService;

    private ExecutorService hasherExecutor;
    private final List<BlockingQueue<EnhancedCdcEvent>> hasherQueues = new ArrayList<>();
    private final AtomicLong hasherCounter = new AtomicLong(0);

    // Enhanced Metrics
    private final AtomicLong totalEventsReceived = new AtomicLong(0);
    private final AtomicLong totalEventsHashed = new AtomicLong(0);
    private final AtomicLong hashedBatchCounter = new AtomicLong(0);
    private volatile long lastInfoLog = System.currentTimeMillis();

    @PostConstruct
    public void init() {
        logger.info("Initializing Primary Key Hasher Service");
        logger.info("Hasher Threads: {}, Target Partitions: {}, Queue Size: {}",
                hasherThreadCount, totalPartitions, hasherQueueSize);

        for (int i = 0; i < hasherThreadCount; i++) {
            hasherQueues.add(new LinkedBlockingQueue<>(hasherQueueSize));
        }

        hasherExecutor = Executors.newFixedThreadPool(hasherThreadCount, r -> {
            Thread t = new Thread(r, "Enhanced-Hasher-" + Thread.currentThread().getId());
            t.setDaemon(true);
            return t;
        });

        for (int i = 0; i < hasherThreadCount; i++) {
            final int hasherId = i;
            hasherExecutor.submit(() -> runHasher(hasherId));
        }

        logger.info("Primary Key Hasher Service started with {} threads", hasherThreadCount);
    }

    public boolean submitBatch(List<EnhancedCdcEvent> events) {
        if (events == null || events.isEmpty()) {
            return true;
        }

        totalEventsReceived.addAndGet(events.size());
        long currentBatch = hashedBatchCounter.incrementAndGet();

        // Log every 20 batches or large batches
        if (currentBatch % 20 == 0 || events.size() > 200) {
            logger.info("HASHER: Processed {} batches, current: {} events, total received: {}, total hashed: {}",
                    currentBatch, events.size(), totalEventsReceived.get(), totalEventsHashed.get());
        } else {
            logger.debug("Hasher processing batch {} with {} events", currentBatch, events.size());
        }

        try {
            Map<Integer, List<EnhancedCdcEvent>> hasherBatches = new HashMap<>();

            for (EnhancedCdcEvent event : events) {
                int hasherId = (int) (hasherCounter.getAndIncrement() % hasherThreadCount);
                hasherBatches.computeIfAbsent(hasherId, k -> new ArrayList<>()).add(event);
            }

            boolean allSubmitted = true;
            for (Map.Entry<Integer, List<EnhancedCdcEvent>> entry : hasherBatches.entrySet()) {
                int hasherId = entry.getKey();
                List<EnhancedCdcEvent> hasherEvents = entry.getValue();

                for (EnhancedCdcEvent event : hasherEvents) {
                    if (!hasherQueues.get(hasherId).offer(event, 100, TimeUnit.MILLISECONDS)) {
                        logger.warn("Failed to submit event to hasher {} queue (queue full)", hasherId);
                        allSubmitted = false;
                    }
                }
            }

            return allSubmitted;

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("Interrupted while submitting batch to hashers", e);
            return false;
        } catch (Exception e) {
            logger.error("Error submitting batch to hashers", e);
            return false;
        }
    }

    private void runHasher(int hasherId) {
        logger.info("Hasher-{} thread started and ready", hasherId);
        BlockingQueue<EnhancedCdcEvent> queue = hasherQueues.get(hasherId);
        long eventsProcessed = 0;
        long lastProgressLog = System.currentTimeMillis();

        while (!Thread.currentThread().isInterrupted()) {
            try {
                EnhancedCdcEvent event = queue.take();

                int partition = calculatePartition(event);

                boolean submitted = workerPoolService.submitToWorker(partition, event);

                if (submitted) {
                    totalEventsHashed.incrementAndGet();
                    eventsProcessed++;

                    // Log progress every minute or every 1000 events
                    long now = System.currentTimeMillis();
                    if (now - lastProgressLog > 60000 || eventsProcessed % 1000 == 0) {
                        logger.info("HASHER-{}: {} events processed, queue size: {}, total hashed: {}",
                                hasherId, eventsProcessed, queue.size(), totalEventsHashed.get());
                        lastProgressLog = now;
                    }
                } else {
                    logger.warn("Hasher-{}: Failed to submit event to worker partition {}", hasherId, partition);
                }

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.info("Hasher-{} thread interrupted", hasherId);
                break;
            } catch (Exception e) {
                logger.error("Error in hasher thread {}", hasherId, e);
            }
        }

        logger.info("Hasher-{} thread stopped (processed {} events)", hasherId, eventsProcessed);
    }

    private int calculatePartition(EnhancedCdcEvent enhancedEvent) {
        try {
            CdcEvent event = enhancedEvent.getEvent();
            Map<String, FieldTypeInfo> fieldTypeMap = enhancedEvent.getFieldTypeMap();

            List<String> primaryKeyFields = sqlExecutionService.getPrimaryKeyFields(event.getTableName());

            if (primaryKeyFields.isEmpty()) {
                return Math.abs(event.getTableName().hashCode()) % totalPartitions;
            }

            StringBuilder pkValue = new StringBuilder();
            JsonNode dataNode = event.getAfterNode() != null ? event.getAfterNode() : event.getBeforeNode();

            if (dataNode == null) {
                return Math.abs(event.getTableName().hashCode()) % totalPartitions;
            }

            for (String pkField : primaryKeyFields) {
                Object value = extractFieldValueCaseInsensitive(dataNode, pkField);
                if (value != null) {
                    pkValue.append(pkField).append("=").append(value).append(";");
                }
            }

            if (pkValue.length() == 0) {
                return Math.abs(event.getTableName().hashCode()) % totalPartitions;
            }

            String primaryKeyString = event.getTableName() + ":" + pkValue.toString();
            int hash = primaryKeyString.hashCode();
            int partition = Math.abs(hash) % totalPartitions;

            logger.trace("Event for table {} with PK {} mapped to partition {}",
                    event.getTableName(), pkValue.toString(), partition);

            return partition;

        } catch (Exception e) {
            logger.error("Error calculating partition for event", e);
            return Math.abs(enhancedEvent.getEvent().getTableName().hashCode()) % totalPartitions;
        }
    }

    private Object extractFieldValueCaseInsensitive(JsonNode node, String fieldName) {
        if (node == null) {
            return null;
        }

        if (node.has(fieldName)) {
            JsonNode valueNode = node.get(fieldName);
            return valueNode.isNull() ? null : valueNode.asText();
        }

        Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> field = fields.next();
            if (field.getKey().equalsIgnoreCase(fieldName)) {
                JsonNode valueNode = field.getValue();
                return valueNode.isNull() ? null : valueNode.asText();
            }
        }

        return null;
    }

    public Map<String, Object> getMetrics() {
        Map<String, Object> metrics = new HashMap<>();
        metrics.put("totalEventsReceived", totalEventsReceived.get());
        metrics.put("totalEventsHashed", totalEventsHashed.get());
        metrics.put("hasherThreads", hasherThreadCount);
        metrics.put("totalPartitions", totalPartitions);
        metrics.put("batchesProcessed", hashedBatchCounter.get());

        Map<String, Integer> queueSizes = new HashMap<>();
        for (int i = 0; i < hasherQueues.size(); i++) {
            queueSizes.put("hasher" + i, hasherQueues.get(i).size());
        }
        metrics.put("queueSizes", queueSizes);

        return metrics;
    }

    @PreDestroy
    public void shutdown() {
        logger.info("Shutting down Primary Key Hasher Service");

        if (hasherExecutor != null) {
            hasherExecutor.shutdown();
            try {
                if (!hasherExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
                    logger.warn("Hasher pool did not terminate gracefully, forcing shutdown");
                    hasherExecutor.shutdownNow();
                } else {
                    logger.info("All hasher threads terminated successfully");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                hasherExecutor.shutdownNow();
            }
        }

        logger.info("Primary Key Hasher Service shutdown complete");
    }
}