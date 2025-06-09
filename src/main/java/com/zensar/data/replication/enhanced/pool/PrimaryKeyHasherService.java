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
 * Primary Key Hasher Service with 4 threads that hash events to 16 partitions
 * based on primary key values for consistent ordering.
 */
@Service
public class PrimaryKeyHasherService {
    private static final Logger logger = LoggerFactory.getLogger(PrimaryKeyHasherService.class);

    private static final int HASHER_THREADS = 4;
    private static final int TOTAL_PARTITIONS = 16;

    @Value("${cdc.enhanced.hasher.queue-size:10000}")
    private int hasherQueueSize;

    @Autowired
    private SqlExecutionService sqlExecutionService;

    @Autowired
    private WorkerPoolService workerPoolService;

    private ExecutorService hasherExecutor;
    private final List<BlockingQueue<EnhancedCdcEvent>> hasherQueues = new ArrayList<>();
    private final AtomicLong hasherCounter = new AtomicLong(0);

    // Metrics
    private final AtomicLong totalEventsReceived = new AtomicLong(0);
    private final AtomicLong totalEventsHashed = new AtomicLong(0);

    @PostConstruct
    public void init() {
        logger.info("Initializing Primary Key Hasher Service with {} hashers for {} partitions",
                HASHER_THREADS, TOTAL_PARTITIONS);

        for (int i = 0; i < HASHER_THREADS; i++) {
            hasherQueues.add(new LinkedBlockingQueue<>(hasherQueueSize));
        }

        hasherExecutor = Executors.newFixedThreadPool(HASHER_THREADS, r -> {
            Thread t = new Thread(r, "Enhanced-Hasher-" + Thread.currentThread().getId());
            t.setDaemon(true);
            return t;
        });

        for (int i = 0; i < HASHER_THREADS; i++) {
            final int hasherId = i;
            hasherExecutor.submit(() -> runHasher(hasherId));
        }

        logger.info("Primary Key Hasher Service initialized successfully");
    }

    public boolean submitBatch(List<EnhancedCdcEvent> events) {
        if (events == null || events.isEmpty()) {
            return true;
        }

        totalEventsReceived.addAndGet(events.size());

        try {
            Map<Integer, List<EnhancedCdcEvent>> hasherBatches = new HashMap<>();

            for (EnhancedCdcEvent event : events) {
                int hasherId = (int) (hasherCounter.getAndIncrement() % HASHER_THREADS);
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
        logger.info("Starting hasher thread {}", hasherId);
        BlockingQueue<EnhancedCdcEvent> queue = hasherQueues.get(hasherId);

        while (!Thread.currentThread().isInterrupted()) {
            try {
                EnhancedCdcEvent event = queue.take();

                int partition = calculatePartition(event);

                boolean submitted = workerPoolService.submitToWorker(partition, event);

                if (submitted) {
                    totalEventsHashed.incrementAndGet();
                } else {
                    logger.warn("Failed to submit event to worker partition {}", partition);
                }

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.info("Hasher thread {} interrupted", hasherId);
                break;
            } catch (Exception e) {
                logger.error("Error in hasher thread {}", hasherId, e);
            }
        }

        logger.info("Hasher thread {} stopped", hasherId);
    }

    private int calculatePartition(EnhancedCdcEvent enhancedEvent) {
        try {
            CdcEvent event = enhancedEvent.getEvent();
            Map<String, FieldTypeInfo> fieldTypeMap = enhancedEvent.getFieldTypeMap();

            List<String> primaryKeyFields = sqlExecutionService.getPrimaryKeyFields(event.getTableName());

            if (primaryKeyFields.isEmpty()) {
                return Math.abs(event.getTableName().hashCode()) % TOTAL_PARTITIONS;
            }

            StringBuilder pkValue = new StringBuilder();
            JsonNode dataNode = event.getAfterNode() != null ? event.getAfterNode() : event.getBeforeNode();

            if (dataNode == null) {
                return Math.abs(event.getTableName().hashCode()) % TOTAL_PARTITIONS;
            }

            for (String pkField : primaryKeyFields) {
                Object value = extractFieldValueCaseInsensitive(dataNode, pkField);
                if (value != null) {
                    pkValue.append(pkField).append("=").append(value).append(";");
                }
            }

            if (pkValue.length() == 0) {
                return Math.abs(event.getTableName().hashCode()) % TOTAL_PARTITIONS;
            }

            String primaryKeyString = event.getTableName() + ":" + pkValue.toString();
            int hash = primaryKeyString.hashCode();
            int partition = Math.abs(hash) % TOTAL_PARTITIONS;

            logger.debug("Event for table {} with PK {} mapped to partition {}",
                    event.getTableName(), pkValue.toString(), partition);

            return partition;

        } catch (Exception e) {
            logger.error("Error calculating partition for event", e);
            return Math.abs(enhancedEvent.getEvent().getTableName().hashCode()) % TOTAL_PARTITIONS;
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
        metrics.put("hasherThreads", HASHER_THREADS);
        metrics.put("totalPartitions", TOTAL_PARTITIONS);

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
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                hasherExecutor.shutdownNow();
            }
        }

        logger.info("Primary Key Hasher Service shutdown complete");
    }
}