package com.zensar.data.replication.controller;

import com.zensar.data.replication.enhanced.consumer.DedicatedCdcConsumer;
import com.zensar.data.replication.enhanced.pool.PrimaryKeyHasherService;
import com.zensar.data.replication.enhanced.worker.WorkerPoolService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

/**
 * REST Controller for CDC Processing Metrics and Health Check
 */
@RestController
@RequestMapping("/api/cdc/metrics")
public class CdcMetricsController {

    private static final Logger logger = LoggerFactory.getLogger(CdcMetricsController.class);

    @Autowired
    private DedicatedCdcConsumer cdcConsumer;

    @Autowired
    private PrimaryKeyHasherService hasherService;

    @Autowired
    private WorkerPoolService workerPoolService;

    /**
     * Get comprehensive CDC processing metrics
     */
    @GetMapping("/status")
    public ResponseEntity<Map<String, Object>> getCdcStatus() {
        try {
            Map<String, Object> response = new HashMap<>();

            // Consumer Metrics
            Map<String, Object> consumerMetrics = cdcConsumer.getMetrics();

            // Hasher Metrics
            Map<String, Object> hasherMetrics = hasherService.getMetrics();

            // Worker Metrics
            Map<String, Object> workerMetrics = workerPoolService.getMetrics();

            // Current timestamp
            String currentTime = LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);

            // Summary metrics for quick overview
            Map<String, Object> summary = new HashMap<>();
            summary.put("totalEventsReceived", consumerMetrics.get("totalEventsProcessed"));
            summary.put("totalEventsSuccessfullyProcessed", workerMetrics.get("totalStatementsExecuted"));
            summary.put("totalEventsFailed", workerMetrics.get("totalFailedStatements"));
            summary.put("totalBatchesProcessed", consumerMetrics.get("totalBatchesProcessed"));
            summary.put("totalBatchesCommitted", consumerMetrics.get("totalCommittedBatches"));
            summary.put("totalBatchesFailed", consumerMetrics.get("totalFailedBatches"));
            summary.put("pendingBatches", consumerMetrics.get("pendingBatches"));
            summary.put("isRunning", consumerMetrics.get("running"));

            // Calculate success rate
            long totalReceived = (Long) consumerMetrics.get("totalEventsProcessed");
            long totalProcessed = (Long) workerMetrics.get("totalStatementsExecuted");
            double successRate = totalReceived > 0 ? (double) totalProcessed / totalReceived * 100 : 0.0;
            summary.put("successRate", String.format("%.2f%%", successRate));

            // Build complete response
            response.put("timestamp", currentTime);
            response.put("status", "SUCCESS");
            response.put("summary", summary);
            response.put("consumer", consumerMetrics);
            response.put("hasher", hasherMetrics);
            response.put("worker", workerMetrics);

            return ResponseEntity.ok(response);

        } catch (Exception e) {
            logger.error("Error retrieving CDC metrics", e);

            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("timestamp", LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
            errorResponse.put("status", "ERROR");
            errorResponse.put("message", "Failed to retrieve metrics: " + e.getMessage());

            return ResponseEntity.internalServerError().body(errorResponse);
        }
    }

    /**
     * Get simplified summary metrics for quick health check
     */
    @GetMapping("/summary")
    public ResponseEntity<Map<String, Object>> getCdcSummary() {
        try {
            Map<String, Object> consumerMetrics = cdcConsumer.getMetrics();
            Map<String, Object> workerMetrics = workerPoolService.getMetrics();

            Map<String, Object> summary = new HashMap<>();

            // Key metrics
            long totalReceived = (Long) consumerMetrics.get("totalEventsProcessed");
            long totalProcessed = (Long) workerMetrics.get("totalStatementsExecuted");
            long totalFailed = (Long) workerMetrics.get("totalFailedStatements");
            boolean isRunning = (Boolean) consumerMetrics.get("running");
            int pendingBatches = (Integer) consumerMetrics.get("pendingBatches");

            summary.put("timestamp", LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
            summary.put("isRunning", isRunning);
            summary.put("totalEventsReceived", totalReceived);
            summary.put("totalEventsProcessed", totalProcessed);
            summary.put("totalEventsFailed", totalFailed);
            summary.put("pendingBatches", pendingBatches);

            // Calculate success rate
            double successRate = totalReceived > 0 ? (double) totalProcessed / totalReceived * 100 : 0.0;
            summary.put("successRate", String.format("%.2f%%", successRate));

            // Health status
            String healthStatus;
            if (!isRunning) {
                healthStatus = "STOPPED";
            } else if (pendingBatches > 100) {
                healthStatus = "BACKLOG";
            } else if (successRate < 95.0 && totalReceived > 100) {
                healthStatus = "DEGRADED";
            } else {
                healthStatus = "HEALTHY";
            }
            summary.put("healthStatus", healthStatus);

            return ResponseEntity.ok(summary);

        } catch (Exception e) {
            logger.error("Error retrieving CDC summary", e);

            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("timestamp", LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
            errorResponse.put("status", "ERROR");
            errorResponse.put("message", e.getMessage());

            return ResponseEntity.internalServerError().body(errorResponse);
        }
    }

    /**
     * Get consumer-specific metrics
     */
    @GetMapping("/consumer")
    public ResponseEntity<Map<String, Object>> getConsumerMetrics() {
        try {
            Map<String, Object> metrics = cdcConsumer.getMetrics();
            metrics.put("timestamp", LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
            return ResponseEntity.ok(metrics);
        } catch (Exception e) {
            logger.error("Error retrieving consumer metrics", e);
            return ResponseEntity.internalServerError().body(
                    Map.of("error", "Failed to retrieve consumer metrics", "message", e.getMessage())
            );
        }
    }

    /**
     * Get hasher-specific metrics
     */
    @GetMapping("/hasher")
    public ResponseEntity<Map<String, Object>> getHasherMetrics() {
        try {
            Map<String, Object> metrics = hasherService.getMetrics();
            metrics.put("timestamp", LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
            return ResponseEntity.ok(metrics);
        } catch (Exception e) {
            logger.error("Error retrieving hasher metrics", e);
            return ResponseEntity.internalServerError().body(
                    Map.of("error", "Failed to retrieve hasher metrics", "message", e.getMessage())
            );
        }
    }

    /**
     * Get worker pool metrics
     */
    @GetMapping("/workers")
    public ResponseEntity<Map<String, Object>> getWorkerMetrics() {
        try {
            Map<String, Object> metrics = workerPoolService.getMetrics();
            metrics.put("timestamp", LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
            return ResponseEntity.ok(metrics);
        } catch (Exception e) {
            logger.error("Error retrieving worker metrics", e);
            return ResponseEntity.internalServerError().body(
                    Map.of("error", "Failed to retrieve worker metrics", "message", e.getMessage())
            );
        }
    }

    /**
     * Get queue status across all components
     */
    @GetMapping("/queues")
    public ResponseEntity<Map<String, Object>> getQueueStatus() {
        try {
            Map<String, Object> queueStatus = new HashMap<>();

            // Get hasher queue sizes
            Map<String, Object> hasherMetrics = hasherService.getMetrics();
            queueStatus.put("hasherQueues", hasherMetrics.get("queueSizes"));

            // Get worker queue sizes
            Map<String, Object> workerMetrics = workerPoolService.getMetrics();
            queueStatus.put("workerQueues", workerMetrics.get("workerQueueSizes"));

            // Calculate total queue depths
            Map<String, Integer> hasherQueues = (Map<String, Integer>) hasherMetrics.get("queueSizes");
            Map<String, Integer> workerQueues = (Map<String, Integer>) workerMetrics.get("workerQueueSizes");

            int totalHasherQueueDepth = hasherQueues.values().stream().mapToInt(Integer::intValue).sum();
            int totalWorkerQueueDepth = workerQueues.values().stream().mapToInt(Integer::intValue).sum();

            queueStatus.put("totalHasherQueueDepth", totalHasherQueueDepth);
            queueStatus.put("totalWorkerQueueDepth", totalWorkerQueueDepth);
            queueStatus.put("totalQueueDepth", totalHasherQueueDepth + totalWorkerQueueDepth);
            queueStatus.put("timestamp", LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));

            return ResponseEntity.ok(queueStatus);

        } catch (Exception e) {
            logger.error("Error retrieving queue status", e);
            return ResponseEntity.internalServerError().body(
                    Map.of("error", "Failed to retrieve queue status", "message", e.getMessage())
            );
        }
    }

    /**
     * Health check endpoint
     */
    @GetMapping("/health")
    public ResponseEntity<Map<String, Object>> getHealthCheck() {
        try {
            Map<String, Object> consumerMetrics = cdcConsumer.getMetrics();
            boolean isRunning = (Boolean) consumerMetrics.get("running");

            Map<String, Object> health = new HashMap<>();
            health.put("timestamp", LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
            health.put("status", isRunning ? "UP" : "DOWN");
            health.put("cdcConsumerRunning", isRunning);

            if (isRunning) {
                return ResponseEntity.ok(health);
            } else {
                return ResponseEntity.status(503).body(health); // Service Unavailable
            }

        } catch (Exception e) {
            logger.error("Error performing health check", e);

            Map<String, Object> health = new HashMap<>();
            health.put("timestamp", LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
            health.put("status", "DOWN");
            health.put("error", e.getMessage());

            return ResponseEntity.status(503).body(health);
        }
    }

    /**
     * Get CORRECTED TPS (Transactions Per Second) metrics
     */
    @GetMapping("/tps")
    public ResponseEntity<Map<String, Object>> getTpsMetrics() {
        try {
            Map<String, Object> consumerMetrics = cdcConsumer.getMetrics();
            Map<String, Object> workerMetrics = workerPoolService.getMetrics();

            Map<String, Object> tpsData = new HashMap<>();

            // Consumer TPS (CORRECTED - only actual processing time)
            double actualTps = (Double) consumerMetrics.getOrDefault("actualAverageTps", 0.0);
            long lastBatchTps = (Long) consumerMetrics.getOrDefault("lastBatchTps", 0L);
            long totalEvents = (Long) consumerMetrics.getOrDefault("totalEvents", 0L);
            long processingTimeSeconds = (Long) consumerMetrics.getOrDefault("actualProcessingTimeSeconds", 0L);
            long appRuntimeSeconds = (Long) consumerMetrics.getOrDefault("appRuntimeSeconds", 0L);
            double utilization = (Double) consumerMetrics.getOrDefault("utilizationPercentage", 0.0);

            // Worker TPS (SQL statements executed)
            long totalStatements = (Long) workerMetrics.get("totalStatementsExecuted");
            double workerTps = processingTimeSeconds > 0 ? (double) totalStatements / processingTimeSeconds : 0.0;

            tpsData.put("timestamp", LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
            tpsData.put("actualProcessingTps", String.format("%.2f events/sec", actualTps));
            tpsData.put("lastBatchTps", lastBatchTps + " events/sec");
            tpsData.put("workerTps", String.format("%.2f statements/sec", workerTps));
            tpsData.put("totalEventsProcessed", totalEvents);
            tpsData.put("totalStatementsExecuted", totalStatements);
            tpsData.put("actualProcessingTime", processingTimeSeconds + " seconds");
            tpsData.put("appRuntime", appRuntimeSeconds + " seconds");
            tpsData.put("processingUtilization", String.format("%.1f%%", utilization));
            tpsData.put("efficiency", totalEvents > 0 ? String.format("%.2f%%", (double) totalStatements / totalEvents * 100) : "0.00%");

            return ResponseEntity.ok(tpsData);

        } catch (Exception e) {
            logger.error("Error retrieving TPS metrics", e);
            return ResponseEntity.internalServerError().body(
                    Map.of("error", "Failed to retrieve TPS metrics", "message", e.getMessage())
            );
        }
    }
    public ResponseEntity<Map<String, Object>> getThroughputMetrics() {
        try {
            Map<String, Object> consumerMetrics = cdcConsumer.getMetrics();
            Map<String, Object> hasherMetrics = hasherService.getMetrics();
            Map<String, Object> workerMetrics = workerPoolService.getMetrics();

            Map<String, Object> throughput = new HashMap<>();

            // Current counts
            long totalReceived = (Long) consumerMetrics.get("totalEventsProcessed");
            long totalHashed = (Long) hasherMetrics.get("totalEventsHashed");
            long totalProcessed = (Long) workerMetrics.get("totalStatementsExecuted");
            long totalBatches = (Long) consumerMetrics.get("totalBatchesProcessed");

            throughput.put("totalEventsReceived", totalReceived);
            throughput.put("totalEventsHashed", totalHashed);
            throughput.put("totalEventsProcessed", totalProcessed);
            throughput.put("totalBatchesProcessed", totalBatches);

            // Calculate averages
            if (totalBatches > 0) {
                double avgEventsPerBatch = (double) totalReceived / totalBatches;
                throughput.put("averageEventsPerBatch", String.format("%.1f", avgEventsPerBatch));
            }

            // Pipeline efficiency
            if (totalReceived > 0) {
                double hashingEfficiency = (double) totalHashed / totalReceived * 100;
                double processingEfficiency = (double) totalProcessed / totalReceived * 100;

                throughput.put("hashingEfficiency", String.format("%.2f%%", hashingEfficiency));
                throughput.put("processingEfficiency", String.format("%.2f%%", processingEfficiency));
            }

            throughput.put("timestamp", LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));

            return ResponseEntity.ok(throughput);

        } catch (Exception e) {
            logger.error("Error retrieving throughput metrics", e);
            return ResponseEntity.internalServerError().body(
                    Map.of("error", "Failed to retrieve throughput metrics", "message", e.getMessage())
            );
        }
    }
}