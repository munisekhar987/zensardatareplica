package com.zensar.data.replication.enhanced.worker;

import com.zensar.data.replication.enhanced.consumer.DedicatedCdcConsumer;
import com.zensar.data.replication.enhanced.consumer.DedicatedCdcConsumer.EnhancedCdcEvent;
import com.zensar.data.replication.enhanced.service.SqlStatementGenerator;
import com.zensar.data.replication.enhanced.service.SqlStatementGenerator.PreparedSqlStatement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Simplified Worker Pool Service with simple count-based completion tracking.
 */
@Service
public class WorkerPoolService {
    private static final Logger logger = LoggerFactory.getLogger(WorkerPoolService.class);

    @Value("${cdc.enhanced.worker.thread-count:16}")
    private int totalWorkers;

    @Value("${cdc.enhanced.worker.queue-size:5000}")
    private int workerQueueSize;

    @Value("${cdc.enhanced.worker.batch-size:100}")
    private int batchSize;

    @Value("${cdc.enhanced.worker.batch-timeout-ms:1000}")
    private long batchTimeoutMs;

    @Autowired
    private SqlStatementGenerator sqlStatementGenerator;

    @Autowired
    private DataSource dataSource;

    @Autowired
    @Lazy
    private DedicatedCdcConsumer cdcConsumer;

    // Thread pool for workers
    private ExecutorService workerExecutor;

    // Queues for each worker
    private List<BlockingQueue<EventWithBatchId>> workerQueues;

    // Track if each worker is currently executing (prevents overlapping batches)
    private AtomicBoolean[] workerExecuting;

    // Enhanced Metrics
    private final AtomicLong totalEventsReceived = new AtomicLong(0);
    private final AtomicLong totalBatchesExecuted = new AtomicLong(0);
    private final AtomicLong totalStatementsExecuted = new AtomicLong(0);
    private final AtomicLong totalFailedStatements = new AtomicLong(0);
    private final AtomicLong workerBatchCounter = new AtomicLong(0);
    private volatile long lastProgressLog = System.currentTimeMillis();

    @PostConstruct
    public void init() {
        logger.info("Initializing Simplified Worker Pool Service");
        logger.info("Workers: {}, Queue Size: {}, Batch Size: {}, Timeout: {}ms",
                totalWorkers, workerQueueSize, batchSize, batchTimeoutMs);

        // Initialize collections based on configured worker count
        workerQueues = new ArrayList<>(totalWorkers);
        workerExecuting = new AtomicBoolean[totalWorkers];

        // Initialize queues and execution flags for each worker
        for (int i = 0; i < totalWorkers; i++) {
            workerQueues.add(new LinkedBlockingQueue<>(workerQueueSize));
            workerExecuting[i] = new AtomicBoolean(false);
        }

        // Create thread pool
        workerExecutor = Executors.newFixedThreadPool(totalWorkers, r -> {
            Thread t = new Thread(r, "Simplified-Worker-" + Thread.currentThread().getId());
            t.setDaemon(true);
            return t;
        });

        // Start worker threads
        for (int i = 0; i < totalWorkers; i++) {
            final int workerId = i;
            workerExecutor.submit(() -> runWorker(workerId));
        }

        logger.info("Simplified Worker Pool Service started with {} workers", totalWorkers);
    }

    /**
     * Submit event to specific worker with batch ID for tracking
     */
    public boolean submitToWorker(int workerId, EnhancedCdcEvent event, String batchId) {
        if (workerId < 0 || workerId >= totalWorkers) {
            logger.error("Invalid worker ID: {} (valid range: 0-{})", workerId, totalWorkers - 1);
            // Notify consumer of failure with details
            cdcConsumer.onStatementCompleted(batchId, false, "Invalid worker ID",
                    event.getEvent().getTableName(), event.getEvent().getOperation());
            return false;
        }

        try {
            EventWithBatchId eventWithBatchId = new EventWithBatchId(event, batchId);
            boolean submitted = workerQueues.get(workerId).offer(eventWithBatchId, 100, TimeUnit.MILLISECONDS);

            if (submitted) {
                totalEventsReceived.incrementAndGet();
            } else {
                logger.warn("Failed to submit event to worker {} (queue full)", workerId);
                // Notify consumer of failure with details
                cdcConsumer.onStatementCompleted(batchId, false, "Worker queue full",
                        event.getEvent().getTableName(), event.getEvent().getOperation());
            }
            return submitted;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("Interrupted while submitting to worker {}", workerId, e);
            // Notify consumer of failure with details
            cdcConsumer.onStatementCompleted(batchId, false, "Thread interrupted: " + e.getMessage(),
                    event.getEvent().getTableName(), event.getEvent().getOperation());
            return false;
        }
    }

    /**
     * Main worker thread logic with simple completion tracking
     */
    private void runWorker(int workerId) {
        logger.info("Worker-{} started and ready for processing", workerId);
        BlockingQueue<EventWithBatchId> queue = workerQueues.get(workerId);

        List<EventWithBatchId> currentBatch = new ArrayList<>();
        long lastBatchTime = System.currentTimeMillis();
        long workerEventsProcessed = 0;

        while (!Thread.currentThread().isInterrupted()) {
            try {
                // Only process if this worker is not currently executing
                if (workerExecuting[workerId].get()) {
                    Thread.sleep(10); // Brief wait if still executing
                    continue;
                }

                // Try to get an event
                EventWithBatchId eventWithBatchId = queue.poll(100, TimeUnit.MILLISECONDS);

                if (eventWithBatchId != null) {
                    currentBatch.add(eventWithBatchId);
                }

                // Check if we should process the batch
                boolean shouldProcessBatch = false;
                long currentTime = System.currentTimeMillis();

                if (currentBatch.size() >= batchSize) {
                    shouldProcessBatch = true;
                    logger.debug("Worker-{} processing batch due to size: {}", workerId, currentBatch.size());
                } else if (!currentBatch.isEmpty() && (currentTime - lastBatchTime) >= batchTimeoutMs) {
                    shouldProcessBatch = true;
                    logger.debug("Worker-{} processing batch due to timeout: {} events", workerId, currentBatch.size());
                }

                if (shouldProcessBatch) {
                    long batchNum = workerBatchCounter.incrementAndGet();

                    // Only log INFO for every 25th batch or large batches
                    if (batchNum % 25 == 0 || currentBatch.size() > 50) {
                        logger.info("WORKER-{}: Processing batch #{} with {} events (total processed: {})",
                                workerId, batchNum, currentBatch.size(), workerEventsProcessed);
                    } else {
                        logger.debug("Worker-{} processing batch #{} with {} events",
                                workerId, batchNum, currentBatch.size());
                    }

                    // Set executing flag to prevent overlap
                    workerExecuting[workerId].set(true);

                    try {
                        processBatch(workerId, new ArrayList<>(currentBatch));
                        workerEventsProcessed += currentBatch.size();
                    } finally {
                        // Always clear executing flag
                        workerExecuting[workerId].set(false);
                    }

                    currentBatch.clear();
                    lastBatchTime = currentTime;
                }

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.info("Worker-{} thread interrupted", workerId);
                break;
            } catch (Exception e) {
                logger.error("Error in worker thread {}", workerId, e);
                // Notify consumer of failures with details
                for (EventWithBatchId eventWithBatchId : currentBatch) {
                    cdcConsumer.onStatementCompleted(eventWithBatchId.getBatchId(), false,
                            "Worker thread error: " + e.getMessage(),
                            eventWithBatchId.getEvent().getEvent().getTableName(),
                            eventWithBatchId.getEvent().getEvent().getOperation());
                }
                currentBatch.clear();
                lastBatchTime = System.currentTimeMillis();
                workerExecuting[workerId].set(false); // Ensure flag is cleared
            }
        }

        // Process any remaining events
        if (!currentBatch.isEmpty()) {
            try {
                workerExecuting[workerId].set(true);
                processBatch(workerId, currentBatch);
                workerEventsProcessed += currentBatch.size();
            } catch (Exception e) {
                logger.error("Error processing final batch in worker {}", workerId, e);
                // Notify consumer of failures with details
                for (EventWithBatchId eventWithBatchId : currentBatch) {
                    cdcConsumer.onStatementCompleted(eventWithBatchId.getBatchId(), false,
                            "Final batch processing error: " + e.getMessage(),
                            eventWithBatchId.getEvent().getEvent().getTableName(),
                            eventWithBatchId.getEvent().getEvent().getOperation());
                }
            } finally {
                workerExecuting[workerId].set(false);
            }
        }

        logger.info("Worker-{} stopped (processed {} events total)", workerId, workerEventsProcessed);
    }

    /**
     * Process a batch of events by creating and executing SQL statements
     */
    @Transactional
    private void processBatch(int workerId, List<EventWithBatchId> batch) {
        if (batch.isEmpty()) {
            return;
        }

        // Step 1: Generate SQL statements for all events
        List<StatementWithBatchId> statements = new ArrayList<>();

        for (EventWithBatchId eventWithBatchId : batch) {
            try {
                EnhancedCdcEvent event = eventWithBatchId.getEvent();
                PreparedSqlStatement statement = sqlStatementGenerator.generateStatement(
                        event.getEvent(), event.getFieldTypeMap());

                if (statement != null) {
                    statements.add(new StatementWithBatchId(statement, eventWithBatchId.getBatchId()));
                } else {
                    logger.warn("Failed to generate statement for event: {} on table {}",
                            event.getEvent().getOperation(), event.getEvent().getTableName());
                    // Notify consumer of failure with details
                    cdcConsumer.onStatementCompleted(eventWithBatchId.getBatchId(), false,
                            "Failed to generate SQL statement",
                            event.getEvent().getTableName(),
                            event.getEvent().getOperation());
                }

            } catch (Exception e) {
                logger.error("Error generating statement for event", e);
                // Notify consumer of failure with details
                cdcConsumer.onStatementCompleted(eventWithBatchId.getBatchId(), false,
                        "Statement generation error: " + e.getMessage(),
                        eventWithBatchId.getEvent().getEvent().getTableName(),
                        eventWithBatchId.getEvent().getEvent().getOperation());
            }
        }

        if (statements.isEmpty()) {
            logger.warn("No valid statements generated for batch in worker {}", workerId);
            return;
        }

        // Step 2: Execute batch of statements on connection pool
        executeBatchedStatements(workerId, statements);
    }

    /**
     * Execute a batch of SQL statements within a transaction
     */
    private void executeBatchedStatements(int workerId, List<StatementWithBatchId> statements) {
        Connection connection = null;
        boolean transactionSuccessful = false;

        try {
            // Get connection from pool
            connection = dataSource.getConnection();
            connection.setAutoCommit(false);

            logger.debug("Worker-{} executing batch of {} statements", workerId, statements.size());

            // Execute each statement in the batch
            int successCount = 0;
            List<StatementWithBatchId> successfulStatements = new ArrayList<>();
            List<StatementWithBatchId> failedStatements = new ArrayList<>();

            for (StatementWithBatchId stmt : statements) {
                try {
                    boolean success = executeStatement(connection, stmt.getStatement());
                    if (success) {
                        successCount++;
                        successfulStatements.add(stmt);
                    } else {
                        logger.warn("Statement execution returned false: {}", stmt.getStatement());
                        failedStatements.add(stmt);
                    }
                } catch (SQLException e) {
                    String errorMessage = String.format("SQL Error [%d]: %s", e.getErrorCode(), e.getMessage());
                    logger.warn("SQL execution failed for {}: {} - {}",
                            stmt.getStatement().getType(), stmt.getStatement().getTableName(), errorMessage);

                    // Create enhanced statement with error details
                    StatementWithBatchId failedStmt = new StatementWithBatchId(stmt.getStatement(), stmt.getBatchId(), errorMessage);
                    failedStatements.add(failedStmt);

                    // For critical errors, abort the entire batch
                    if (isCriticalError(e)) {
                        logger.error("Critical error encountered, aborting batch");
                        throw e;
                    }
                }
            }

            // Commit transaction if all statements succeeded
            if (successCount == statements.size()) {
                connection.commit();
                transactionSuccessful = true;
                totalBatchesExecuted.incrementAndGet();
                totalStatementsExecuted.addAndGet(successCount);

                // Notify consumer of all successes
                for (StatementWithBatchId stmt : successfulStatements) {
                    cdcConsumer.onStatementCompleted(stmt.getBatchId(), true, null,
                            stmt.getStatement().getTableName(), stmt.getStatement().getOperation());
                }

                // Log progress every 2 minutes or every 100 batches
                long now = System.currentTimeMillis();
                long batchCount = totalBatchesExecuted.get();
                if (now - lastProgressLog > 120000 || batchCount % 100 == 0) {
                    logger.info("WORKER PROGRESS: {} batches completed, {} statements executed, {} events processed, {} failed",
                            batchCount, totalStatementsExecuted.get(), totalEventsReceived.get(), totalFailedStatements.get());
                    lastProgressLog = now;
                } else {
                    logger.debug("Worker-{} committed batch: {} statements", workerId, successCount);
                }
            } else {
                connection.rollback();
                logger.warn("Worker-{} rolled back batch due to failures. Success: {}, Total: {}",
                        workerId, successCount, statements.size());

                // Notify consumer of all failures (entire batch failed due to transaction rollback)
                for (StatementWithBatchId stmt : statements) {
                    cdcConsumer.onStatementCompleted(stmt.getBatchId(), false,
                            "Transaction rolled back due to batch failures",
                            stmt.getStatement().getTableName(),
                            stmt.getStatement().getOperation());
                }

                totalFailedStatements.addAndGet(statements.size());
            }

        } catch (SQLException e) {
            logger.error("BATCH FAILED - Worker-{}: {} ({} statements lost)",
                    workerId, e.getMessage(), statements.size());

            // Log the first few failed statements for debugging
            if (statements.size() <= 5) {
                for (StatementWithBatchId stmt : statements) {
                    logger.warn("Failed statement: {} on table {}",
                            stmt.getStatement().getType(), stmt.getStatement().getTableName());
                }
            } else {
                Set<String> tableNames = new HashSet<>();
                for (StatementWithBatchId stmt : statements) {
                    tableNames.add(stmt.getStatement().getTableName());
                }
                logger.warn("Failed batch contained {} statements for tables: {}",
                        statements.size(), tableNames);
            }

            // Rollback transaction
            if (connection != null) {
                try {
                    connection.rollback();
                    logger.debug("Transaction rolled back for worker {}", workerId);
                } catch (SQLException rollbackEx) {
                    logger.error("Error rolling back transaction for worker {}: {}", workerId, rollbackEx.getMessage());
                }
            }

            // Notify consumer of all failures
            for (StatementWithBatchId stmt : statements) {
                cdcConsumer.onStatementCompleted(stmt.getBatchId(), false,
                        "Batch execution failed: " + e.getMessage(),
                        stmt.getStatement().getTableName(),
                        stmt.getStatement().getOperation());
            }

            totalFailedStatements.addAndGet(statements.size());

        } finally {
            // Always close connection and reset auto-commit
            if (connection != null) {
                try {
                    connection.setAutoCommit(true);
                    connection.close();
                } catch (SQLException e) {
                    logger.warn("Error closing connection for worker {}: {}", workerId, e.getMessage());
                }
            }
        }

        if (!transactionSuccessful) {
            throw new RuntimeException("Batch execution failed for worker " + workerId);
        }
    }

    /**
     * Execute a single SQL statement
     */
    private boolean executeStatement(Connection connection, PreparedSqlStatement stmt) throws SQLException {
        try (PreparedStatement ps = connection.prepareStatement(stmt.getSql())) {

            // Set parameters
            List<Object> parameters = stmt.getParameters();
            if (parameters != null) {
                for (int i = 0; i < parameters.size(); i++) {
                    setStatementParameter(ps, i + 1, parameters.get(i));
                }
            }

            // Execute statement
            int rowsAffected = ps.executeUpdate();

            logger.trace("Statement executed: {} (rows affected: {})", stmt.getType(), rowsAffected);

            return true;

        } catch (SQLException e) {
            logger.debug("SQL execution error for {}: {}", stmt.getType(), e.getMessage());
            throw e;
        }
    }

    /**
     * Set parameter in prepared statement with type handling
     */
    private void setStatementParameter(PreparedStatement statement, int index, Object param) throws SQLException {
        if (param == null) {
            statement.setNull(index, Types.NULL);
        } else if (param instanceof String) {
            String strParam = (String) param;

            // Handle UDT constructor strings
            if ((strParam.startsWith("HANDOFF_ROUTING_ROUTE_NO(") && strParam.endsWith(")")) ||
                    (strParam.startsWith("HANDOFF_ROADNET_ROUTE_NO(") && strParam.endsWith(")"))) {

                try {
                    Connection conn = statement.getConnection();
                    String query = "SELECT " + strParam + " AS column_value FROM DUAL";

                    try (java.sql.Statement selectStmt = conn.createStatement();
                         java.sql.ResultSet rs = selectStmt.executeQuery(query)) {

                        if (rs.next()) {
                            Object udtObject = rs.getObject(1);
                            statement.setObject(index, udtObject);
                        } else {
                            statement.setString(index, strParam);
                        }
                    }
                } catch (SQLException e) {
                    logger.warn("Failed to evaluate UDT constructor, using string: {}", e.getMessage());
                    statement.setString(index, strParam);
                }
            } else {
                statement.setString(index, strParam);
            }
        } else if (param instanceof Integer) {
            statement.setInt(index, (Integer) param);
        } else if (param instanceof Long) {
            statement.setLong(index, (Long) param);
        } else if (param instanceof Double) {
            statement.setDouble(index, (Double) param);
        } else if (param instanceof java.math.BigDecimal) {
            statement.setBigDecimal(index, (java.math.BigDecimal) param);
        } else if (param instanceof java.util.Date) {
            statement.setTimestamp(index, new java.sql.Timestamp(((java.util.Date) param).getTime()));
        } else if (param instanceof java.sql.Timestamp) {
            statement.setTimestamp(index, (java.sql.Timestamp) param);
        } else if (param instanceof Boolean) {
            statement.setBoolean(index, (Boolean) param);
        } else {
            statement.setObject(index, param);
        }
    }

    /**
     * Check if an SQL exception is critical and should stop batch processing
     */
    private boolean isCriticalError(SQLException e) {
        String sqlState = e.getSQLState();
        int errorCode = e.getErrorCode();

        // Oracle-specific critical errors
        if (sqlState != null) {
            if (sqlState.startsWith("08")) return true; // Connection errors
            if (sqlState.startsWith("42")) return true; // Syntax errors
        }

        // Oracle error codes that are critical
        if (errorCode == 942) return true; // Table or view does not exist
        if (errorCode == 1017) return true; // Invalid username/password
        if (errorCode == 12154) return true; // TNS: could not resolve service name

        return false;
    }

    /**
     * Get current metrics
     */
    public Map<String, Object> getMetrics() {
        Map<String, Object> metrics = new HashMap<>();
        metrics.put("totalWorkers", totalWorkers);
        metrics.put("totalEventsReceived", totalEventsReceived.get());
        metrics.put("totalBatchesExecuted", totalBatchesExecuted.get());
        metrics.put("totalStatementsExecuted", totalStatementsExecuted.get());
        metrics.put("totalFailedStatements", totalFailedStatements.get());

        // Queue sizes for each worker
        Map<String, Integer> workerQueueSizes = new HashMap<>();
        for (int i = 0; i < workerQueues.size(); i++) {
            workerQueueSizes.put("worker" + i, workerQueues.get(i).size());
        }
        metrics.put("workerQueueSizes", workerQueueSizes);

        // Active workers
        long activeWorkers = 0;
        for (int i = 0; i < totalWorkers; i++) {
            if (workerExecuting[i].get()) {
                activeWorkers++;
            }
        }
        metrics.put("activeWorkers", activeWorkers);

        return metrics;
    }

    @PreDestroy
    public void shutdown() {
        logger.info("Shutting down Simplified Worker Pool Service");

        if (workerExecutor != null) {
            workerExecutor.shutdown();
            try {
                if (!workerExecutor.awaitTermination(60, TimeUnit.SECONDS)) {
                    logger.warn("Worker pool did not terminate gracefully, forcing shutdown");
                    workerExecutor.shutdownNow();
                } else {
                    logger.info("All worker threads terminated successfully");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                workerExecutor.shutdownNow();
            }
        }

        logger.info("Simplified Worker Pool Service shutdown complete");
    }

    // Helper classes for simple tracking
    private static class EventWithBatchId {
        private final EnhancedCdcEvent event;
        private final String batchId;

        public EventWithBatchId(EnhancedCdcEvent event, String batchId) {
            this.event = event;
            this.batchId = batchId;
        }

        public EnhancedCdcEvent getEvent() { return event; }
        public String getBatchId() { return batchId; }
    }

    private static class StatementWithBatchId {
        private final PreparedSqlStatement statement;
        private final String batchId;
        private final String errorMessage;

        public StatementWithBatchId(PreparedSqlStatement statement, String batchId) {
            this(statement, batchId, null);
        }

        public StatementWithBatchId(PreparedSqlStatement statement, String batchId, String errorMessage) {
            this.statement = statement;
            this.batchId = batchId;
            this.errorMessage = errorMessage;
        }

        public PreparedSqlStatement getStatement() { return statement; }
        public String getBatchId() { return batchId; }
        public String getErrorMessage() { return errorMessage; }
    }
}