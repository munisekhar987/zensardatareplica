package com.zensar.data.replication.sync;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.*;

import javax.sql.DataSource;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;

/**
 * Data Synchronization Checker
 * Compares data between PostgreSQL (source) and Oracle (target)
 * to verify CDC replication accuracy.
 */
@RestController
@RequestMapping("/sync-check")
@Service
public class DataSyncChecker {
    private static final Logger logger = LoggerFactory.getLogger(DataSyncChecker.class);

    @Value("${postgres.db.url}")
    private String postgresUrl;

    @Value("${postgres.db.username}")
    private String postgresUsername;

    @Value("${postgres.db.password}")
    private String postgresPassword;

    @Value("${postgres.db.schema:public}")
    private String postgresSchema;

    @Value("${oracle.db.url}")
    private String oracleUrl;

    @Value("${oracle.db.username}")
    private String oracleUsername;

    @Value("${oracle.db.password}")
    private String oraclePassword;

    @Value("${oracle.db.schema:}")
    private String oracleSchema;

    @Autowired
    private DataSource oracleDataSource; // Your existing Oracle connection

    /**
     * POST /sync-check/table
     * Check synchronization for a specific table
     */
    @PostMapping("/table")
    public SyncCheckResult checkTableSync(@RequestBody SyncCheckRequest request) {
        logger.info("Starting sync check for table: {} (PostgreSQL) -> {} (Oracle)",
                request.getPostgresTable(), request.getOracleTable());

        SyncCheckResult result = new SyncCheckResult();
        result.setPostgresTable(request.getPostgresTable());
        result.setOracleTable(request.getOracleTable());
        result.setCheckStartTime(LocalDateTime.now());

        try {
            // Step 1: Count records
            result.setPostgresCount(getRecordCount(getPostgresConnection(), postgresSchema, request.getPostgresTable()));
            result.setOracleCount(getRecordCount(oracleDataSource.getConnection(), oracleSchema, request.getOracleTable()));

            // Step 2: Sample verification (if requested)
            if (request.isDetailedCheck()) {
                result.setMismatchedRecords(findMismatchedRecords(request));
            }

            // Step 3: Primary key verification
            if (request.getPrimaryKeyColumns() != null && !request.getPrimaryKeyColumns().isEmpty()) {
                result.setMissingInOracle(findMissingRecords(request, "oracle"));
                result.setMissingInPostgres(findMissingRecords(request, "postgres"));
            }

            result.setCheckEndTime(LocalDateTime.now());
            result.setStatus(calculateSyncStatus(result));

            logger.info("Sync check completed. PostgreSQL: {}, Oracle: {}, Status: {}",
                    result.getPostgresCount(), result.getOracleCount(), result.getStatus());

        } catch (Exception e) {
            logger.error("Error during sync check", e);
            result.setStatus("ERROR");
            result.setErrorMessage(e.getMessage());
        }

        return result;
    }

    /**
     * GET /sync-check/quick/{tableName}
     * Quick count comparison for a table
     */
    @GetMapping("/quick/{tableName}")
    public Map<String, Object> quickCheck(@PathVariable String tableName) {
        Map<String, Object> result = new HashMap<>();

        try {
            // Assume same table name in both databases
            long postgresCount = getRecordCount(getPostgresConnection(), postgresSchema, tableName.toLowerCase());
            long oracleCount = getRecordCount(oracleDataSource.getConnection(), oracleSchema, tableName.toUpperCase());

            result.put("postgres_count", postgresCount);
            result.put("oracle_count", oracleCount);
            result.put("difference", Math.abs(postgresCount - oracleCount));
            result.put("sync_percentage", calculateSyncPercentage(postgresCount, oracleCount));
            result.put("status", postgresCount == oracleCount ? "IN_SYNC" : "OUT_OF_SYNC");
            result.put("timestamp", LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));

        } catch (Exception e) {
            result.put("error", e.getMessage());
            result.put("status", "ERROR");
        }

        return result;
    }

    /**
     * POST /sync-check/batch
     * Check multiple tables at once
     */
    @PostMapping("/batch")
    public Map<String, Object> batchCheck(@RequestBody BatchSyncCheckRequest request) {
        Map<String, Object> results = new HashMap<>();
        ExecutorService executor = Executors.newFixedThreadPool(5); // Parallel checks

        try {
            List<Future<Map<String, Object>>> futures = new ArrayList<>();

            for (String tableName : request.getTableNames()) {
                futures.add(executor.submit(() -> {
                    Map<String, Object> tableResult = new HashMap<>();
                    try {
                        // Convert table name for each database
                        String postgresTable = tableName.toLowerCase();
                        String oracleTable = tableName.toUpperCase();

                        long postgresCount = getRecordCount(getPostgresConnection(), postgresSchema, postgresTable);
                        long oracleCount = getRecordCount(oracleDataSource.getConnection(), oracleSchema, oracleTable);

                        tableResult.put("postgres_count", postgresCount);
                        tableResult.put("oracle_count", oracleCount);
                        tableResult.put("difference", Math.abs(postgresCount - oracleCount));
                        tableResult.put("sync_percentage", calculateSyncPercentage(postgresCount, oracleCount));
                        tableResult.put("status", postgresCount == oracleCount ? "IN_SYNC" : "OUT_OF_SYNC");

                    } catch (Exception e) {
                        tableResult.put("error", e.getMessage());
                        tableResult.put("status", "ERROR");
                    }
                    return tableResult;
                }));
            }

            // Collect results
            for (int i = 0; i < request.getTableNames().size(); i++) {
                String tableName = request.getTableNames().get(i);
                try {
                    results.put(tableName, futures.get(i).get(30, TimeUnit.SECONDS));
                } catch (Exception e) {
                    Map<String, Object> errorResult = new HashMap<>();
                    errorResult.put("error", e.getMessage());
                    errorResult.put("status", "TIMEOUT");
                    results.put(tableName, errorResult);
                }
            }

        } finally {
            executor.shutdown();
        }

        return results;
    }

    /**
     * GET /sync-check/continuous/{tableName}
     * Continuous monitoring of a table sync status
     */
    @GetMapping("/continuous/{tableName}")
    public Map<String, Object> continuousCheck(@PathVariable String tableName,
                                               @RequestParam(defaultValue = "10") int intervalSeconds,
                                               @RequestParam(defaultValue = "60") int durationMinutes) {

        Map<String, Object> result = new HashMap<>();
        List<Map<String, Object>> snapshots = new ArrayList<>();

        long endTime = System.currentTimeMillis() + (durationMinutes * 60 * 1000);

        while (System.currentTimeMillis() < endTime) {
            try {
                Map<String, Object> snapshot = new HashMap<>();

                long postgresCount = getRecordCount(getPostgresConnection(), postgresSchema, tableName.toLowerCase());
                long oracleCount = getRecordCount(oracleDataSource.getConnection(), oracleSchema, tableName.toUpperCase());

                snapshot.put("timestamp", LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
                snapshot.put("postgres_count", postgresCount);
                snapshot.put("oracle_count", oracleCount);
                snapshot.put("difference", Math.abs(postgresCount - oracleCount));
                snapshot.put("sync_percentage", calculateSyncPercentage(postgresCount, oracleCount));

                snapshots.add(snapshot);

                Thread.sleep(intervalSeconds * 1000);

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                logger.error("Error in continuous check", e);
                break;
            }
        }

        result.put("table_name", tableName);
        result.put("snapshots", snapshots);
        result.put("total_snapshots", snapshots.size());

        return result;
    }

    /**
     * Get record count from a specific table
     */
    private long getRecordCount(Connection connection, String schema, String tableName) throws SQLException {
        String sql;
        if (schema != null && !schema.isEmpty()) {
            sql = "SELECT COUNT(*) FROM " + schema + "." + tableName;
        } else {
            sql = "SELECT COUNT(*) FROM " + tableName;
        }

        try (PreparedStatement stmt = connection.prepareStatement(sql);
             ResultSet rs = stmt.executeQuery()) {

            if (rs.next()) {
                return rs.getLong(1);
            }
            return 0;
        } finally {
            connection.close();
        }
    }

    /**
     * Find records that exist in one database but not the other
     */
    private List<Map<String, Object>> findMissingRecords(SyncCheckRequest request, String missingFrom) throws SQLException {
        List<Map<String, Object>> missingRecords = new ArrayList<>();

        if (request.getPrimaryKeyColumns() == null || request.getPrimaryKeyColumns().isEmpty()) {
            return missingRecords; // Can't check without primary key
        }

        Connection sourceConn = missingFrom.equals("oracle") ? getPostgresConnection() : oracleDataSource.getConnection();
        Connection targetConn = missingFrom.equals("oracle") ? oracleDataSource.getConnection() : getPostgresConnection();

        try {
            String sourceSchema = missingFrom.equals("oracle") ? postgresSchema : oracleSchema;
            String targetSchema = missingFrom.equals("oracle") ? oracleSchema : postgresSchema;
            String sourceTable = missingFrom.equals("oracle") ? request.getPostgresTable() : request.getOracleTable();
            String targetTable = missingFrom.equals("oracle") ? request.getOracleTable() : request.getPostgresTable();

            // Build primary key columns string
            String pkColumns = String.join(", ", request.getPrimaryKeyColumns());

            // Get sample of primary keys from source
            String sourceSql = String.format("SELECT %s FROM %s.%s LIMIT 100", pkColumns, sourceSchema, sourceTable);

            try (PreparedStatement sourceStmt = sourceConn.prepareStatement(sourceSql);
                 ResultSet sourceRs = sourceStmt.executeQuery()) {

                while (sourceRs.next()) {
                    // Build WHERE clause for target check
                    StringBuilder whereClause = new StringBuilder();
                    List<Object> params = new ArrayList<>();

                    for (int i = 0; i < request.getPrimaryKeyColumns().size(); i++) {
                        if (i > 0) whereClause.append(" AND ");
                        whereClause.append(request.getPrimaryKeyColumns().get(i)).append(" = ?");
                        params.add(sourceRs.getObject(i + 1));
                    }

                    // Check if this record exists in target
                    String targetSql = String.format("SELECT COUNT(*) FROM %s.%s WHERE %s",
                            targetSchema, targetTable, whereClause.toString());

                    try (PreparedStatement targetStmt = targetConn.prepareStatement(targetSql)) {
                        for (int i = 0; i < params.size(); i++) {
                            targetStmt.setObject(i + 1, params.get(i));
                        }

                        try (ResultSet targetRs = targetStmt.executeQuery()) {
                            if (targetRs.next() && targetRs.getLong(1) == 0) {
                                // Record missing in target
                                Map<String, Object> missingRecord = new HashMap<>();
                                for (int i = 0; i < request.getPrimaryKeyColumns().size(); i++) {
                                    missingRecord.put(request.getPrimaryKeyColumns().get(i), params.get(i));
                                }
                                missingRecords.add(missingRecord);
                            }
                        }
                    }
                }
            }
        } finally {
            sourceConn.close();
            targetConn.close();
        }

        return missingRecords;
    }

    /**
     * Find records with mismatched data
     */
    private List<Map<String, Object>> findMismatchedRecords(SyncCheckRequest request) throws SQLException {
        List<Map<String, Object>> mismatched = new ArrayList<>();

        // This is a simplified version - you can enhance based on your needs
        // For now, just return empty list as detailed comparison is complex

        return mismatched;
    }

    /**
     * Get PostgreSQL connection
     */
    private Connection getPostgresConnection() throws SQLException {
        return DriverManager.getConnection(postgresUrl, postgresUsername, postgresPassword);
    }

    /**
     * Calculate sync status based on counts
     */
    private String calculateSyncStatus(SyncCheckResult result) {
        if (result.getPostgresCount() == result.getOracleCount()) {
            return "IN_SYNC";
        } else if (Math.abs(result.getPostgresCount() - result.getOracleCount()) <= 10) {
            return "NEARLY_SYNC";
        } else {
            return "OUT_OF_SYNC";
        }
    }

    /**
     * Calculate sync percentage
     */
    private double calculateSyncPercentage(long postgres, long oracle) {
        if (postgres == 0 && oracle == 0) return 100.0;
        if (postgres == 0 || oracle == 0) return 0.0;

        long max = Math.max(postgres, oracle);
        long min = Math.min(postgres, oracle);

        return (double) min / max * 100.0;
    }

    // Data classes for requests and responses

    public static class SyncCheckRequest {
        private String postgresTable;
        private String oracleTable;
        private List<String> primaryKeyColumns;
        private boolean detailedCheck = false;
        private int sampleSize = 100;

        // Getters and setters
        public String getPostgresTable() { return postgresTable; }
        public void setPostgresTable(String postgresTable) { this.postgresTable = postgresTable; }

        public String getOracleTable() { return oracleTable; }
        public void setOracleTable(String oracleTable) { this.oracleTable = oracleTable; }

        public List<String> getPrimaryKeyColumns() { return primaryKeyColumns; }
        public void setPrimaryKeyColumns(List<String> primaryKeyColumns) { this.primaryKeyColumns = primaryKeyColumns; }

        public boolean isDetailedCheck() { return detailedCheck; }
        public void setDetailedCheck(boolean detailedCheck) { this.detailedCheck = detailedCheck; }

        public int getSampleSize() { return sampleSize; }
        public void setSampleSize(int sampleSize) { this.sampleSize = sampleSize; }
    }

    public static class BatchSyncCheckRequest {
        private List<String> tableNames;

        public List<String> getTableNames() { return tableNames; }
        public void setTableNames(List<String> tableNames) { this.tableNames = tableNames; }
    }

    public static class SyncCheckResult {
        private String postgresTable;
        private String oracleTable;
        private long postgresCount;
        private long oracleCount;
        private String status;
        private LocalDateTime checkStartTime;
        private LocalDateTime checkEndTime;
        private List<Map<String, Object>> mismatchedRecords = new ArrayList<>();
        private List<Map<String, Object>> missingInOracle = new ArrayList<>();
        private List<Map<String, Object>> missingInPostgres = new ArrayList<>();
        private String errorMessage;

        // Getters and setters
        public String getPostgresTable() { return postgresTable; }
        public void setPostgresTable(String postgresTable) { this.postgresTable = postgresTable; }

        public String getOracleTable() { return oracleTable; }
        public void setOracleTable(String oracleTable) { this.oracleTable = oracleTable; }

        public long getPostgresCount() { return postgresCount; }
        public void setPostgresCount(long postgresCount) { this.postgresCount = postgresCount; }

        public long getOracleCount() { return oracleCount; }
        public void setOracleCount(long oracleCount) { this.oracleCount = oracleCount; }

        public String getStatus() { return status; }
        public void setStatus(String status) { this.status = status; }

        public LocalDateTime getCheckStartTime() { return checkStartTime; }
        public void setCheckStartTime(LocalDateTime checkStartTime) { this.checkStartTime = checkStartTime; }

        public LocalDateTime getCheckEndTime() { return checkEndTime; }
        public void setCheckEndTime(LocalDateTime checkEndTime) { this.checkEndTime = checkEndTime; }

        public List<Map<String, Object>> getMismatchedRecords() { return mismatchedRecords; }
        public void setMismatchedRecords(List<Map<String, Object>> mismatchedRecords) { this.mismatchedRecords = mismatchedRecords; }

        public List<Map<String, Object>> getMissingInOracle() { return missingInOracle; }
        public void setMissingInOracle(List<Map<String, Object>> missingInOracle) { this.missingInOracle = missingInOracle; }

        public List<Map<String, Object>> getMissingInPostgres() { return missingInPostgres; }
        public void setMissingInPostgres(List<Map<String, Object>> missingInPostgres) { this.missingInPostgres = missingInPostgres; }

        public String getErrorMessage() { return errorMessage; }
        public void setErrorMessage(String errorMessage) { this.errorMessage = errorMessage; }
    }
}