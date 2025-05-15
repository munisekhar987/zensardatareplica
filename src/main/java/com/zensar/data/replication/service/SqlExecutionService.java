package com.zensar.data.replication.service;

import com.zensar.data.replication.model.CdcEvent;
import com.zensar.data.replication.model.FieldTypeInfo;
import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Service for generating and executing SQL statements based on CDC events.
 * Uses table primary keys strictly from configuration.
 */
@Service
public class SqlExecutionService {
    private static final Logger logger = LoggerFactory.getLogger(SqlExecutionService.class);

    @Value("${oracle.db.url}")
    private String dbUrl;

    @Value("${oracle.db.username}")
    private String dbUsername;

    @Value("${oracle.db.password}")
    private String dbPassword;

    @Value("${oracle.db.schema:}")
    private String dbSchema;

    @Value("${cdc.handle.duplicates:merge}")
    private String duplicateHandlingStrategy;

    // Configuration map to store primary keys for each table
    @Value("#{${cdc.table.primary-keys}}")
    private Map<String, String> tablePrimaryKeys;

    private final SimpleDateFormat timestampFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    @Autowired
    private CdcValueExtractorService valueExtractorService;

    // Case-insensitive map for field values
    private static class CaseInsensitiveMap<V> extends HashMap<String, V> {
        @Override
        public V get(Object key) {
            if (key instanceof String) {
                for (Map.Entry<String, V> entry : entrySet()) {
                    if (entry.getKey().equalsIgnoreCase((String) key)) {
                        return entry.getValue();
                    }
                }
            }
            return super.get(key);
        }

        @Override
        public boolean containsKey(Object key) {
            if (key instanceof String) {
                for (String k : keySet()) {
                    if (k.equalsIgnoreCase((String) key)) {
                        return true;
                    }
                }
                return false;
            }
            return super.containsKey(key);
        }

        @Override
        public V remove(Object key) {
            if (key instanceof String) {
                Iterator<Map.Entry<String, V>> it = entrySet().iterator();
                while (it.hasNext()) {
                    Map.Entry<String, V> entry = it.next();
                    if (entry.getKey().equalsIgnoreCase((String) key)) {
                        V value = entry.getValue();
                        it.remove();
                        return value;
                    }
                }
            }
            return super.remove(key);
        }

        @Override
        public V put(String key, V value) {
            // Check if a case-insensitive version of the key already exists
            for (String k : keySet()) {
                if (k.equalsIgnoreCase(key)) {
                    // Remove the existing entry to avoid duplicates
                    V oldValue = remove(k);
                    super.put(key.toUpperCase(), value); // Store keys in uppercase
                    return oldValue;
                }
            }
            // No case-insensitive match found, add new entry
            return super.put(key.toUpperCase(), value); // Store keys in uppercase
        }
    }

    /**
     * Execute a standard INSERT statement with duplicate handling
     */
    public void executeStandardInsert(CdcEvent event, Map<String, FieldTypeInfo> fieldTypeMap) {
        try {
            String tableName = event.getTableName().toUpperCase();
            // Use case-insensitive map for field values
            Map<String, Object> fieldValues = extractFieldValues(event.getAfterNode(), fieldTypeMap);

            if (fieldValues.isEmpty()) {
                logger.warn("No valid fields found for INSERT operation on {}", tableName);
                return;
            }

            // Log field values with types for debugging
            logger.debug("Field values for INSERT:");
            for (Map.Entry<String, Object> entry : fieldValues.entrySet()) {
                Object value = entry.getValue();
                String type = value != null ? value.getClass().getName() : "null";
                String valueStr = value != null ? value.toString() : "null";

                if (value instanceof Timestamp) {
                    valueStr = timestampFormat.format(value);
                }

                logger.debug("Field: {}, Value: {}, Type: {}", entry.getKey(), valueStr, type);
            }

            // Get primary key fields for this table - convert to uppercase for consistency
            List<String> primaryKeyFields = getPrimaryKeyFields(tableName);
            if (primaryKeyFields.isEmpty()) {
                logger.error("Primary key configuration not found for table {}. Cannot process INSERT operation without primary keys.", tableName);
                return;
            }

            // Extract primary key values - handle case insensitivity
            Map<String, Object> primaryKeyValues = new CaseInsensitiveMap<>();
            for (String pkField : primaryKeyFields) {
                // Case-insensitive get from fieldValues
                Object pkValue = null;
                for (Map.Entry<String, Object> entry : fieldValues.entrySet()) {
                    if (entry.getKey().equalsIgnoreCase(pkField)) {
                        pkValue = entry.getValue();
                        break;
                    }
                }

                if (pkValue == null) {
                    logger.warn("Primary key value for {} not found", pkField);
                    executeRegularInsert(tableName, fieldValues);
                    return;
                }
                primaryKeyValues.put(pkField.toUpperCase(), pkValue);
            }

            // Check if record with the given primary key already exists
            if (recordExists(tableName, primaryKeyValues)) {
                logger.info("Record with primary key {} already exists in {}, performing update", primaryKeyValues, tableName);
                // Create a copy of fieldValues excluding primary keys for update
                Map<String, Object> updateValues = new CaseInsensitiveMap<>();

                for (Map.Entry<String, Object> entry : fieldValues.entrySet()) {
                    boolean isPrimaryKey = false;
                    for (String pkField : primaryKeyFields) {
                        if (entry.getKey().equalsIgnoreCase(pkField)) {
                            isPrimaryKey = true;
                            break;
                        }
                    }
                    if (!isPrimaryKey) {
                        updateValues.put(entry.getKey().toUpperCase(), entry.getValue());
                    }
                }

                // Execute update operation
                executeUpdateWithPrimaryKeys(tableName, updateValues, primaryKeyValues);
            } else {
                logger.info("Record with primary key {} does not exist in {}, performing insert", primaryKeyValues, tableName);
                // Execute regular insert - make sure field names are uppercase
                Map<String, Object> upperCaseFieldValues = new CaseInsensitiveMap<>();
                for (Map.Entry<String, Object> entry : fieldValues.entrySet()) {
                    upperCaseFieldValues.put(entry.getKey().toUpperCase(), entry.getValue());
                }
                executeRegularInsert(tableName, upperCaseFieldValues);
            }
        } catch (Exception e) {
            logger.error("Error executing INSERT operation: {}", e.getMessage(), e);
        }
    }

    /**
     * Execute update operation based on CDC event.
     */
    public void executeUpdate(CdcEvent event, Map<String, FieldTypeInfo> fieldTypeMap) {
        try {
            String tableName = event.getTableName().toUpperCase();
            // Use case-insensitive map for field values
            Map<String, Object> fieldValues = extractFieldValues(event.getAfterNode(), fieldTypeMap);

            if (fieldValues.isEmpty()) {
                logger.warn("No valid fields found for UPDATE operation on {}", tableName);
                return;
            }

            // Get primary key fields for this table - already uppercase
            List<String> primaryKeyFields = getPrimaryKeyFields(tableName);
            if (primaryKeyFields.isEmpty()) {
                logger.error("Primary key configuration not found for table {} in UPDATE operation", tableName);
                return;
            }

            // Extract primary key values and create separate maps for PK and non-PK fields
            Map<String, Object> primaryKeyValues = new CaseInsensitiveMap<>();
            Map<String, Object> updateValues = new CaseInsensitiveMap<>();

            // Copy all fields to updateValues first
            for (Map.Entry<String, Object> entry : fieldValues.entrySet()) {
                updateValues.put(entry.getKey().toUpperCase(), entry.getValue());
            }

            // Extract primary keys and remove them from updateValues
            for (String pkField : primaryKeyFields) {
                // Case-insensitive retrieval
                Object pkValue = null;
                String matchedKey = null;

                for (Map.Entry<String, Object> entry : fieldValues.entrySet()) {
                    if (entry.getKey().equalsIgnoreCase(pkField)) {
                        pkValue = entry.getValue();
                        matchedKey = entry.getKey();
                        break;
                    }
                }

                if (pkValue == null) {
                    logger.warn("Primary key value for {} not found for UPDATE operation on {}", pkField, tableName);
                    return;
                }

                primaryKeyValues.put(pkField.toUpperCase(), pkValue);
                updateValues.remove(matchedKey);
            }

            if (updateValues.isEmpty()) {
                logger.warn("No fields to update after removing primary keys for table {}", tableName);
                return;
            }

            executeUpdateWithPrimaryKeys(tableName, updateValues, primaryKeyValues);

        } catch (Exception e) {
            logger.error("Error executing UPDATE operation: {}", e.getMessage(), e);
        }
    }

    /**
     * Execute delete operation based on CDC event.
     */
    public void executeDelete(CdcEvent event, Map<String, FieldTypeInfo> fieldTypeMap) {
        try {
            String tableName = event.getTableName().toUpperCase();

            // Get primary key fields for this table
            List<String> primaryKeyFields = getPrimaryKeyFields(tableName);
            if (primaryKeyFields.isEmpty()) {
                logger.error("Primary key configuration not found for table {} in DELETE operation", tableName);
                return;
            }

            // Extract values for all primary key fields - handle case insensitivity
            Map<String, Object> primaryKeyValues = new CaseInsensitiveMap<>();
            for (String pkField : primaryKeyFields) {
                JsonNode pkNode = null;
                // Case-insensitive search for the field in the beforeNode
                Iterator<Map.Entry<String, JsonNode>> fields = event.getBeforeNode().fields();
                while (fields.hasNext()) {
                    Map.Entry<String, JsonNode> field = fields.next();
                    if (field.getKey().equalsIgnoreCase(pkField)) {
                        pkNode = field.getValue();
                        break;
                    }
                }

                if (pkNode == null) {
                    logger.warn("Primary key field {} not found in beforeNode for DELETE operation", pkField);
                    return;
                }

                FieldTypeInfo keyTypeInfo = null;
                // Find the field type info with case-insensitive matching
                for (Map.Entry<String, FieldTypeInfo> entry : fieldTypeMap.entrySet()) {
                    if (entry.getKey().equalsIgnoreCase(pkField)) {
                        keyTypeInfo = entry.getValue();
                        break;
                    }
                }

                if (keyTypeInfo == null) {
                    logger.warn("Field type info for {} not found", pkField);
                    keyTypeInfo = new FieldTypeInfo("unknown", null, Collections.emptyMap());
                }

                Object pkValue = valueExtractorService.extractValue(pkNode, pkField, keyTypeInfo);

                if (pkValue == null) {
                    logger.warn("Primary key value for {} not found for DELETE operation on {}", pkField, tableName);
                    return;
                }
                primaryKeyValues.put(pkField.toUpperCase(), pkValue);
            }

            // Build WHERE clause for composite primary key
            StringBuilder whereClause = new StringBuilder();
            List<Object> params = new ArrayList<>();

            for (String pkField : primaryKeyFields) {
                if (whereClause.length() > 0) {
                    whereClause.append(" AND ");
                }
                whereClause.append(pkField.toUpperCase()).append(" = ?");
                params.add(primaryKeyValues.get(pkField.toUpperCase()));
            }

            String sql = String.format(
                    "DELETE FROM %s.%s WHERE %s",
                    dbSchema.toUpperCase(),
                    tableName,
                    whereClause
            );

            logger.info("DELETE SQL: {}", sql);
            logger.info("SQL with values: {}", constructDebugSql(sql, params));

            executeStatement(sql, params);
        } catch (Exception e) {
            logger.error("Error executing DELETE operation: {}", e.getMessage(), e);
        }
    }

    /**
     * Extract all field values from a node using schema information.
     * Returns a case-insensitive map to handle field name case differences.
     */
    private Map<String, Object> extractFieldValues(JsonNode dataNode, Map<String, FieldTypeInfo> fieldTypeMap) {
        // Use our case-insensitive map implementation
        Map<String, Object> result = new CaseInsensitiveMap<>();

        if (dataNode == null) {
            return result;
        }

        Iterator<Map.Entry<String, JsonNode>> fields = dataNode.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> field = fields.next();
            String fieldName = field.getKey();
            JsonNode fieldValue = field.getValue();

            if (fieldValue.isNull()) {
                result.put(fieldName.toUpperCase(), null);
                continue;
            }

            // Find field type info with case-insensitive matching
            FieldTypeInfo typeInfo = null;
            for (Map.Entry<String, FieldTypeInfo> entry : fieldTypeMap.entrySet()) {
                if (entry.getKey().equalsIgnoreCase(fieldName)) {
                    typeInfo = entry.getValue();
                    break;
                }
            }

            if (typeInfo == null) {
                typeInfo = new FieldTypeInfo("unknown", null, Collections.emptyMap());
            }

            Object value = valueExtractorService.extractValue(fieldValue, fieldName, typeInfo);
            result.put(fieldName.toUpperCase(), value);
        }

        return result;
    }

    /**
     * Get primary key fields for a table from configuration.
     * Returns a list of primary key field names in uppercase for consistency.
     * Returns an empty list if no configuration is found.
     */
    private List<String> getPrimaryKeyFields(String tableName) {
        // Normalize table name to uppercase
        String upperTableName = tableName.toUpperCase();

        // Try to find the primary key config with case-insensitive matching
        String primaryKeyConfig = null;
        for (Map.Entry<String, String> entry : tablePrimaryKeys.entrySet()) {
            if (entry.getKey().equalsIgnoreCase(upperTableName)) {
                primaryKeyConfig = entry.getValue();
                break;
            }
        }

        if (primaryKeyConfig == null || primaryKeyConfig.trim().isEmpty()) {
            logger.error("No primary key configuration found for table: {}", upperTableName);
            return Collections.emptyList();
        }

        List<String> primaryKeyFields = new ArrayList<>();
        for (String pkField : primaryKeyConfig.split(",")) {
            primaryKeyFields.add(pkField.trim().toUpperCase()); // Store all in uppercase
        }

        logger.debug("Primary key fields for table {}: {}", upperTableName, primaryKeyFields);
        return primaryKeyFields;
    }

    /**
     * Check if a record with the given primary key values already exists in the database
     */
    private boolean recordExists(String tableName, Map<String, Object> primaryKeyValues) {
        StringBuilder whereClause = new StringBuilder();
        List<Object> params = new ArrayList<>();

        for (Map.Entry<String, Object> entry : primaryKeyValues.entrySet()) {
            if (whereClause.length() > 0) {
                whereClause.append(" AND ");
            }
            whereClause.append(entry.getKey().toUpperCase()).append(" = ?");
            params.add(entry.getValue());
        }

        String sql = String.format(
                "SELECT COUNT(*) FROM %s.%s WHERE %s",
                dbSchema.toUpperCase(),
                tableName.toUpperCase(),
                whereClause
        );

        logger.info("Checking existence with SQL: {}", sql);
        logger.info("With parameters: {}", params);

        // Print the SQL with actual values for better debugging
        String sqlWithValues = constructDebugSql(sql, params);
        logger.info("SQL with values: {}", sqlWithValues);

        try (Connection connection = DriverManager.getConnection(dbUrl, dbUsername, dbPassword);
             PreparedStatement statement = connection.prepareStatement(sql)) {

            // Set parameters
            for (int i = 0; i < params.size(); i++) {
                setStatementParameter(statement, i + 1, params.get(i));
            }

            // Execute query
            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    int count = resultSet.getInt(1);
                    logger.info("Record existence check result: {} records found", count);
                    return count > 0;
                }
            }

        } catch (SQLException e) {
            logger.error("Error checking record existence: {}", e.getMessage(), e);
        }

        // If there's an error, assume record doesn't exist
        return false;
    }

    /**
     * Execute a regular INSERT statement without duplicate checking
     */
    private void executeRegularInsert(String tableName, Map<String, Object> fieldValues) {
        StringBuilder columns = new StringBuilder();
        StringBuilder placeholders = new StringBuilder();
        List<Object> params = new ArrayList<>();

        for (Map.Entry<String, Object> entry : fieldValues.entrySet()) {
            if (columns.length() > 0) {
                columns.append(", ");
                placeholders.append(", ");
            }
            columns.append(entry.getKey().toUpperCase());
            placeholders.append("?");
            params.add(entry.getValue());
        }

        String sql = String.format(
                "INSERT INTO %s.%s (%s) VALUES (%s)",
                dbSchema.toUpperCase(),
                tableName.toUpperCase(),
                columns,
                placeholders
        );

        logger.info("Executing regular INSERT: {}", sql);
        logger.info("Parameters: {}", params);

        // Print SQL with values for better debugging
        String sqlWithValues = constructDebugSql(sql, params);
        logger.info("SQL with values: {}", sqlWithValues);

        executeStatement(sql, params);
    }

    /**
     * Execute UPDATE statement with primary keys in WHERE clause
     */
    private void executeUpdateWithPrimaryKeys(String tableName, Map<String, Object> updateValues,
                                              Map<String, Object> primaryKeyValues) {
        if (updateValues.isEmpty()) {
            logger.warn("No fields to update for table {}", tableName);
            return;
        }

        StringBuilder setClause = new StringBuilder();
        StringBuilder whereClause = new StringBuilder();
        List<Object> params = new ArrayList<>();

        // Build SET clause
        for (Map.Entry<String, Object> entry : updateValues.entrySet()) {
            if (setClause.length() > 0) {
                setClause.append(", ");
            }
            setClause.append(entry.getKey().toUpperCase()).append(" = ?");
            params.add(entry.getValue());
        }

        // Build WHERE clause for primary keys
        for (Map.Entry<String, Object> entry : primaryKeyValues.entrySet()) {
            if (whereClause.length() > 0) {
                whereClause.append(" AND ");
            }
            whereClause.append(entry.getKey().toUpperCase()).append(" = ?");
            params.add(entry.getValue());
        }

        String sql = String.format(
                "UPDATE %s.%s SET %s WHERE %s",
                dbSchema.toUpperCase(),
                tableName.toUpperCase(),
                setClause,
                whereClause
        );

        logger.info("Executing UPDATE: {}", sql);
        logger.info("Parameters: {}", params);

        // Print SQL with values for better debugging
        String sqlWithValues = constructDebugSql(sql, params);
        logger.info("SQL with values: {}", sqlWithValues);

        executeStatement(sql, params);
    }

    /**
     * Execute SQL statement with parameters.
     */
    private void executeStatement(String sql, List<Object> params) {
        logger.info("Executing SQL: {}", sql);
        logger.info("Parameters: {}", params);

        // Always print the SQL with actual values for better debugging
        String sqlWithValues = constructDebugSql(sql, params);
        logger.info("SQL with values: {}", sqlWithValues);

        try (Connection connection = DriverManager.getConnection(dbUrl, dbUsername, dbPassword);
             PreparedStatement statement = connection.prepareStatement(sql)) {

            // Set parameters
            for (int i = 0; i < params.size(); i++) {
                setStatementParameter(statement, i + 1, params.get(i));
            }

            // Execute
            int rowsAffected = statement.executeUpdate();
            logger.info("Rows affected: {}", rowsAffected);

        } catch (SQLException e) {
            handleSqlException(e, sql);
        }
    }

    /**
     * Set a parameter in a prepared statement with appropriate type handling.
     */
    private void setStatementParameter(PreparedStatement statement, int index, Object param) throws SQLException {
        if (param == null) {
            logger.debug("Setting parameter #{} to NULL", index);
            statement.setNull(index, Types.NULL);
        } else if (param instanceof String) {
            logger.debug("Setting parameter #{} to String: {}", index, param);
            statement.setString(index, (String) param);
        } else if (param instanceof Integer) {
            logger.debug("Setting parameter #{} to Integer: {}", index, param);
            statement.setInt(index, (Integer) param);
        } else if (param instanceof Long) {
            logger.debug("Setting parameter #{} to Long: {}", index, param);
            statement.setLong(index, (Long) param);
        } else if (param instanceof Double) {
            logger.debug("Setting parameter #{} to Double: {}", index, param);
            statement.setDouble(index, (Double) param);
        } else if (param instanceof BigDecimal) {
            logger.debug("Setting parameter #{} to BigDecimal: {}", index, param);
            statement.setBigDecimal(index, (BigDecimal) param);
        } else if (param instanceof java.util.Date) {
            logger.debug("Setting parameter #{} to Timestamp from java.util.Date: {}", index, timestampFormat.format(param));
            statement.setTimestamp(index, new Timestamp(((java.util.Date) param).getTime()));
        } else if (param instanceof Timestamp) {
            logger.debug("Setting parameter #{} to Timestamp: {}", index, ((Timestamp) param).toString());
            statement.setTimestamp(index, (Timestamp) param);
        } else if (param instanceof Boolean) {
            logger.debug("Setting parameter #{} to Boolean: {}", index, param);
            statement.setBoolean(index, (Boolean) param);
        } else {
            logger.debug("Setting parameter #{} to Object: {} (type: {})", index, param, param.getClass().getName());
            statement.setObject(index, param);
        }
    }

    /**
     * Handle SQL exceptions with improved diagnostics.
     */
    private void handleSqlException(SQLException e, String sql) {
        logger.error("Error executing SQL statement: {}", e.getMessage(), e);

        // Check if the error is related to table not found
        if (e.getMessage().contains("table or view does not exist") || e.getMessage().contains("ORA-00942")) {
            logger.error("Table might not exist or you don't have access privileges");

            // Try to verify table existence
            String tableName = extractTableNameFromSql(sql);
            if (tableName != null) {
                checkTableExistence(tableName);
            }
        }

        // Check if it's a data type error
        if (e.getMessage().contains("inconsistent datatypes") || e.getMessage().contains("ORA-00932")) {
            logger.error("Data type inconsistency detected. Verify table column types in Oracle match the data types being sent.");
        }
    }

    /**
     * Check if a table exists and report database information for troubleshooting.
     */
    private void checkTableExistence(String tableName) {
        try (Connection connection = DriverManager.getConnection(dbUrl, dbUsername, dbPassword)) {
            DatabaseMetaData metaData = connection.getMetaData();

            // Show database information
            logger.info("Database: {} {}",
                    metaData.getDatabaseProductName(),
                    metaData.getDatabaseProductVersion());

            // Check if table exists - use uppercase for consistency
            String[] types = {"TABLE"};
            boolean tableFound = false;
            String upperTableName = tableName.toUpperCase();

            try (ResultSet tables = metaData.getTables(null, dbSchema.toUpperCase(), upperTableName, types)) {
                if (tables.next()) {
                    logger.info("Table {} exists under schema {}",
                            upperTableName, tables.getString("TABLE_SCHEM"));
                    tableFound = true;

                    // Get column information for debugging
                    logger.info("Columns in table {}:", upperTableName);
                    try (ResultSet columns = metaData.getColumns(null, dbSchema.toUpperCase(), upperTableName, null)) {
                        while (columns.next()) {
                            String columnName = columns.getString("COLUMN_NAME");
                            String dataType = columns.getString("TYPE_NAME");
                            int columnSize = columns.getInt("COLUMN_SIZE");

                            logger.info("  Column: {}, Type: {}, Size: {}", columnName, dataType, columnSize);
                        }
                    }
                }
            }

            if (!tableFound) {
                logger.error("Table {} does not exist under schema {}", upperTableName, dbSchema.toUpperCase());

                // List available tables
                try (ResultSet allTables = metaData.getTables(null, dbSchema.toUpperCase(), "%", types)) {
                    logger.info("Available tables in schema {}:", dbSchema.toUpperCase());
                    while (allTables.next()) {
                        logger.info("  {}.{}",
                                allTables.getString("TABLE_SCHEM"),
                                allTables.getString("TABLE_NAME"));
                    }
                }
            }
        } catch (SQLException ex) {
            logger.error("Failed to check table existence: {}", ex.getMessage(), ex);
        }
    }

    /**
     * Extract table name from SQL statement (simple version).
     * Returns the table name in uppercase for consistency.
     */
    private String extractTableNameFromSql(String sql) {
        try {
            String upperSql = sql.toUpperCase();
            if (upperSql.contains("INSERT INTO")) {
                int startIdx = upperSql.indexOf("INSERT INTO") + 12;
                int endIdx = upperSql.indexOf('(', startIdx);
                if (endIdx == -1) endIdx = upperSql.indexOf(' ', startIdx);
                String tablePath = upperSql.substring(startIdx, endIdx).trim();
                String[] parts = tablePath.split("\\.");
                return parts[parts.length - 1];
            } else if (upperSql.contains("UPDATE")) {
                int startIdx = upperSql.indexOf("UPDATE") + 7;
                int endIdx = upperSql.indexOf("SET", startIdx);
                String tablePath = upperSql.substring(startIdx, endIdx).trim();
                String[] parts = tablePath.split("\\.");
                return parts[parts.length - 1];
            } else if (upperSql.contains("DELETE FROM")) {
                int startIdx = upperSql.indexOf("DELETE FROM") + 12;
                int endIdx = upperSql.indexOf("WHERE", startIdx);
                if (endIdx == -1) endIdx = sql.length();
                String tablePath = upperSql.substring(startIdx, endIdx).trim();
                String[] parts = tablePath.split("\\.");
                return parts[parts.length - 1];
            }
        } catch (Exception e) {
            logger.warn("Failed to extract table name from SQL: {}", e.getMessage());
        }
        return null;
    }

    /**
     * Construct SQL with actual values for debugging.
     */
    private String constructDebugSql(String sql, List<Object> params) {
        StringBuilder result = new StringBuilder(sql);
        int paramIndex = 0;
        int questionMarkPos;

        while ((questionMarkPos = result.indexOf("?")) != -1 && paramIndex < params.size()) {
            Object param = params.get(paramIndex++);
            String replacement = formatParamForSql(param);
            result.replace(questionMarkPos, questionMarkPos + 1, replacement);
        }

        return result.toString();
    }

    /**
     * Format a parameter value for SQL debugging.
     */
    private String formatParamForSql(Object param) {
        if (param == null) {
            return "NULL";
        } else if (param instanceof String) {
            return "'" + ((String) param).replace("'", "''") + "'";
        } else if (param instanceof java.util.Date) {
            return "TO_TIMESTAMP('" + timestampFormat.format((java.util.Date) param) + "', 'YYYY-MM-DD HH24:MI:SS')";
        } else if (param instanceof Timestamp) {
            return "TO_TIMESTAMP('" + timestampFormat.format(new java.util.Date(((Timestamp) param).getTime())) +
                    "', 'YYYY-MM-DD HH24:MI:SS')";
        } else {
            return param.toString();
        }
    }
}