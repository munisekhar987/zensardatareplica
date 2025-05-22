package com.zensar.data.replication.service;

import com.zensar.data.replication.model.CdcEvent;
import com.zensar.data.replication.util.OracleUdtUtil;
import com.fasterxml.jackson.databind.JsonNode;
import org.postgresql.util.PGobject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import java.sql.*;
import java.util.*;

/**
 * Service for fetching complete row data from PostgreSQL, including UDT columns
 * that may have been dropped by Debezium during CDC capture.
 * Enhanced to support composite primary keys with case-insensitive handling.
 * Enhanced to support UDT type mapping between PostgreSQL and Oracle.
 */
@Service
public class PostgresSourceService {
    private static final Logger logger = LoggerFactory.getLogger(PostgresSourceService.class);

    @Value("${postgres.db.url}")
    private String dbUrl;

    @Value("${postgres.db.username}")
    private String dbUsername;

    @Value("${postgres.db.password}")
    private String dbPassword;

    @Value("${postgres.db.schema:public}")
    private String dbSchema;

    @Value("${postgres.udt.tables:}")
    private String udtTablesConfig;

    @Value("#{${postgres.udt.type-mapping:{}}}")
    private Map<String, String> udtTypeMapping;

    @Value("#{${postgres.udt.column-mapping:{}}}")
    private Map<String, String> udtColumnMapping;

    // Service to get complete primary key info for tables
    @Autowired
    private SqlExecutionService sqlExecutionService;

    // Map of table name to primary key column(s)
    // Format: TABLE_NAME -> PRIMARY_KEY_COLUMN(S)
    // For composite keys, this will only contain the first PK column from the config
    private final Map<String, String> udtTables = new HashMap<>();

    // Map of table.column -> UDT type name
    // Populated from udtColumnMapping
    private final Map<String, String> columnToUdtType = new HashMap<>();

    @PostConstruct
    public void init() {
        if (udtTablesConfig != null && !udtTablesConfig.isEmpty()) {
            String[] tableEntries = udtTablesConfig.split(",");
            for (String entry : tableEntries) {
                String[] parts = entry.trim().split(":");
                if (parts.length == 2) {
                    udtTables.put(parts[0].trim().toUpperCase(), parts[1].trim());
                    logger.info("UDT table configuration: {} with first primary key column: {}",
                            parts[0].trim(), parts[1].trim());
                }
            }
        }

        // Log UDT type mappings
        logger.info("UDT Type Mappings:");
        for (Map.Entry<String, String> entry : udtTypeMapping.entrySet()) {
            logger.info("  PostgreSQL '{}' -> Oracle '{}'", entry.getKey(), entry.getValue());
        }

        // Process column to UDT type mappings
        logger.info("UDT Column Mappings:");
        for (Map.Entry<String, String> entry : udtColumnMapping.entrySet()) {
            String key = entry.getKey().toUpperCase();
            String value = entry.getValue().toLowerCase();
            columnToUdtType.put(key, value);
            logger.info("  Column '{}' -> UDT type '{}'", key, value);
        }
    }

    /**
     * Check if a table has UDT columns that need special handling
     * @param tableName the table name
     * @return true if the table is configured for UDT handling
     */
    public boolean isUdtTable(String tableName) {
        return udtTables.containsKey(tableName.toUpperCase());
    }

    /**
     * Check if a column is a UDT type
     * @param tableName the table name
     * @param columnName the column name
     * @return true if the column is a UDT type, false otherwise
     */
    public boolean isUdtColumn(String tableName, String columnName) {
        String key = tableName.toUpperCase() + "." + columnName.toUpperCase();
        return columnToUdtType.containsKey(key);
    }

    /**
     * Get the UDT type for a column
     * @param tableName the table name
     * @param columnName the column name
     * @return the UDT type name or null if not a UDT column
     */
    public String getUdtTypeForColumn(String tableName, String columnName) {
        String key = tableName.toUpperCase() + "." + columnName.toUpperCase();
        return columnToUdtType.get(key);
    }

    /**
     * Fetch complete row data from PostgreSQL for a CDC event
     * This will fetch all column data, including UDT columns that Debezium might have dropped
     * Enhanced to handle composite primary keys with case-insensitive column matching.
     *
     * @param event The CDC event containing primary key info
     * @return Map of column names to their values
     */
    public Map<String, Object> fetchCompleteRowData(CdcEvent event) {
        String tableName = event.getTableName().toUpperCase();

        if (!isUdtTable(tableName)) {
            logger.warn("Table {} is not configured for UDT handling", tableName);
            return Collections.emptyMap();
        }

        // Get complete list of primary key columns for this table
        List<String> primaryKeyColumns = sqlExecutionService.getPrimaryKeyFields(tableName);
        if (primaryKeyColumns.isEmpty()) {
            logger.error("No primary key columns found for UDT table {}", tableName);
            return Collections.emptyMap();
        }

        // Extract primary key values from the CDC event with case-insensitive matching
        Map<String, Object> primaryKeyValues = new HashMap<>();
        boolean allKeysFound = true;

        for (String pkColumn : primaryKeyColumns) {
            Object pkValue = null;

            // Try to find the primary key value in afterNode (case-insensitive)
            if (event.getAfterNode() != null) {
                pkValue = findFieldValueCaseInsensitive(event.getAfterNode(), pkColumn);
            }

            // If not found in afterNode, try beforeNode (case-insensitive)
            if (pkValue == null && event.getBeforeNode() != null) {
                pkValue = findFieldValueCaseInsensitive(event.getBeforeNode(), pkColumn);
            }

            if (pkValue == null) {
                logger.error("Primary key value for column {} not found in CDC event for table {}",
                        pkColumn, tableName);
                allKeysFound = false;
                break;
            }

            primaryKeyValues.put(pkColumn, pkValue);
        }

        if (!allKeysFound || primaryKeyValues.isEmpty()) {
            logger.error("Could not extract all primary key values for table {}", tableName);
            return Collections.emptyMap();
        }

        // Fetch the row from PostgreSQL using all primary key columns
        return fetchRowByPrimaryKeys(tableName, primaryKeyValues);
    }

    /**
     * Find a field value in a JsonNode with case-insensitive matching of field names
     *
     * @param node The JsonNode to search in
     * @param fieldName The field name to find (case-insensitive)
     * @return The field value or null if not found
     */
    private Object findFieldValueCaseInsensitive(JsonNode node, String fieldName) {
        if (node == null) {
            return null;
        }

        // Case 1: Exact match (most common case)
        if (node.has(fieldName)) {
            JsonNode valueNode = node.get(fieldName);
            return valueNode.isNull() ? null : valueNode.asText();
        }

        // Case 2: Case-insensitive match
        Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> field = fields.next();
            if (field.getKey().equalsIgnoreCase(fieldName)) {
                JsonNode valueNode = field.getValue();
                logger.debug("Found case-insensitive match for field '{}' as '{}' with value: {}",
                        fieldName, field.getKey(), valueNode);
                return valueNode.isNull() ? null : valueNode.asText();
            }
        }

        // Log details for debugging
        StringBuilder availableFields = new StringBuilder();
        node.fieldNames().forEachRemaining(name -> {
            if (availableFields.length() > 0) {
                availableFields.append(", ");
            }
            availableFields.append(name);
        });

        logger.debug("Field '{}' not found in JsonNode. Available fields: {}", fieldName, availableFields);
        return null;
    }

    /**
     * Fetch a row from PostgreSQL using a composite primary key
     *
     * @param tableName the table name
     * @param primaryKeyValues map of primary key column names to their values
     * @return Map of column names to their values
     */
    public Map<String, Object> fetchRowByPrimaryKeys(String tableName, Map<String, Object> primaryKeyValues) {
        Map<String, Object> result = new HashMap<>();

        // Build WHERE clause for composite primary key
        StringBuilder whereClause = new StringBuilder();
        List<Object> params = new ArrayList<>();

        for (Map.Entry<String, Object> entry : primaryKeyValues.entrySet()) {
            if (whereClause.length() > 0) {
                whereClause.append(" AND ");
            }
            // Use lowercase for column names in PostgreSQL query
            whereClause.append(entry.getKey().toLowerCase()).append(" = ?");
            params.add(entry.getValue());
        }

        String sql = String.format(
                "SELECT * FROM %s.%s WHERE %s",
                dbSchema.toLowerCase(),
                tableName.toLowerCase(), // PostgreSQL table names are usually lowercase
                whereClause
        );

        logger.info("Fetching complete row from PostgreSQL: {}", sql);
        logger.info("Primary key values: {}", primaryKeyValues);

        try (Connection connection = DriverManager.getConnection(dbUrl, dbUsername, dbPassword);
             PreparedStatement stmt = connection.prepareStatement(sql)) {

            // Set parameters for all primary key columns
            for (int i = 0; i < params.size(); i++) {
                Object param = params.get(i);

                if (param instanceof String) {
                    stmt.setString(i + 1, (String) param);
                } else if (param instanceof Integer) {
                    stmt.setInt(i + 1, (Integer) param);
                } else if (param instanceof Long) {
                    stmt.setLong(i + 1, (Long) param);
                } else {
                    stmt.setObject(i + 1, param);
                }
            }

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    ResultSetMetaData metaData = rs.getMetaData();
                    int columnCount = metaData.getColumnCount();

                    for (int i = 1; i <= columnCount; i++) {
                        String columnName = metaData.getColumnName(i);
                        Object value = rs.getObject(i);

                        // Convert PostgreSQL types to Java types that Oracle can handle
                        if (value instanceof PGobject) {
                            PGobject pgObject = (PGobject) value;
                            logger.debug("Found PGobject for column {}: type={}, value={}",
                                    columnName, pgObject.getType(), pgObject.getValue());

                            // Check if this is a known UDT column
                            String udtType = getUdtTypeForColumn(tableName, columnName);
                            if (udtType != null) {
                                // Handle UDT conversion based on known type
                                logger.debug("Column {}.{} is mapped to UDT type: {}",
                                        tableName, columnName, udtType);

                                // Convert using our utility
                                String formattedValue = OracleUdtUtil.formatPgUdtForOracle(
                                        udtType, pgObject.getValue());

                                value = formattedValue;
                            } else {
                                // Handle conversion based on generic type
                                value = convertPgObjectToJava(pgObject);
                            }
                        }

                        result.put(columnName.toUpperCase(), value);
                        logger.debug("Column: {}, Value: {}, Type: {}",
                                columnName, value, value != null ? value.getClass().getName() : "null");
                    }
                } else {
                    logger.warn("No row found in PostgreSQL for table {} with primary keys {}",
                            tableName, primaryKeyValues);
                }
            }
        } catch (SQLException e) {
            logger.error("Error fetching row from PostgreSQL: {}", e.getMessage(), e);
        }

        return result;
    }

    /**
     * Convert PostgreSQL object types to Java types
     * that can be handled by Oracle JDBC driver.
     * Enhanced to handle specific UDT types for HANDOFF_ROUTING_ROUTE_NO and HANDOFF_ROADNET_ROUTE_NO.
     */
    private Object convertPgObjectToJava(PGobject pgObject) {
        if (pgObject == null || pgObject.getValue() == null) {
            return null;
        }

        String value = pgObject.getValue();
        String type = pgObject.getType().toLowerCase();

        logger.debug("Converting PGObject of type '{}' with value: {}", type, value);

        // Check if we have a mapping for this UDT type
        String oracleType = udtTypeMapping.get(type);
        if (oracleType != null) {
            logger.debug("Found UDT mapping: PostgreSQL '{}' -> Oracle '{}'", type, oracleType);

            // Convert using our utility method for better UDT handling
            return OracleUdtUtil.formatPgUdtForOracle(type, value);
        }

        // Generic handling for common types
        switch (type) {
            case "json":
            case "jsonb":
                return value; // Return as string

            case "uuid":
                return value; // Return as string

            case "point":
                // Format: (x,y)
                value = value.replace("(", "").replace(")", "");
                String[] coordinates = value.split(",");
                if (coordinates.length == 2) {
                    return "POINT(" + coordinates[0].trim() + " " + coordinates[1].trim() + ")";
                }
                return value;

            case "handoff_routing_route_no":
            case "handoff_roadnet_route_no":
                // Special handling for these specific UDT types
                // Convert PostgreSQL composite type to Oracle VARRAY format
                return OracleUdtUtil.formatPgUdtForOracle(type, value);

            case "interval":
            case "hstore":
                return value; // Return as string

            case "_text": // Array of text
            case "_int4": // Array of integers
            case "_numeric": // Array of decimals
                // Handle array types - convert PostgreSQL array format to Oracle format if needed
                if (value.startsWith("{") && value.endsWith("}")) {
                    // Most Oracle implementations can handle this format
                    return value;
                }
                return value;

            default:
                return value;
        }
    }
}