package com.zensar.data.replication.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * Utility class for handling Oracle UDT types.
 * Helps with type mapping and conversion between PostgreSQL and Oracle.
 */
public class OracleUdtUtil {
    private static final Logger logger = LoggerFactory.getLogger(OracleUdtUtil.class);

    // Cache of UDT type info to avoid repeated metadata queries
    private static final Map<String, String> udtTypeCache = new HashMap<>();

    /**
     * Check if a table column is an Oracle UDT type
     *
     * @param connection Oracle database connection
     * @param schema Schema name
     * @param tableName Table name
     * @param columnName Column name
     * @return true if the column is a UDT type
     */
    public static boolean isUdtColumn(Connection connection, String schema, String tableName, String columnName) {
        try {
            String typeInfo = getColumnTypeInfo(connection, schema, tableName, columnName);
            return typeInfo != null && !typeInfo.isEmpty() && !isStandardType(typeInfo);
        } catch (SQLException e) {
            logger.error("Error checking if column is UDT: {}", e.getMessage(), e);
            return false;
        }
    }

    /**
     * Get type information for a column
     *
     * @param connection Oracle database connection
     * @param schema Schema name
     * @param tableName Table name
     * @param columnName Column name
     * @return Type name for the column
     */
    public static String getColumnTypeInfo(Connection connection, String schema, String tableName, String columnName)
            throws SQLException {

        // Check cache first
        String cacheKey = schema.toUpperCase() + "." + tableName.toUpperCase() + "." + columnName.toUpperCase();
        if (udtTypeCache.containsKey(cacheKey)) {
            return udtTypeCache.get(cacheKey);
        }

        DatabaseMetaData metaData = connection.getMetaData();
        String typeName = null;

        try (ResultSet columns = metaData.getColumns(null, schema.toUpperCase(),
                tableName.toUpperCase(), columnName.toUpperCase())) {

            if (columns.next()) {
                typeName = columns.getString("TYPE_NAME");
                logger.debug("Column {}.{}.{} has type: {}",
                        schema, tableName, columnName, typeName);

                // Cache the result
                udtTypeCache.put(cacheKey, typeName);
            } else {
                logger.warn("Column {}.{}.{} not found in metadata",
                        schema, tableName, columnName);
            }
        }

        return typeName;
    }

    /**
     * Check if a type is a standard SQL type (not a UDT)
     *
     * @param typeName Type name from metadata
     * @return true if it's a standard type
     */
    private static boolean isStandardType(String typeName) {
        if (typeName == null) return true;

        String upperType = typeName.toUpperCase();

        // List of standard Oracle types
        return upperType.equals("VARCHAR2") ||
                upperType.equals("NVARCHAR2") ||
                upperType.equals("CHAR") ||
                upperType.equals("NCHAR") ||
                upperType.equals("NUMBER") ||
                upperType.equals("INTEGER") ||
                upperType.equals("FLOAT") ||
                upperType.equals("BINARY_FLOAT") ||
                upperType.equals("BINARY_DOUBLE") ||
                upperType.equals("DATE") ||
                upperType.equals("TIMESTAMP") ||
                upperType.equals("CLOB") ||
                upperType.equals("NCLOB") ||
                upperType.equals("BLOB") ||
                upperType.equals("RAW") ||
                upperType.equals("LONG RAW") ||
                upperType.equals("ROWID") ||
                upperType.equals("UROWID");
    }

    /**
     * Format a PostgreSQL UDT value for Oracle
     *
     * @param pgUdtType PostgreSQL UDT type name
     * @param pgUdtValue PostgreSQL UDT value
     * @return Formatted value for Oracle
     */
    public static String formatPgUdtForOracle(String pgUdtType, String pgUdtValue) {
        if (pgUdtValue == null) {
            return null;
        }

        logger.debug("Formatting PostgreSQL UDT: {} with value: {}", pgUdtType, pgUdtValue);

        // Handle specific UDT types
        switch (pgUdtType.toLowerCase()) {
            case "handoff_routing_route_no":
            case "handoff_roadnet_route_no":
                return formatArrayUdt(pgUdtType, pgUdtValue);

            default:
                // For unknown types, return as is
                return pgUdtValue;
        }
    }

    /**
     * Format a PostgreSQL array UDT value for Oracle VARRAY
     *
     * @param udtType UDT type name
     * @param udtValue UDT value from PostgreSQL
     * @return Formatted value for Oracle
     */
    private static String formatArrayUdt(String udtType, String udtValue) {
        // Extract array portion from PostgreSQL composite type
        // PostgreSQL format might be: ({"123","456","789"})
        String arrayValue = udtValue;

        // Remove outer parentheses if present
        if (arrayValue.startsWith("(") && arrayValue.endsWith(")")) {
            arrayValue = arrayValue.substring(1, arrayValue.length() - 1);
        }

        // Handle case where the value is a nested structure
        if (arrayValue.contains("={")) {
            // Format might be: handoff_routing_route_no={"123","456"}
            int equalsIndex = arrayValue.indexOf('=');
            if (equalsIndex >= 0 && arrayValue.length() > equalsIndex + 1) {
                arrayValue = arrayValue.substring(equalsIndex + 1);
            }
        }

        // Convert to Oracle VARRAY format: HANDOFF_ROUTING_ROUTE_NO('123','456','789')
        if (arrayValue.startsWith("{") && arrayValue.endsWith("}")) {
            // Remove the curly braces
            arrayValue = arrayValue.substring(1, arrayValue.length() - 1);

            // Split the array elements
            String[] elements = arrayValue.split(",");

            // Build Oracle VARRAY constructor format
            StringBuilder oracleFormat = new StringBuilder();
            oracleFormat.append(udtType.toUpperCase()).append("(");

            boolean first = true;
            for (String element : elements) {
                // Remove any quotes
                element = element.trim().replace("\"", "");

                if (!first) {
                    oracleFormat.append(",");
                }
                oracleFormat.append("'").append(element).append("'");
                first = false;
            }

            oracleFormat.append(")");

            logger.debug("Converted array UDT to Oracle format: {}", oracleFormat.toString());
            return oracleFormat.toString();
        }

        // If we can't parse it, return as is
        return udtValue;
    }
}