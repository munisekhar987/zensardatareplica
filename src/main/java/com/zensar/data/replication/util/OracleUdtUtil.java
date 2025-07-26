package com.zensar.data.replication.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utility class for converting PostgreSQL UDT values to Oracle UDT format.
 * Handles conversion of PostgreSQL composite types and arrays to Oracle VARRAY format.
 */
public class OracleUdtUtil {
    private static final Logger logger = LoggerFactory.getLogger(OracleUdtUtil.class);

    // Pattern to match PostgreSQL composite type format: (value1,value2,...)
    private static final Pattern PG_COMPOSITE_PATTERN = Pattern.compile("\\(([^)]+)\\)");

    // Pattern to match PostgreSQL array format: {value1,value2,...} or {"value1","value2",...}
    private static final Pattern PG_ARRAY_PATTERN = Pattern.compile("\\{([^}]+)\\}");

    /**
     * Convert PostgreSQL UDT value to Oracle UDT constructor format using column-specific mapping.
     * This method uses the column mapping to determine the correct Oracle UDT type.
     *
     * @param tableName The table name
     * @param columnName The column name
     * @param pgUdtType The PostgreSQL UDT type from column mapping
     * @param pgValue The PostgreSQL UDT value
     * @param typeMapping The UDT type mapping configuration
     * @return Oracle UDT constructor string
     */
    public static String formatPgUdtForOracleByColumn(String tableName, String columnName,
                                                      String pgUdtType, String pgValue,
                                                      Map<String, String> typeMapping) {
        if (pgValue == null || pgValue.trim().isEmpty()) {
            return null;
        }

        logger.debug("Converting PostgreSQL UDT for column {}.{}: pgUdtType={}, value={}",
                tableName, columnName, pgUdtType, pgValue);

        try {
            // Get the Oracle UDT type from the type mapping
            String oracleUdtType = typeMapping.get(pgUdtType.toLowerCase());
            if (oracleUdtType == null) {
                logger.warn("No Oracle UDT mapping found for PostgreSQL type: {}", pgUdtType);
                oracleUdtType = pgUdtType.toUpperCase();
            }

            // Add schema prefix if not present
            if (!oracleUdtType.contains(".")) {
                oracleUdtType = "TRANSP." + oracleUdtType;
            }

            // Parse the PostgreSQL value and convert to Oracle format
            String oracleValue = convertPgValueToOracleFormat(pgValue);

            // Construct Oracle UDT constructor
            String result = oracleUdtType + "(" + oracleValue + ")";

            logger.info("FINAL UDT CONVERSION for column {}.{}: INPUT='{}' OUTPUT='{}'",
                    tableName, columnName, pgValue, result);
            return result;

        } catch (Exception e) {
            logger.error("Error converting PostgreSQL UDT value '{}' of type '{}' for column {}.{}: {}",
                    pgValue, pgUdtType, tableName, columnName, e.getMessage());
            return null;
        }
    }

    /**
     * Convert PostgreSQL UDT value to Oracle UDT constructor format.
     *
     * @param udtType The UDT type name (e.g., "handoff_routing_route_no")
     * @param pgValue The PostgreSQL UDT value
     * @return Oracle UDT constructor string
     */
    public static String formatPgUdtForOracle(String udtType, String pgValue) {
        if (pgValue == null || pgValue.trim().isEmpty()) {
            return null;
        }

        logger.debug("Converting PostgreSQL UDT: type={}, value={}", udtType, pgValue);

        try {
            // Map PostgreSQL UDT type to Oracle UDT type
            String oracleUdtType = mapPgTypeToOracleType(udtType);

            // Parse the PostgreSQL value and convert to Oracle format
            String oracleValue = convertPgValueToOracleFormat(pgValue);

            // Construct Oracle UDT constructor
            String result = oracleUdtType + "(" + oracleValue + ")";

            logger.debug("Converted UDT: {} -> {}", pgValue, result);
            return result;

        } catch (Exception e) {
            logger.error("Error converting PostgreSQL UDT value '{}' of type '{}': {}",
                    pgValue, udtType, e.getMessage());
            return null;
        }
    }

    /**
     * Map PostgreSQL UDT type name to Oracle UDT type name.
     */
    private static String mapPgTypeToOracleType(String pgType) {
        switch (pgType.toLowerCase()) {
            case "handoff_routing_route_no":
                return "TRANSP.HANDOFF_ROUTING_ROUTE_NO";
            case "handoff_roadnet_route_no":
                return "TRANSP.HANDOFF_ROADNET_ROUTE_NO";
            default:
                // Default mapping - convert to uppercase and add schema prefix
                return "TRANSP." + pgType.toUpperCase();
        }
    }

    /**
     * Convert PostgreSQL value format to Oracle VARRAY format.
     * Handles various PostgreSQL formats:
     * - Simple values: 576-1
     * - Composite types: (576-1)
     * - Arrays: {value1,value2,...}
     * - Quoted arrays: {"value1","value2",...}
     * - Single values in braces: {9988279}
     * - Complex quoted arrays: "({040-1,040-1,...})"
     * - NULL values: null, NULL, "NULL", etc.
     */
    private static String convertPgValueToOracleFormat(String pgValue) {
        if (pgValue == null) {
            return "null"; // Return unquoted null for actual null values
        }

        String trimmedValue = pgValue.trim();

        // Handle NULL values early (case-insensitive)
        if (trimmedValue.equalsIgnoreCase("null") || trimmedValue.equalsIgnoreCase("NULL")) {
            return "null"; // Return unquoted null
        }

        // Handle quoted NULL values: "NULL" -> null
        if (trimmedValue.equals("\"NULL\"") || trimmedValue.equals("\"null\"") ||
                trimmedValue.equals("'NULL'") || trimmedValue.equals("'null'")) {
            return "null"; // Return unquoted null
        }

        // Handle quoted complex arrays: "({040-1,040-1,...})"
        if (trimmedValue.startsWith("\"(") && trimmedValue.endsWith(")\"")) {
            // Remove outer quotes and parentheses: "({...})" -> {...}
            String innerContent = trimmedValue.substring(2, trimmedValue.length() - 2);

            // Remove braces if present: {...} -> ...
            if (innerContent.startsWith("{") && innerContent.endsWith("}")) {
                innerContent = innerContent.substring(1, innerContent.length() - 1);
            }

            return convertArrayToOracleFormat(innerContent);
        }

        // Handle composite type format: (value1,value2,...)
        Matcher compositeMatcher = PG_COMPOSITE_PATTERN.matcher(trimmedValue);
        if (compositeMatcher.find()) {
            String innerValue = compositeMatcher.group(1);

            // Special case: if inner value is wrapped in braces, remove them
            if (innerValue.startsWith("{") && innerValue.endsWith("}")) {
                innerValue = innerValue.substring(1, innerValue.length() - 1);
            }

            return convertArrayToOracleFormat(innerValue);
        }

        // Handle array format: {value1,value2,...}
        Matcher arrayMatcher = PG_ARRAY_PATTERN.matcher(trimmedValue);
        if (arrayMatcher.find()) {
            String innerValue = arrayMatcher.group(1);
            return convertArrayToOracleFormat(innerValue);
        }

        // Handle single values wrapped in braces: {9988279}
        if (trimmedValue.startsWith("{") && trimmedValue.endsWith("}")) {
            String innerValue = trimmedValue.substring(1, trimmedValue.length() - 1);
            // Check if it's a single value (no commas)
            if (!innerValue.contains(",")) {
                String cleanedValue = cleanIndividualValue(innerValue);
                if (cleanedValue == null) {
                    return "null"; // Return unquoted null
                }
                return "'" + cleanedValue + "'";
            } else {
                // Multiple values, process as array
                return convertArrayToOracleFormat(innerValue);
            }
        }

        // Handle quoted single values: "value"
        if (trimmedValue.startsWith("\"") && trimmedValue.endsWith("\"")) {
            String innerValue = trimmedValue.substring(1, trimmedValue.length() - 1);

            // Check if the quoted content is actually an array
            if (innerValue.startsWith("{") && innerValue.endsWith("}")) {
                innerValue = innerValue.substring(1, innerValue.length() - 1);
                return convertArrayToOracleFormat(innerValue);
            }

            String cleanedValue = cleanIndividualValue(innerValue);
            if (cleanedValue == null) {
                return "null"; // Return unquoted null
            }
            return "'" + cleanedValue + "'";
        }

        // Handle simple value
        String cleanedValue = cleanIndividualValue(trimmedValue);
        if (cleanedValue == null) {
            return "null"; // Return unquoted null
        }
        return "'" + cleanedValue + "'";
    }

    /**
     * Convert comma-separated values to Oracle VARRAY format.
     * Enhanced to handle values that may be wrapped in braces and various PostgreSQL formats.
     * Enhanced to handle NULL values properly.
     */
    private static String convertArrayToOracleFormat(String arrayContent) {
        if (arrayContent == null || arrayContent.trim().isEmpty()) {
            return "";
        }

        String content = arrayContent.trim();

        // Log the input for debugging
        logger.debug("Converting array content: '{}'", content);

        // Handle multiple layers of quotes and braces that PostgreSQL might send
        // Keep cleaning until no more changes
        String previousContent;
        do {
            previousContent = content;

            // Remove outer braces: {value1,value2} -> value1,value2
            if (content.startsWith("{") && content.endsWith("}")) {
                content = content.substring(1, content.length() - 1).trim();
            }

            // Remove outer quotes: "value1,value2" -> value1,value2
            if (content.startsWith("\"") && content.endsWith("\"")) {
                content = content.substring(1, content.length() - 1).trim();
            }

        } while (!content.equals(previousContent)); // Continue until no more changes

        // Now split by comma and process each value
        String[] values = content.split(",");
        StringBuilder result = new StringBuilder();

        for (int i = 0; i < values.length; i++) {
            String value = values[i].trim();

            // Clean each individual value
            String cleanedValue = cleanIndividualValue(value);

            // Handle NULL values - they should appear as null (not quoted)
            if (cleanedValue == null) {
                if (result.length() > 0) {
                    result.append(", ");
                }
                result.append("null"); // No quotes for null
                continue;
            }

            // Skip completely empty values (but not null)
            if (cleanedValue.isEmpty()) {
                continue;
            }

            if (result.length() > 0) {
                result.append(", ");
            }

            result.append("'").append(cleanedValue).append("'");
        }

        logger.debug("Converted array result: '{}'", result.toString());
        return result.toString();
    }

    /**
     * Clean an individual value by removing all PostgreSQL formatting artifacts.
     * Enhanced to handle NULL values properly.
     */
    private static String cleanIndividualValue(String value) {
        if (value == null) {
            return null; // Return null for actual null values
        }

        String cleaned = value.trim();

        // Handle NULL values (case-insensitive)
        if (cleaned.equalsIgnoreCase("null") || cleaned.equalsIgnoreCase("NULL")) {
            return null; // Return null for NULL string values
        }

        // Keep cleaning until no more changes
        String previousCleaned;
        do {
            previousCleaned = cleaned;

            // Remove quotes: "040-1" -> 040-1
            if (cleaned.startsWith("\"") && cleaned.endsWith("\"")) {
                cleaned = cleaned.substring(1, cleaned.length() - 1).trim();
            }

            // Remove braces: {040-1} -> 040-1
            if (cleaned.startsWith("{") && cleaned.endsWith("}")) {
                cleaned = cleaned.substring(1, cleaned.length() - 1).trim();
            }

            // Remove single quotes if they exist: '040-1' -> 040-1
            if (cleaned.startsWith("'") && cleaned.endsWith("'")) {
                cleaned = cleaned.substring(1, cleaned.length() - 1).trim();
            }

            // Check again for NULL after cleaning quotes/braces
            if (cleaned.equalsIgnoreCase("null") || cleaned.equalsIgnoreCase("NULL")) {
                return null;
            }

        } while (!cleaned.equals(previousCleaned));

        return cleaned;
    }

    /**
     * Handle complex PostgreSQL UDT values that contain multiple composite types.
     * Example input: "({576-1})({577-3})({577-2})({530-4})({271-1})({"060-1,060-1,060-1,..."})"
     */
    public static String formatComplexPgUdtForOracle(String udtType, String complexPgValue) {
        if (complexPgValue == null || complexPgValue.trim().isEmpty()) {
            return null;
        }

        logger.debug("Converting complex PostgreSQL UDT: type={}, value={}", udtType, complexPgValue);

        try {
            String oracleUdtType = mapPgTypeToOracleType(udtType);

            // Find all composite patterns in the complex value
            Matcher matcher = PG_COMPOSITE_PATTERN.matcher(complexPgValue);
            StringBuilder allValues = new StringBuilder();

            while (matcher.find()) {
                String innerValue = matcher.group(1);

                if (allValues.length() > 0) {
                    allValues.append(", ");
                }

                // Convert each composite part
                String convertedValue = convertArrayToOracleFormat(innerValue);
                allValues.append(convertedValue);
            }

            if (allValues.length() == 0) {
                // Fallback: treat the entire value as a simple array
                return formatPgUdtForOracle(udtType, complexPgValue);
            }

            String result = oracleUdtType + "(" + allValues.toString() + ")";
            logger.debug("Converted complex UDT: {} -> {}", complexPgValue, result);
            return result;

        } catch (Exception e) {
            logger.error("Error converting complex PostgreSQL UDT value '{}' of type '{}': {}",
                    complexPgValue, udtType, e.getMessage());
            return null;
        }
    }

    /**
     * Handle complex PostgreSQL UDT values using column-specific mapping.
     */
    public static String formatComplexPgUdtForOracleByColumn(String tableName, String columnName,
                                                             String pgUdtType, String complexPgValue,
                                                             Map<String, String> typeMapping) {
        if (complexPgValue == null || complexPgValue.trim().isEmpty()) {
            return null;
        }

        logger.debug("Converting complex PostgreSQL UDT for column {}.{}: pgUdtType={}, value={}",
                tableName, columnName, pgUdtType, complexPgValue);

        try {
            // Get the Oracle UDT type from the type mapping
            String oracleUdtType = typeMapping.get(pgUdtType.toLowerCase());
            if (oracleUdtType == null) {
                logger.warn("No Oracle UDT mapping found for PostgreSQL type: {}", pgUdtType);
                oracleUdtType = pgUdtType.toUpperCase();
            }

            // Add schema prefix if not present
            if (!oracleUdtType.contains(".")) {
                oracleUdtType = "TRANSP." + oracleUdtType;
            }

            // Find all composite patterns in the complex value
            Matcher matcher = PG_COMPOSITE_PATTERN.matcher(complexPgValue);
            StringBuilder allValues = new StringBuilder();

            while (matcher.find()) {
                String innerValue = matcher.group(1);

                if (allValues.length() > 0) {
                    allValues.append(", ");
                }

                // Convert each composite part
                String convertedValue = convertArrayToOracleFormat(innerValue);
                allValues.append(convertedValue);
            }

            if (allValues.length() == 0) {
                // Fallback: treat the entire value as a simple array
                return formatPgUdtForOracleByColumn(tableName, columnName, pgUdtType, complexPgValue, typeMapping);
            }

            String result = oracleUdtType + "(" + allValues.toString() + ")";
            logger.debug("Converted complex UDT for column {}.{}: {} -> {}",
                    tableName, columnName, complexPgValue, result);
            return result;

        } catch (Exception e) {
            logger.error("Error converting complex PostgreSQL UDT value '{}' of type '{}' for column {}.{}: {}",
                    complexPgValue, pgUdtType, tableName, columnName, e.getMessage());
            return null;
        }
    }
}