package com.zensar.data.replication.service;

import com.zensar.data.replication.model.FieldTypeInfo;
import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Base64;

/**
 * Service for extracting and parsing values from Debezium CDC events based on schema information.
 */
@Service
public class CdcValueExtractorService {
    private static final Logger logger = LoggerFactory.getLogger(CdcValueExtractorService.class);

    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    /**
     * Extract value from JsonNode based on field type info from schema
     *
     * @param fieldValue JsonNode containing the field value
     * @param fieldName Name of the field
     * @param typeInfo Type information for the field
     * @return Properly typed Java object
     */
    public Object extractValue(JsonNode fieldValue, String fieldName, FieldTypeInfo typeInfo) {
        if (fieldValue == null || fieldValue.isNull()) {
            return null;
        }

        // Handle timestamps
        if (typeInfo.isTimestamp()) {
            return extractTimestamp(fieldValue, fieldName, typeInfo);
        }
        // Handle decimal types
        else if (typeInfo.isDecimal()) {
            return extractDecimal(fieldValue, fieldName, typeInfo);
        }
        // Handle variable scale decimal types
        else if (typeInfo.isVariableScaleDecimal()) {
            return extractVariableScaleDecimal(fieldValue, fieldName);
        }
        // Handle basic types
        else {
            return extractBasicType(fieldValue, typeInfo.getType());
        }
    }

    /**
     * Extract timestamp value with proper handling for different timestamp types
     */
    private Object extractTimestamp(JsonNode fieldValue, String fieldName, FieldTypeInfo typeInfo) {
        try {
            long timestamp = fieldValue.asLong();
            Timestamp sqlTimestamp;

            if (typeInfo.isMicroTimestamp()) {
                // Convert microseconds to milliseconds
                sqlTimestamp = new Timestamp(timestamp / 1000);
                logger.debug("Converted micro timestamp for field '{}': {} microseconds -> {} ({})",
                        fieldName, timestamp, sqlTimestamp, DATE_FORMAT.format(sqlTimestamp));
            } else if (typeInfo.isNanoTimestamp()) {
                // Convert nanoseconds to milliseconds
                sqlTimestamp = new Timestamp(timestamp / 1_000_000);
                logger.debug("Converted nano timestamp for field '{}': {} nanoseconds -> {} ({})",
                        fieldName, timestamp, sqlTimestamp, DATE_FORMAT.format(sqlTimestamp));
            } else {
                // Default: assume milliseconds (io.debezium.time.Timestamp)
                sqlTimestamp = new Timestamp(timestamp);
                logger.debug("Converted timestamp for field '{}': {} milliseconds -> {} ({})",
                        fieldName, timestamp, sqlTimestamp, DATE_FORMAT.format(sqlTimestamp));
            }

            return sqlTimestamp;
        } catch (Exception e) {
            logger.warn("Error parsing timestamp for field {}: {}", fieldName, e.getMessage());
            return null;
        }
    }

    /**
     * Extract decimal value from Base64 encoded bytes
     */
    private BigDecimal extractDecimal(JsonNode fieldValue, String fieldName, FieldTypeInfo typeInfo) {
        try {
            if (fieldValue.isTextual()) {
                // Decode Base64 and convert to BigDecimal
                byte[] decodedBytes = Base64.getDecoder().decode(fieldValue.asText());
                BigDecimal value = new BigDecimal(new BigInteger(decodedBytes));

                // Apply scale from schema parameters
                int scale = typeInfo.getParameterAsInt("scale", 0);
                if (scale > 0) {
                    value = value.divide(BigDecimal.TEN.pow(scale));
                }

                logger.debug("Converted decimal for field '{}': {} -> {} (scale={})",
                        fieldName, fieldValue.asText(), value, scale);

                return value;
            }
            return null;
        } catch (Exception e) {
            logger.warn("Error parsing decimal for field {}: {}", fieldName, e.getMessage());
            return null;
        }
    }

    /**
     * Extract variable scale decimal value from struct
     */
    private BigDecimal extractVariableScaleDecimal(JsonNode fieldValue, String fieldName) {
        try {
            if (fieldValue.isObject() && fieldValue.has("scale") && fieldValue.has("value")) {
                int scale = fieldValue.get("scale").asInt();
                String base64Value = fieldValue.get("value").asText();

                // Decode Base64 and convert to BigDecimal
                byte[] decodedBytes = Base64.getDecoder().decode(base64Value);
                BigDecimal value = new BigDecimal(new BigInteger(decodedBytes));

                // Apply scale
                if (scale > 0) {
                    value = value.divide(BigDecimal.TEN.pow(scale));
                }

                logger.debug("Converted variable scale decimal for field '{}': scale={}, value={} -> {}",
                        fieldName, scale, base64Value, value);

                return value;
            }
            return null;
        } catch (Exception e) {
            logger.warn("Error parsing variable scale decimal for field {}: {}", fieldName, e.getMessage());
            return null;
        }
    }

    /**
     * Extract basic type value based on schema type
     */
    private Object extractBasicType(JsonNode fieldValue, String baseType) {
        Object result = null;

        // String type
        if ("string".equals(baseType) && fieldValue.isTextual()) {
            result = fieldValue.asText();
        }
        // Integer types
        else if (("int32".equals(baseType) || "int".equals(baseType)) && fieldValue.isNumber()) {
            result = fieldValue.asInt();
        }
        else if ("int16".equals(baseType) && fieldValue.isNumber()) {
            result = fieldValue.asInt(); // Oracle handles this as INT
        }
        // Long type
        else if (("int64".equals(baseType) || "long".equals(baseType)) && fieldValue.isNumber()) {
            result = fieldValue.asLong();
        }
        // Double type
        else if (("float64".equals(baseType) || "double".equals(baseType)) && fieldValue.isDouble()) {
            result = fieldValue.asDouble();
        }
        // Boolean type
        else if ("boolean".equals(baseType) && fieldValue.isBoolean()) {
            result = fieldValue.asBoolean();
        }
        // Fallback: Try to convert as best we can
        else if (("int32".equals(baseType) || "int".equals(baseType)) && fieldValue.isNumber()) {
            result = fieldValue.asInt();
        }
        else if (("int64".equals(baseType) || "long".equals(baseType)) && fieldValue.isNumber()) {
            result = fieldValue.asLong();
        }
        else if (("float64".equals(baseType) || "double".equals(baseType)) && fieldValue.isNumber()) {
            result = fieldValue.asDouble();
        }
        // Fallback: Determine type from JsonNode
        else {
            result = inferTypeFromJsonNode(fieldValue);
        }

        logger.debug("Converted basic type for field type '{}': {} -> {}", baseType, fieldValue, result);
        return result;
    }

    /**
     * Infer type from JsonNode when schema type is unknown or doesn't match
     */
    private Object inferTypeFromJsonNode(JsonNode fieldValue) {
        if (fieldValue.isTextual()) {
            return fieldValue.asText();
        } else if (fieldValue.isInt()) {
            return fieldValue.asInt();
        } else if (fieldValue.isLong()) {
            return fieldValue.asLong();
        } else if (fieldValue.isDouble()) {
            return fieldValue.asDouble();
        } else if (fieldValue.isBoolean()) {
            return fieldValue.asBoolean();
        } else if (fieldValue.isNumber()) {
            // Check if it's a decimal
            try {
                return fieldValue.decimalValue();
            } catch (Exception e) {
                // If it fails, just use asDouble as a fallback
                return fieldValue.asDouble();
            }
        } else {
            return fieldValue.toString();
        }
    }
}