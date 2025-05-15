package com.zensar.data.replication.model;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Class to represent field type information from the Debezium schema.
 */
public class FieldTypeInfo {
    private final String type;
    private final String typeName;
    private final Map<String, String> parameters;

    public FieldTypeInfo(String type, String typeName, Map<String, String> parameters) {
        this.type = type;
        this.typeName = typeName;
        this.parameters = parameters != null ? new HashMap<>(parameters) : Collections.emptyMap();
    }

    public String getType() {
        return type;
    }

    public String getTypeName() {
        return typeName;
    }

    public Map<String, String> getParameters() {
        return Collections.unmodifiableMap(parameters);
    }

    /**
     * Get a parameter value with default fallback
     * @param paramName Parameter name
     * @param defaultValue Default value if parameter is not found
     * @return Parameter value or default
     */
    public String getParameter(String paramName, String defaultValue) {
        return parameters.getOrDefault(paramName, defaultValue);
    }

    /**
     * Get a parameter as integer with default fallback
     * @param paramName Parameter name
     * @param defaultValue Default value if parameter is not found or invalid
     * @return Parameter value as integer or default
     */
    public int getParameterAsInt(String paramName, int defaultValue) {
        try {
            String value = parameters.get(paramName);
            return value != null ? Integer.parseInt(value) : defaultValue;
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    /**
     * Check if this field is a timestamp type
     * @return true if field is a timestamp
     */
    public boolean isTimestamp() {
        return typeName != null && typeName.equals("io.debezium.time.Timestamp");
    }

    /**
     * Check if this field is a decimal type
     * @return true if field is a decimal
     */
    public boolean isDecimal() {
        return "bytes".equals(type) && typeName != null &&
                typeName.equals("org.apache.kafka.connect.data.Decimal");
    }

    @Override
    public String toString() {
        return "FieldTypeInfo{" +
                "type='" + type + '\'' +
                ", typeName='" + typeName + '\'' +
                ", parameters=" + parameters +
                '}';
    }
}