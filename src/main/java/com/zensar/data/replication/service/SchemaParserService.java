package com.zensar.data.replication.service;

import com.zensar.data.replication.model.FieldTypeInfo;
import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Service for parsing and extracting schema information from Debezium events.
 * Enhanced with improved error handling for different schema structures.
 */
@Service
public class SchemaParserService {
    private static final Logger logger = LoggerFactory.getLogger(SchemaParserService.class);

    /**
     * Build a map of field names to their type information from schema.
     *
     * @param schemaNode JsonNode containing the schema definition
     * @return Map of field names to their type information
     */
    public Map<String, FieldTypeInfo> buildFieldTypeMap(JsonNode schemaNode) {
        Map<String, FieldTypeInfo> fieldTypeMap = new HashMap<>();

        if (schemaNode == null) {
            logger.warn("Schema node is null");
            return fieldTypeMap;
        }

        if (!schemaNode.has("fields")) {
            logger.warn("Missing fields section in schema");

            // Additional handling for edge cases: check if schema itself defines a field structure
            if (schemaNode.has("field") && schemaNode.has("type")) {
                try {
                    String fieldName = schemaNode.get("field").asText();
                    String fieldType = schemaNode.get("type").asText();
                    String typeName = schemaNode.has("name") ? schemaNode.get("name").asText() : null;

                    Map<String, String> parameters = new HashMap<>();
                    if (schemaNode.has("parameters")) {
                        JsonNode paramsNode = schemaNode.get("parameters");
                        Iterator<Map.Entry<String, JsonNode>> paramsFields = paramsNode.fields();
                        while (paramsFields.hasNext()) {
                            Map.Entry<String, JsonNode> param = paramsFields.next();
                            parameters.put(param.getKey(), param.getValue().asText());
                        }
                    }

                    fieldTypeMap.put(fieldName, new FieldTypeInfo(fieldType, typeName, parameters));
                } catch (Exception e) {
                    logger.warn("Error processing single field schema: {}", e.getMessage());
                }
            }

            return fieldTypeMap;
        }

        JsonNode fieldsNode = schemaNode.get("fields");

        // First, try to find the "after" node in schema fields
        for (JsonNode fieldNode : fieldsNode) {
            if (fieldNode.has("field") && fieldNode.get("field").asText().equals("after")) {
                processAfterNode(fieldNode, fieldTypeMap);

                // If we processed the after node and got fields, return them
                if (!fieldTypeMap.isEmpty()) {
                    return fieldTypeMap;
                }

                // Otherwise, try to fall back to processing "before" node if present
                for (JsonNode beforeFieldNode : fieldsNode) {
                    if (beforeFieldNode.has("field") && beforeFieldNode.get("field").asText().equals("before")) {
                        logger.info("Falling back to using 'before' node for schema information");
                        processAfterNode(beforeFieldNode, fieldTypeMap);
                        return fieldTypeMap;
                    }
                }
            }
        }

        // If no "after" or "before" node processed successfully, try to process directly as field definitions
        // This handles cases where the schema might be structured differently
        logger.info("No 'after' node found, trying to process fields directly");
        processFieldsNode(fieldsNode, fieldTypeMap);

        // Log the result for debugging
        if (fieldTypeMap.isEmpty()) {
            logger.warn("No field type information extracted from schema");
        } else {
            logger.info("Extracted {} field types from schema", fieldTypeMap.size());
        }

        return fieldTypeMap;
    }

    /**
     * Process the "after" node in the schema to extract field type information.
     */
    private void processAfterNode(JsonNode afterNode, Map<String, FieldTypeInfo> fieldTypeMap) {
        if (afterNode.has("fields")) {
            JsonNode fieldsInAfter = afterNode.get("fields");
            processFieldsNode(fieldsInAfter, fieldTypeMap);
        } else if (afterNode.has("type") && "struct".equals(afterNode.get("type").asText()) && afterNode.has("schema")) {
            // Handle nested schema case
            JsonNode nestedSchema = afterNode.get("schema");
            if (nestedSchema.has("fields")) {
                processFieldsNode(nestedSchema.get("fields"), fieldTypeMap);
            }
        } else {
            logger.warn("'after' node found but it has no fields or nested schema");
        }
    }

    /**
     * Process fields in a node to extract field type information.
     */
    private void processFieldsNode(JsonNode fieldsNode, Map<String, FieldTypeInfo> fieldTypeMap) {
        if (fieldsNode == null) {
            logger.warn("Fields node is null");
            return;
        }

        if (!fieldsNode.isArray()) {
            logger.warn("Fields node is not an array: {}", fieldsNode.getNodeType());
            return;
        }

        for (JsonNode field : fieldsNode) {
            try {
                if (field.has("field")) {
                    String fieldName = field.get("field").asText();
                    String fieldType = field.has("type") ? field.get("type").asText() : "unknown";
                    String typeName = field.has("name") ? field.get("name").asText() : null;

                    Map<String, String> parameters = new HashMap<>();
                    if (field.has("parameters")) {
                        JsonNode paramsNode = field.get("parameters");
                        Iterator<Map.Entry<String, JsonNode>> paramsFields = paramsNode.fields();
                        while (paramsFields.hasNext()) {
                            Map.Entry<String, JsonNode> param = paramsFields.next();
                            parameters.put(param.getKey(), param.getValue().asText());
                        }
                    }

                    fieldTypeMap.put(fieldName, new FieldTypeInfo(fieldType, typeName, parameters));
                    logger.debug("Added field type for '{}': type={}, typeName={}, parameters={}",
                            fieldName, fieldType, typeName, parameters);
                }
            } catch (Exception e) {
                logger.warn("Error processing field node: {}", e.getMessage());
            }
        }
    }
}