package com.zensar.data.replication.model;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Represents a CDC event from Debezium.
 */
public class CdcEvent {
    private final String topic;
    private final String tableName;
    private final String operation;
    private final JsonNode beforeNode;
    private final JsonNode afterNode;
    private final JsonNode sourceNode;

    public CdcEvent(String topic, String tableName, String operation,
                    JsonNode beforeNode, JsonNode afterNode, JsonNode sourceNode) {
        this.topic = topic;
        this.tableName = tableName;
        this.operation = operation;
        this.beforeNode = beforeNode;
        this.afterNode = afterNode;
        this.sourceNode = sourceNode;
    }

    public String getTopic() {
        return topic;
    }

    public String getTableName() {
        return tableName;
    }

    public String getOperation() {
        return operation;
    }

    public JsonNode getBeforeNode() {
        return beforeNode;
    }

    public JsonNode getAfterNode() {
        return afterNode;
    }

    public JsonNode getSourceNode() {
        return sourceNode;
    }

    /**
     * Check if this event is an insert/create operation.
     * @return true if this is an insert operation
     */
    public boolean isInsert() {
        return "c".equals(operation);
    }

    /**
     * Check if this event is a read operation (from snapshot).
     * @return true if this is a read operation
     */
    public boolean isRead() {
        return "r".equals(operation);
    }

    /**
     * Check if this event is an update operation.
     * @return true if this is an update operation
     */
    public boolean isUpdate() {
        return "u".equals(operation);
    }

    /**
     * Check if this event is a delete operation.
     * @return true if this is a delete operation
     */
    public boolean isDelete() {
        return "d".equals(operation);
    }

    @Override
    public String toString() {
        return "CdcEvent{" +
                "topic='" + topic + '\'' +
                ", tableName='" + tableName + '\'' +
                ", operation='" + operation + '\'' +
                '}';
    }
}