package com.zensar.data.replication.model;

/**
 * Class to represent a mapping between a Kafka topic and the target database table.
 */
public class TopicTableMapping {
    private final String topic;
    private final String tableName;

    public TopicTableMapping(String topic, String tableName) {
        this.topic = topic;
        this.tableName = tableName != null ? tableName.toUpperCase() : null;
    }

    public String getTopic() {
        return topic;
    }

    public String getTableName() {
        return tableName;
    }

    @Override
    public String toString() {
        return "TopicTableMapping{" +
                "topic='" + topic + '\'' +
                ", tableName='" + tableName + '\'' +
                '}';
    }
}