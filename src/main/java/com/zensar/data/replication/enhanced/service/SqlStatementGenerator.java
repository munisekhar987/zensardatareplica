package com.zensar.data.replication.enhanced.service;

import com.zensar.data.replication.model.CdcEvent;
import com.zensar.data.replication.model.FieldTypeInfo;
import com.zensar.data.replication.service.CdcValueExtractorService;
import com.zensar.data.replication.service.NoPrimaryKeyTableService;
import com.zensar.data.replication.service.SqlExecutionService;
import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.*;

/**
 * SQL Statement Generator that uses the exact same logic as SqlExecutionService
 * but ONLY creates SQL statements without executing them.
 */
@Service
public class SqlStatementGenerator {
    private static final Logger logger = LoggerFactory.getLogger(SqlStatementGenerator.class);

    @Value("${oracle.db.schema:}")
    private String dbSchema;

    @Autowired
    private CdcValueExtractorService valueExtractorService;

    @Autowired
    private NoPrimaryKeyTableService noPrimaryKeyTableService;

    @Autowired
    private SqlExecutionService sqlExecutionService;

    public PreparedSqlStatement generateStatement(CdcEvent event, Map<String, FieldTypeInfo> fieldTypeMap) {
        try {
            String tableName = event.getTableName().toUpperCase();

            if (event.isInsert() || event.isRead()) {
                return generateInsertStatement(event, fieldTypeMap);
            } else if (event.isUpdate()) {
                return generateUpdateStatement(event, fieldTypeMap);
            } else if (event.isDelete()) {
                return generateDeleteStatement(event, fieldTypeMap);
            } else {
                logger.warn("Unknown operation type: {} for table: {}", event.getOperation(), tableName);
                return null;
            }

        } catch (Exception e) {
            logger.error("Error generating SQL statement for table {}: {}",
                    event.getTableName(), e.getMessage(), e);
            return null;
        }
    }

    private PreparedSqlStatement generateInsertStatement(CdcEvent event, Map<String, FieldTypeInfo> fieldTypeMap) {
        String tableName = event.getTableName().toUpperCase();

        if (noPrimaryKeyTableService.isTableWithoutPrimaryKey(tableName)) {
            logger.debug("Table {} allows duplicates, generating direct insert", tableName);
            return generateDirectInsert(event, fieldTypeMap);
        }

        Map<String, Object> fieldValues = sqlExecutionService.extractFieldValues(event.getAfterNode(), fieldTypeMap);

        if (fieldValues.isEmpty()) {
            logger.warn("No valid fields found for INSERT operation on {}", tableName);
            return null;
        }

        List<String> primaryKeyFields = sqlExecutionService.getPrimaryKeyFields(tableName);
        if (primaryKeyFields.isEmpty()) {
            logger.debug("No primary key found for table {}, generating direct insert", tableName);
            return generateDirectInsert(event, fieldTypeMap);
        }

        return generateMergeStatement(tableName, fieldValues, primaryKeyFields);
    }

    private PreparedSqlStatement generateUpdateStatement(CdcEvent event, Map<String, FieldTypeInfo> fieldTypeMap) {
        String tableName = event.getTableName().toUpperCase();

        if (noPrimaryKeyTableService.isTableWithoutPrimaryKey(tableName)) {
            return generateUpdateWithAllFields(event, fieldTypeMap);
        }

        Map<String, Object> fieldValues = sqlExecutionService.extractFieldValues(event.getAfterNode(), fieldTypeMap);

        if (fieldValues.isEmpty()) {
            return null;
        }

        List<String> primaryKeyFields = sqlExecutionService.getPrimaryKeyFields(tableName);
        if (primaryKeyFields.isEmpty()) {
            return null;
        }

        Map<String, Object> updateValues = new HashMap<>();
        Map<String, Object> keyValues = new HashMap<>();

        for (Map.Entry<String, Object> entry : fieldValues.entrySet()) {
            boolean isPrimaryKey = false;
            for (String pkField : primaryKeyFields) {
                if (entry.getKey().equalsIgnoreCase(pkField)) {
                    keyValues.put(entry.getKey().toUpperCase(), entry.getValue());
                    isPrimaryKey = true;
                    break;
                }
            }
            if (!isPrimaryKey) {
                updateValues.put(entry.getKey().toUpperCase(), entry.getValue());
            }
        }

        if (updateValues.isEmpty()) {
            return null;
        }

        return generateUpdateWithPrimaryKeys(tableName, updateValues, keyValues);
    }

    private PreparedSqlStatement generateDeleteStatement(CdcEvent event, Map<String, FieldTypeInfo> fieldTypeMap) {
        String tableName = event.getTableName().toUpperCase();

        if (noPrimaryKeyTableService.isTableWithoutPrimaryKey(tableName)) {
            return generateDeleteWithAllFields(event, fieldTypeMap);
        }

        List<String> primaryKeyFields = sqlExecutionService.getPrimaryKeyFields(tableName);
        if (primaryKeyFields.isEmpty()) {
            return null;
        }

        Map<String, Object> primaryKeyValues = new HashMap<>();
        for (String pkField : primaryKeyFields) {
            JsonNode pkNode = null;

            Iterator<Map.Entry<String, JsonNode>> fields = event.getBeforeNode().fields();
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> field = fields.next();
                if (field.getKey().equalsIgnoreCase(pkField)) {
                    pkNode = field.getValue();
                    break;
                }
            }

            if (pkNode == null) {
                return null;
            }

            FieldTypeInfo keyTypeInfo = null;
            for (Map.Entry<String, FieldTypeInfo> entry : fieldTypeMap.entrySet()) {
                if (entry.getKey().equalsIgnoreCase(pkField)) {
                    keyTypeInfo = entry.getValue();
                    break;
                }
            }

            if (keyTypeInfo == null) {
                keyTypeInfo = new FieldTypeInfo("unknown", null, Collections.emptyMap());
            }

            Object pkValue = valueExtractorService.extractValue(pkNode, pkField, keyTypeInfo);

            if (pkValue == null) {
                return null;
            }
            primaryKeyValues.put(pkField.toUpperCase(), pkValue);
        }

        return generateDeleteWithPrimaryKeys(tableName, primaryKeyValues);
    }

    private PreparedSqlStatement generateDirectInsert(CdcEvent event, Map<String, FieldTypeInfo> fieldTypeMap) {
        String tableName = event.getTableName().toUpperCase();
        Map<String, Object> fieldValues = sqlExecutionService.extractFieldValues(event.getAfterNode(), fieldTypeMap);

        if (fieldValues.isEmpty()) {
            return null;
        }

        StringBuilder sql = new StringBuilder();
        List<Object> parameters = new ArrayList<>();

        sql.append("INSERT INTO ").append(dbSchema.toUpperCase()).append(".").append(tableName).append(" (");

        List<String> columns = new ArrayList<>(fieldValues.keySet());
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) sql.append(", ");
            sql.append(columns.get(i).toUpperCase());
        }

        sql.append(") VALUES (");

        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) sql.append(", ");
            sql.append("?");
            parameters.add(fieldValues.get(columns.get(i)));
        }

        sql.append(")");

        return new PreparedSqlStatement(PreparedSqlStatement.StatementType.INSERT, sql.toString(), parameters, tableName, event.getOperation());
    }

    private PreparedSqlStatement generateMergeStatement(String tableName, Map<String, Object> fieldValues, List<String> primaryKeyFields) {
        StringBuilder sql = new StringBuilder();
        List<Object> parameters = new ArrayList<>();

        sql.append("MERGE INTO ").append(dbSchema.toUpperCase()).append(".").append(tableName).append(" target ");
        sql.append("USING (SELECT ");

        List<String> allFields = new ArrayList<>(fieldValues.keySet());
        for (int i = 0; i < allFields.size(); i++) {
            if (i > 0) sql.append(", ");
            sql.append("? AS ").append(allFields.get(i));
            parameters.add(fieldValues.get(allFields.get(i)));
        }

        sql.append(" FROM DUAL) source ON (");

        for (int i = 0; i < primaryKeyFields.size(); i++) {
            if (i > 0) sql.append(" AND ");
            String pkField = primaryKeyFields.get(i).toUpperCase();
            sql.append("target.").append(pkField).append(" = source.").append(pkField);
        }

        sql.append(") WHEN MATCHED THEN UPDATE SET ");

        boolean first = true;
        for (String field : allFields) {
            boolean isPrimaryKey = false;
            for (String pkField : primaryKeyFields) {
                if (field.equalsIgnoreCase(pkField)) {
                    isPrimaryKey = true;
                    break;
                }
            }
            if (!isPrimaryKey) {
                if (!first) sql.append(", ");
                sql.append("target.").append(field.toUpperCase()).append(" = source.").append(field.toUpperCase());
                first = false;
            }
        }

        sql.append(" WHEN NOT MATCHED THEN INSERT (");

        for (int i = 0; i < allFields.size(); i++) {
            if (i > 0) sql.append(", ");
            sql.append(allFields.get(i).toUpperCase());
        }

        sql.append(") VALUES (");

        for (int i = 0; i < allFields.size(); i++) {
            if (i > 0) sql.append(", ");
            sql.append("source.").append(allFields.get(i).toUpperCase());
        }

        sql.append(")");

        return new PreparedSqlStatement(PreparedSqlStatement.StatementType.MERGE, sql.toString(), parameters, tableName, "c");
    }

    private PreparedSqlStatement generateUpdateWithPrimaryKeys(String tableName, Map<String, Object> updateValues, Map<String, Object> keyValues) {
        StringBuilder sql = new StringBuilder();
        List<Object> parameters = new ArrayList<>();

        sql.append("UPDATE ").append(dbSchema.toUpperCase()).append(".").append(tableName).append(" SET ");

        boolean first = true;
        for (Map.Entry<String, Object> entry : updateValues.entrySet()) {
            if (!first) sql.append(", ");
            sql.append(entry.getKey().toUpperCase()).append(" = ?");
            parameters.add(entry.getValue());
            first = false;
        }

        sql.append(" WHERE ");

        first = true;
        for (Map.Entry<String, Object> entry : keyValues.entrySet()) {
            if (!first) sql.append(" AND ");
            sql.append(entry.getKey().toUpperCase()).append(" = ?");
            parameters.add(entry.getValue());
            first = false;
        }

        return new PreparedSqlStatement(PreparedSqlStatement.StatementType.UPDATE, sql.toString(), parameters, tableName, "u");
    }

    private PreparedSqlStatement generateDeleteWithPrimaryKeys(String tableName, Map<String, Object> primaryKeyValues) {
        StringBuilder sql = new StringBuilder();
        List<Object> parameters = new ArrayList<>();

        sql.append("DELETE FROM ").append(dbSchema.toUpperCase()).append(".").append(tableName).append(" WHERE ");

        boolean first = true;
        for (Map.Entry<String, Object> entry : primaryKeyValues.entrySet()) {
            if (!first) sql.append(" AND ");
            sql.append(entry.getKey().toUpperCase()).append(" = ?");
            parameters.add(entry.getValue());
            first = false;
        }

        return new PreparedSqlStatement(PreparedSqlStatement.StatementType.DELETE, sql.toString(), parameters, tableName, "d");
    }

    private PreparedSqlStatement generateUpdateWithAllFields(CdcEvent event, Map<String, FieldTypeInfo> fieldTypeMap) {
        String tableName = event.getTableName().toUpperCase();

        Map<String, Object> afterValues = sqlExecutionService.extractFieldValues(event.getAfterNode(), fieldTypeMap);
        Map<String, Object> beforeValues = sqlExecutionService.extractFieldValues(event.getBeforeNode(), fieldTypeMap);

        if (afterValues.isEmpty() || beforeValues.isEmpty()) {
            return null;
        }

        StringBuilder sql = new StringBuilder();
        List<Object> parameters = new ArrayList<>();

        sql.append("UPDATE ").append(dbSchema.toUpperCase()).append(".").append(tableName).append(" SET ");

        boolean first = true;
        for (Map.Entry<String, Object> entry : afterValues.entrySet()) {
            if (!first) sql.append(", ");
            sql.append(entry.getKey().toUpperCase()).append(" = ?");
            parameters.add(entry.getValue());
            first = false;
        }

        sql.append(" WHERE ");

        first = true;
        for (Map.Entry<String, Object> entry : beforeValues.entrySet()) {
            if (!first) sql.append(" AND ");

            Object value = entry.getValue();
            if (value == null) {
                sql.append(entry.getKey().toUpperCase()).append(" IS NULL");
            } else {
                sql.append(entry.getKey().toUpperCase()).append(" = ?");
                parameters.add(value);
            }
            first = false;
        }

        return new PreparedSqlStatement(PreparedSqlStatement.StatementType.UPDATE, sql.toString(), parameters, tableName, "u");
    }

    private PreparedSqlStatement generateDeleteWithAllFields(CdcEvent event, Map<String, FieldTypeInfo> fieldTypeMap) {
        String tableName = event.getTableName().toUpperCase();

        if (event.getBeforeNode() == null) {
            return null;
        }

        Map<String, Object> beforeValues = sqlExecutionService.extractFieldValues(event.getBeforeNode(), fieldTypeMap);

        if (beforeValues.isEmpty()) {
            return null;
        }

        StringBuilder sql = new StringBuilder();
        List<Object> parameters = new ArrayList<>();

        sql.append("DELETE FROM ").append(dbSchema.toUpperCase()).append(".").append(tableName).append(" WHERE ");

        boolean first = true;
        for (Map.Entry<String, Object> entry : beforeValues.entrySet()) {
            if (!first) sql.append(" AND ");

            Object value = entry.getValue();
            if (value == null) {
                sql.append(entry.getKey().toUpperCase()).append(" IS NULL");
            } else {
                sql.append(entry.getKey().toUpperCase()).append(" = ?");
                parameters.add(value);
            }
            first = false;
        }

        return new PreparedSqlStatement(PreparedSqlStatement.StatementType.DELETE, sql.toString(), parameters, tableName, "d");
    }

    public static class PreparedSqlStatement {
        public enum StatementType {
            INSERT, UPDATE, DELETE, MERGE
        }

        private final StatementType type;
        private final String sql;
        private final List<Object> parameters;
        private final String tableName;
        private final String operation;

        public PreparedSqlStatement(StatementType type, String sql, List<Object> parameters, String tableName, String operation) {
            this.type = type;
            this.sql = sql;
            this.parameters = parameters;
            this.tableName = tableName;
            this.operation = operation;
        }

        public StatementType getType() { return type; }
        public String getSql() { return sql; }
        public List<Object> getParameters() { return parameters; }
        public String getTableName() { return tableName; }
        public String getOperation() { return operation; }

        @Override
        public String toString() {
            return String.format("PreparedSqlStatement{type=%s, table=%s, operation=%s}",
                    type, tableName, operation);
        }
    }
}