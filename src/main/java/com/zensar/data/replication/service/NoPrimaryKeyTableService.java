package com.zensar.data.replication.service;

import com.zensar.data.replication.model.CdcEvent;
import com.zensar.data.replication.model.FieldTypeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.*;

/**
 * Service for handling SQL operations on tables without primary keys (tables that allow duplicates).
 * This service is used by SqlExecutionService when operations are performed on tables
 * that are configured to allow duplicates.
 * Modified to break circular dependency with SqlExecutionService.
 */
@Service
public class NoPrimaryKeyTableService {
    private static final Logger logger = LoggerFactory.getLogger(NoPrimaryKeyTableService.class);

    @Value("${oracle.db.schema:}")
    private String dbSchema;

    @Value("${cdc.tables.allow-duplicates:}")
    private String allowDuplicatesTables;

    @Value("${oracle.db.url}")
    private String dbUrl;

    @Value("${oracle.db.username}")
    private String dbUsername;

    @Value("${oracle.db.password}")
    private String dbPassword;

    // Set of tables that allow duplicates (no primary key check)
    private final Set<String> duplicatesAllowedTables = new HashSet<>();

    @Autowired
    private CdcValueExtractorService valueExtractorService;

    // Use ApplicationContext to resolve SqlExecutionService lazily to break circular dependency
    @Autowired
    private ApplicationContext applicationContext;

    // Lazy reference to SqlExecutionService
    private SqlExecutionService sqlExecutionService;

    @PostConstruct
    public void init() {
        if (allowDuplicatesTables != null && !allowDuplicatesTables.isEmpty()) {
            String[] tables = allowDuplicatesTables.split(",");
            for (String table : tables) {
                duplicatesAllowedTables.add(table.trim().toUpperCase());
            }
            logger.info("Tables configured to allow duplicates: {}", duplicatesAllowedTables);
        }
    }

    /**
     * Get SqlExecutionService lazily to break circular dependency
     */
    private SqlExecutionService getSqlExecutionService() {
        if (sqlExecutionService == null) {
            sqlExecutionService = applicationContext.getBean(SqlExecutionService.class);
        }
        return sqlExecutionService;
    }

    /**
     * Check if a table is configured to allow duplicates (no primary key check)
     * @param tableName the table name to check
     * @return true if the table allows duplicates, false otherwise
     */
    public boolean isTableWithoutPrimaryKey(String tableName) {
        return duplicatesAllowedTables.contains(tableName.toUpperCase());
    }

    /**
     * Execute direct insert operation without primary key check
     * @param event the CDC event
     * @param fieldTypeMap the field type map
     */
    public void executeDirectInsert(CdcEvent event, Map<String, FieldTypeInfo> fieldTypeMap) {
        String tableName = event.getTableName().toUpperCase();

        // Extract field values from the afterNode for the insert values
        Map<String, Object> fieldValues = getSqlExecutionService().extractFieldValues(event.getAfterNode(), fieldTypeMap);
        if (fieldValues.isEmpty()) {
            logger.warn("No valid fields found for INSERT operation on {}", tableName);
            return;
        }

        logger.info("Table {} is configured to allow duplicates. Performing direct insert.", tableName);
        getSqlExecutionService().executeRegularInsert(tableName, fieldValues);
    }

    /**
     * Execute update operation using all fields from beforeNode as WHERE conditions
     * @param event the CDC event
     * @param fieldTypeMap the field type map
     */
    public void executeUpdateWithAllFields(CdcEvent event, Map<String, FieldTypeInfo> fieldTypeMap) {
        String tableName = event.getTableName().toUpperCase();

        // Extract field values from the afterNode for the SET clause
        Map<String, Object> afterValues = getSqlExecutionService().extractFieldValues(event.getAfterNode(), fieldTypeMap);
        if (afterValues.isEmpty()) {
            logger.warn("No valid fields found in afterNode for UPDATE operation on {}", tableName);
            return;
        }

        // Extract field values from the beforeNode for the WHERE clause
        Map<String, Object> beforeValues = getSqlExecutionService().extractFieldValues(event.getBeforeNode(), fieldTypeMap);
        if (beforeValues.isEmpty()) {
            logger.warn("No valid fields found in beforeNode for UPDATE operation on {}", tableName);
            return;
        }

        logger.info("Executing UPDATE with all fields for table {} without primary key", tableName);

        // Build SET clause and WHERE clause
        StringBuilder setClause = new StringBuilder();
        StringBuilder whereClause = new StringBuilder();
        List<Object> params = new ArrayList<>();

        // Build SET clause with all fields from afterValues
        for (Map.Entry<String, Object> entry : afterValues.entrySet()) {
            if (setClause.length() > 0) {
                setClause.append(", ");
            }
            setClause.append(entry.getKey().toUpperCase()).append(" = ?");
            params.add(entry.getValue());
        }

        // Build WHERE clause with all fields from beforeValues
        for (Map.Entry<String, Object> entry : beforeValues.entrySet()) {
            if (whereClause.length() > 0) {
                whereClause.append(" AND ");
            }

            Object value = entry.getValue();
            if (value == null) {
                whereClause.append(entry.getKey().toUpperCase()).append(" IS NULL");
            } else {
                whereClause.append(entry.getKey().toUpperCase()).append(" = ?");
                params.add(value);
            }
        }

        if (setClause.length() == 0) {
            logger.warn("No fields to update for table {}", tableName);
            return;
        }

        if (whereClause.length() == 0) {
            logger.warn("No conditions for UPDATE operation on table {}", tableName);
            return;
        }

        String sql = String.format(
                "UPDATE %s.%s SET %s WHERE %s",
                dbSchema.toUpperCase(),
                tableName.toUpperCase(),
                setClause,
                whereClause
        );

        logger.info("UPDATE with all fields SQL: {}", sql);
        logger.info("SQL with values: {}", getSqlExecutionService().constructDebugSql(sql, params));

        getSqlExecutionService().executeStatement(sql, params);
    }

    /**
     * Execute delete operation using all fields from beforeNode as WHERE conditions
     * @param event the CDC event
     * @param fieldTypeMap the field type map
     */
    public void executeDeleteWithAllFields(CdcEvent event, Map<String, FieldTypeInfo> fieldTypeMap) {
        String tableName = event.getTableName().toUpperCase();

        if (event.getBeforeNode() == null) {
            logger.warn("BeforeNode is null for DELETE operation on table {} without primary key", tableName);
            return;
        }

        // Extract field values from the beforeNode for the WHERE clause
        Map<String, Object> beforeValues = getSqlExecutionService().extractFieldValues(event.getBeforeNode(), fieldTypeMap);
        if (beforeValues.isEmpty()) {
            logger.warn("No valid fields found in beforeNode for DELETE operation on {}", tableName);
            return;
        }

        logger.info("Executing DELETE with all fields for table {} without primary key", tableName);

        // Build WHERE clause
        StringBuilder whereClause = new StringBuilder();
        List<Object> params = new ArrayList<>();

        // Build WHERE clause with all fields from beforeValues
        for (Map.Entry<String, Object> entry : beforeValues.entrySet()) {
            if (whereClause.length() > 0) {
                whereClause.append(" AND ");
            }

            Object value = entry.getValue();
            if (value == null) {
                whereClause.append(entry.getKey().toUpperCase()).append(" IS NULL");
            } else {
                whereClause.append(entry.getKey().toUpperCase()).append(" = ?");
                params.add(value);
            }
        }

        if (whereClause.length() == 0) {
            logger.warn("No conditions for DELETE operation on table {}", tableName);
            return;
        }

        String sql = String.format(
                "DELETE FROM %s.%s WHERE %s",
                dbSchema.toUpperCase(),
                tableName.toUpperCase(),
                whereClause
        );

        logger.info("DELETE with all fields SQL: {}", sql);
        logger.info("SQL with values: {}", getSqlExecutionService().constructDebugSql(sql, params));

        getSqlExecutionService().executeStatement(sql, params);
    }
}