package com.zensar.data.replication.config;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import javax.sql.DataSource;

/**
 * Database Configuration for Oracle Connection Pool Only
 * PostgreSQL connections are handled directly via DriverManager when needed
 */
@Configuration
public class DatabaseConfiguration {

    // Oracle Database Configuration
    @Value("${oracle.db.url}")
    private String oracleUrl;

    @Value("${oracle.db.username}")
    private String oracleUsername;

    @Value("${oracle.db.password}")
    private String oraclePassword;

    // Connection Pool Configuration
    @Value("${cdc.enhanced.db.pool.initial-size:2}")
    private int initialSize;

    @Value("${cdc.enhanced.db.pool.max-total:6}")
    private int maxTotal;

    @Value("${cdc.enhanced.db.pool.max-idle:4}")
    private int maxIdle;

    @Value("${cdc.enhanced.db.pool.min-idle:2}")
    private int minIdle;

    @Value("${cdc.enhanced.db.pool.max-wait-millis:30000}")
    private long maxWaitMillis;

    @Value("${cdc.enhanced.db.pool.validation-query:SELECT 1 FROM DUAL}")
    private String validationQuery;

    @Value("${cdc.enhanced.db.pool.test-on-borrow:true}")
    private boolean testOnBorrow;

    @Value("${cdc.enhanced.db.pool.test-while-idle:true}")
    private boolean testWhileIdle;

    /**
     * Primary DataSource bean for Oracle database with HikariCP connection pool
     * This is used by the WorkerPoolService for executing CDC operations
     */
    @Bean
    @Primary
    public DataSource dataSource() {
        HikariConfig config = new HikariConfig();

        // Basic connection settings
        config.setJdbcUrl(oracleUrl);
        config.setUsername(oracleUsername);
        config.setPassword(oraclePassword);
        config.setDriverClassName("oracle.jdbc.OracleDriver");

        // Connection pool settings
        config.setMinimumIdle(minIdle);
        config.setMaximumPoolSize(maxTotal);
        config.setConnectionTimeout(maxWaitMillis);
        config.setIdleTimeout(600000); // 10 minutes
        config.setMaxLifetime(1800000); // 30 minutes
        config.setLeakDetectionThreshold(60000); // 1 minute

        // Validation settings
        config.setConnectionTestQuery(validationQuery);
        config.setValidationTimeout(5000);

        // Pool name for monitoring
        config.setPoolName("Enhanced-CDC-Oracle-Pool");

        // Enable JMX monitoring
        config.setRegisterMbeans(true);

        return new HikariDataSource(config);
    }
}