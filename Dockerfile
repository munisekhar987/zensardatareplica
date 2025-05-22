# Use Java 24 JDK base image
FROM eclipse-temurin:24-jdk AS base

# Set working directory
WORKDIR /app

# Copy your JAR file into the container
COPY target/ZensarDataReplicationApp-0.0.1-SNAPSHOT.jar app.jar

# Use environment variables for configuration
ENTRYPOINT exec java $JAVA_OPTS \
  -Dserver.port=${SERVER_PORT} \
  -Dspring.kafka.bootstrap-servers=${KAFKA_BOOTSTRAP_SERVERS} \
  -Dspring.kafka.consumer.group-id=${KAFKA_GROUP_ID} \
  -Dspring.kafka.consumer.auto-offset-reset=${KAFKA_AUTO_OFFSET_RESET} \
  -Dspring.kafka.consumer.key-deserializer=${KAFKA_KEY_DESERIALIZER} \
  -Dspring.kafka.consumer.value-deserializer=${KAFKA_VALUE_DESERIALIZER} \
  -Dspring.kafka.properties.security.protocol=${KAFKA_SECURITY_PROTOCOL} \
  -Dspring.kafka.properties.sasl.mechanism=${KAFKA_SASL_MECHANISM} \
  -Dspring.kafka.properties.sasl.jaas.config="${KAFKA_SASL_JAAS_CONFIG}" \
  -Dcdc.kafka.topics=${CDC_TOPICS} \
  -Dcdc.topic.table-mappings=${CDC_TOPIC_TABLE_MAPPINGS} \
  -Dcdc.table.primary-keys=${CDC_TABLE_PRIMARY_KEYS} \
  -Dcdc.tables.allow-duplicates=${CDC_ALLOW_DUPLICATES} \
  -Dcdc.handle.duplicates=${CDC_HANDLE_DUPLICATES} \
  -Doracle.db.url=${ORACLE_URL} \
  -Doracle.db.username=${ORACLE_USER} \
  -Doracle.db.password=${ORACLE_PASSWORD} \
  -Doracle.db.schema=${ORACLE_SCHEMA} \
  -Dpostgres.db.url=${POSTGRES_URL} \
  -Dpostgres.db.username=${POSTGRES_USER} \
  -Dpostgres.db.password=${POSTGRES_PASSWORD} \
  -Dpostgres.db.schema=${POSTGRES_SCHEMA} \
  -Dpostgres.udt.tables=${POSTGRES_UDT_TABLES} \
  -Dpostgres.udt.type-mapping=${POSTGRES_UDT_TYPE_MAPPING} \
  -Dpostgres.udt.column-mapping=${POSTGRES_UDT_COLUMN_MAPPING} \
  -Dlogging.level.com.zensar.data.replication=${LOG_LEVEL_REPLICATION} \
  -Dlogging.level.com.zensar.data.replication.consumer=${LOG_LEVEL_CONSUMER} \
  -Dlogging.level.com.zensar.data.replication.service=${LOG_LEVEL_SERVICE} \
  -jar app.jar
