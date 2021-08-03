/*
 * Copyright © 2021 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.plugin.gcp.dataplex.sink.config;

import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.*;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.common.IdUtils;
import io.cdap.plugin.gcp.bigquery.sink.Operation;
import io.cdap.plugin.gcp.bigquery.sink.PartitionType;
import io.cdap.plugin.gcp.bigquery.util.BigQueryUtil;
import io.cdap.plugin.gcp.dataplex.sink.connector.DataplexConnectorConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Dataplex plugin UI configuration parameters and validation wrapper
 *
 */
public class DataplexBatchSinkConfig extends DataplexBaseConfig {
    public static final Set<Schema.Type> SUPPORTED_CLUSTERING_TYPES =
      ImmutableSet.of(Schema.Type.INT, Schema.Type.LONG, Schema.Type.STRING, Schema.Type.BOOLEAN, Schema.Type.BYTES);
    public static final int MAX_NUMBER_OF_COLUMNS = 4;
    private static final Logger LOG = LoggerFactory.getLogger(DataplexBatchSinkConfig.class);
    private static final String NAME_FORMAT = "format";
    private static final String NAME_TABLE = "table";
    private static final String NAME_TABLE_KEY = "tableKey";
    private static final String NAME_DEDUPE_BY = "dedupeBy";
    private static final String NAME_OPERATION = "operation";
    private static final String NAME_PARTITION_FILTER = "partitionFilter";
    private static final String NAME_PARTITIONING_TYPE = "partitioningType";
    private static final String NAME_TRUNCATE_TABLE = "truncateTable";
    private static final String NAME_UPDATE_TABLE_SCHEMA = "updateTableSchema";
    private static final String NAME_PARTITION_BY_FIELD = "partitionField";
    private static final String NAME_REQUIRE_PARTITION_FIELD = "requirePartitionField";
    private static final String NAME_CLUSTERING_ORDER = "clusteringOrder";
    private static final String NAME_RANGE_START = "rangeStart";
    private static final String NAME_RANGE_END = "rangeEnd";
    private static final String NAME_RANGE_INTERVAL = "rangeInterval";
    private static final String NAME_CONTENT_TYPE = "contentType";
    private static final String WHERE = "WHERE";
    private static final String NAME_SCHEMA = "schema";
    private static final String NAME_CUSTOM_CONTENT_TYPE = "customContentType";
    private static final String DEFAULT_CONTENT_TYPE = "application/octet-stream";
    private static final String CONTENT_TYPE_OTHER = "other";
    private static final String CONTENT_TYPE_APPLICATION_JSON = "application/json";
    private static final String CONTENT_TYPE_APPLICATION_AVRO = "application/avro";
    private static final String CONTENT_TYPE_APPLICATION_CSV = "application/csv";
    private static final String CONTENT_TYPE_TEXT_PLAIN = "text/plain";
    private static final String CONTENT_TYPE_TEXT_CSV = "text/csv";
    private static final String CONTENT_TYPE_TEXT_TSV = "text/tab-separated-values";
    private static final String FORMAT_AVRO = "avro";
    private static final String FORMAT_CSV = "csv";
    private static final String FORMAT_JSON = "json";
    private static final String FORMAT_TSV = "tsv";
    private static final String FORMAT_DELIMITED = "delimited";
    private static final String FORMAT_ORC = "orc";
    private static final String FORMAT_PARQUET = "parquet";
    private static final Pattern FIELD_PATTERN = Pattern.compile("[a-zA-Z0-9_]+");
    public static final String NAME_USE_CONNECTION = "useConnection";
    public static final String NAME_CONNECTION = "connection";

    @Name(NAME_FORMAT)
    @Nullable
    @Description("Format to write the records in. " +
      "Raw zone allowed values: avro, csv, delimited, json, orc, parquet, tsv. " +
      "Curated zone allowed values: parquet,avro, orc.")
    @Macro
    protected String format;

    @Name(NAME_CONTENT_TYPE)
    @Nullable
    @Description("The Content Type entity is used to indicate the media type of the resource. " +
      "Defaults to ‘application/octet-stream’. " +
      "Values: avro, csv, delimited, json, orc, parquet, tsv")
    @Macro
    protected String contentType;


    @Name(NAME_TABLE)
    @Nullable
    @Description("The table name of the object to use. Note: check if browsing is required. ")
    @Macro
    protected String table;


    @Name(NAME_TABLE_KEY)
    @Description("List of fields that determine relation between tables during Update and Upsert operations.")
    @Macro
    @Nullable
    protected String tableKey;

    @Name(NAME_DEDUPE_BY)
    @Description("Column names and sort order used to choose which input record to update/upsert when there are " +
      "multiple input records with the same key. For example, if this is set to ‘updated_time desc’, then if there " +
      "are multiple input records with the same key, the one with the largest value for ‘updated_time’ will be " +
      "applied.")
    @Macro
    @Nullable
    protected String dedupeBy;

    @Name(NAME_OPERATION)
    @Nullable
    @Description("Insert/Update/Upsert")
    @Macro
    protected String operation;

    @Name(NAME_PARTITION_FILTER)
    @Description("Partition filter that can be used for partition elimination during Update or Upsert operations. " +
      "Should only be used with Update or Upsert operations for tables where the required partition filter is " +
      "enabled. For example, if the table is partitioned the Partition Filter ‘_PARTITIONTIME > “2020-01-01” " +
      "and _PARTITIONTIME < “2020-03-01”‘, the update operation will be performed only in the partitions " +
      "meeting the criteria.")
    @Macro
    @Nullable
    protected String partitionFilter;


    @Name(NAME_PARTITIONING_TYPE)
    @Description("Specifies the partitioning type. Can either be Integer or Time or None. Defaults to Time. " +
      "This value is ignored if the table already exists.")
    @Macro
    @Nullable
    protected String partitioningType;


    @Name(NAME_RANGE_START)
    @Macro
    @Nullable
    @Description("Start value for range partitioning. The start value is inclusive. Ignored when table already exists")
    protected Long rangeStart;

    @Name(NAME_RANGE_END)
    @Macro
    @Nullable
    @Description("End value for range partitioning. The end value is exclusive. Ignored when table already exists")
    protected Long rangeEnd;

    @Name(NAME_RANGE_INTERVAL)
    @Macro
    @Nullable
    @Description(
      "Interval value for range partitioning. The interval value must be a positive integer."
        + "Ignored when table already exists")
    protected Long rangeInterval;


    @Name(NAME_TRUNCATE_TABLE)
    @Description(
      "Whether or not to truncate the table before writing to it. Should only be used with the Insert operation.")
    @Macro
    @Nullable
    protected Boolean truncateTable;

    @Name(NAME_UPDATE_TABLE_SCHEMA)
    @Description("Whether the BigQuery table schema should be modified when it does not match the schema expected " +
      "by the pipeline.")
    @Macro
    @Nullable
    protected Boolean updateTableSchema;

    @Name(NAME_PARTITION_BY_FIELD)
    @Description("Partitioning column for the BigQuery table. This should be left empty if the BigQuery table " +
      "is an ingestion-time partitioned table.")
    @Macro
    @Nullable
    protected String partitionByField;

    @Name(NAME_REQUIRE_PARTITION_FIELD)
    @Description(
      "Whether to create a table that requires a partition filter. This value is ignored if the table already exists.")
    @Macro
    @Nullable
    protected Boolean requirePartitionField;

    @Name(NAME_CLUSTERING_ORDER)
    @Description("List of fields that determines the sort order of the data. Fields must be of type INT, LONG, " +
      "STRING, DATE, TIMESTAMP, BOOLEAN or DECIMAL. Tables cannot be clustered on more than 4 fields. This " +
      "value is only used when the BigQuery table is automatically created and ignored if the table already exists.")
    @Macro
    @Nullable
    protected String clusteringOrder;

    @Name(NAME_SCHEMA)
    @Macro
    @Nullable
    @Description("The schema of the data to write. If provided, must be compatible with the table schema.")
    private String schema;

    @Name(NAME_USE_CONNECTION)
    @Nullable
    @Description("Whether to use an existing connection.")
    private Boolean useConnection;

    @Name(NAME_CONNECTION)
    @Macro
    @Nullable
    @Description("The existing connection to use.")
    private DataplexConnectorConfig connection;

    @Nullable
    public String getFormat() {
        return format;
    }

    @Nullable
    public String getTable() {
        return table;
    }

    @Nullable
    public String getTableKey() {
        return Strings.isNullOrEmpty(tableKey) ? null : tableKey;
    }

    @Nullable
    public String getDedupeBy() {
        return Strings.isNullOrEmpty(dedupeBy) ? null : dedupeBy;
    }

    @Nullable
    public Operation getOperation() {
        return Strings.isNullOrEmpty(operation) ? Operation.INSERT : Operation.valueOf(operation.toUpperCase());
    }

    @Nullable
    public String getPartitionFilter() {
        if (Strings.isNullOrEmpty(partitionFilter)) {
            return null;
        }
        partitionFilter = partitionFilter.trim();
        // remove the WHERE keyword from the filter if the user adds it at the begging of the expression
        if (partitionFilter.toUpperCase().startsWith(WHERE)) {
            partitionFilter = partitionFilter.substring(WHERE.length());
        }
        return partitionFilter;
    }

    @Nullable
    public PartitionType getPartitioningType() {
        return Strings.isNullOrEmpty(partitioningType) ? PartitionType.TIME
          : PartitionType.valueOf(partitioningType.toUpperCase());
    }

    @Nullable
    public Long getRangeStart() {
        return rangeStart;
    }

    @Nullable
    public Long getRangeEnd() {
        return rangeEnd;
    }

    @Nullable
    public Long getRangeInterval() {
        return rangeInterval;
    }

    @Nullable
    public Boolean isTruncateTable() {
        return truncateTable != null && truncateTable;
    }

    public JobInfo.WriteDisposition getWriteDisposition() {
        return isTruncateTable() ? JobInfo.WriteDisposition.WRITE_TRUNCATE
          : JobInfo.WriteDisposition.WRITE_APPEND;
    }

    @Nullable
    public Boolean isUpdateTableSchema() {
        return updateTableSchema;
    }

    @Nullable
    public String getPartitionByField() {
        return Strings.isNullOrEmpty(partitionByField) ? null : partitionByField;
    }

    @Nullable
    public Boolean isRequirePartitionField() {
        return requirePartitionField;
    }

    @Nullable
    public String getClusteringOrder() {
        return Strings.isNullOrEmpty(clusteringOrder) ? null : clusteringOrder;
    }


    public String getProject() {
        if (connection == null) {
            throw new IllegalArgumentException(
                    "Could not get project information, connection should not be null!");
        }
        return connection.getProject();
    }

    public DataplexConnectorConfig getConnection() {
        return connection;
    }

    @Nullable
    public String tryGetProject() {
        return connection == null ? null : connection.tryGetProject();
    }

    @Nullable
    public String getServiceAccount() {
        return connection == null ? null : connection.getServiceAccount();
    }

    @Nullable
    public Boolean isServiceAccountFilePath() {
        return connection == null ? null : connection.isServiceAccountFilePath();
    }
    public boolean autoServiceAccountUnavailable() {
        if (connection == null || connection.getServiceAccountFilePath() == null &&
                connection.isServiceAccountFilePath()) {
            try {
                ServiceAccountCredentials.getApplicationDefault();
            } catch (IOException e) {
                return true;
            }
        }
        return false;
    }

    @Nullable
    public String getServiceAccountType() {
        return connection == null ? null : connection.getServiceAccountType();
    }


    public void validateBigQueryDataset(FailureCollector collector) {
        IdUtils.validateReferenceName(referenceName, collector);

        if (!containsMacro(NAME_ASSET)) {
            BigQueryUtil.validateDataset(asset, NAME_ASSET, collector);
        }
        if (!containsMacro(NAME_TABLE)) {
            BigQueryUtil.validateTable(table, NAME_TABLE, collector);
        }

        if (getWriteDisposition().equals(JobInfo.WriteDisposition.WRITE_TRUNCATE)
          && !getOperation().equals(Operation.INSERT)) {
            collector.addFailure("Truncate must only be used with operation 'Insert'.",
              "Set Truncate to false, or change the Operation to 'Insert'.")
              .withConfigProperty(NAME_TRUNCATE_TABLE).withConfigProperty(NAME_OPERATION);
        }
    }

    @Nullable
    public Schema getSchema(FailureCollector collector) {
        if (Strings.isNullOrEmpty(schema)) {
            return null;
        }
        try {
            return Schema.parseJson(schema);
        } catch (IOException e) {
            collector.addFailure("Invalid schema: " + e.getMessage(), null).
                    withConfigProperty(NAME_SCHEMA);
        }
        // if there was an error that was added, it will throw an exception, otherwise,
        // this statement will not be executed
        throw collector.getOrThrowException();
    }


    public void validateBigQueryDataset(@Nullable Schema inputSchema, @Nullable Schema outputSchema,
                                        FailureCollector collector) {
        validateBigQueryDataset(collector);
        if (containsMacro(NAME_SCHEMA)) {
            Schema schema = outputSchema == null ? inputSchema : outputSchema;
            validatePartitionProperties(schema, collector);
            validateClusteringOrder(schema, collector);
            validateOperationProperties(schema, collector);
            if (outputSchema == null) {
                return;
            }

            List<String> schemaFields = Objects.requireNonNull(schema.getFields()).stream().
              map(Schema.Field::getName).map(String::toLowerCase).collect(Collectors.toList());

            final Set<String> duplicatedFields = getDuplicatedFields(schemaFields);

            for (Schema.Field field : outputSchema.getFields()) {
                String name = field.getName();
                // BigQuery column names only allow alphanumeric characters and _
                // https://cloud.google.com/bigquery/docs/schemas#column_names
                if (!FIELD_PATTERN.matcher(name).matches()) {
                    collector
                      .addFailure(String.format("Output field '%s' must only contain alphanumeric characters and '_'.",
                        name), null).withOutputSchemaField(name);
                }

                // check if the required fields are present in the input schema.
                if (!field.getSchema().isNullable() && inputSchema != null &&
                  inputSchema.getField(field.getName()) == null) {
                    collector.addFailure(
                      String.format("Required output field '%s' must be present in input schema.", field.getName()),
                      "Change the field to be nullable.")
                      .withOutputSchemaField(name);
                }

                // check if field is duplicated -> case insensitive
                if (duplicatedFields.contains(name.toLowerCase())) {
                    collector.addFailure(
                      String.format("Output field '%s' is duplicated.", name),
                      "BigQuery is case insensitive and does not allow two fields with the same name.")
                      .withOutputSchemaField(name);
                }
            }
        }
    }

    /**
     * Returns list of duplicated fields (case insensitive)
     */
    private Set<String> getDuplicatedFields(List<String> schemaFields) {
        final Set<String> duplicatedFields = new HashSet<>();
        final Set<String> set = new HashSet<>();
        for (String field : schemaFields) {
            if (!set.add(field)) {
                duplicatedFields.add(field);
            }
        }
        return duplicatedFields;
    }

    private void validatePartitionProperties(@Nullable Schema schema, FailureCollector collector) {
        if (tryGetProject() == null) {
            return;
        }
        String project = tryGetProject();
        String dataset = getAsset();
        String tableName = getTable();
        String serviceAccount = getServiceAccount();

        if (project == null || dataset == null || tableName == null || serviceAccount == null) {
            return;
        }

        Table table = BigQueryUtil.getBigQueryTable(project, dataset, tableName, serviceAccount,
          isServiceAccountFilePath(), collector);
        if (table != null) {
            StandardTableDefinition tableDefinition = table.getDefinition();
            TimePartitioning timePartitioning = tableDefinition.getTimePartitioning();
            if (timePartitioning == null) {
                LOG.warn(
                  String.format("The plugin is configured to auto-create a partitioned table, but table '%s' already " +
                      "exists without partitioning. Please verify the partitioning configuration.",
                    table.getTableId().getTable()));
            }
            RangePartitioning rangePartitioning = tableDefinition.getRangePartitioning();
            if (timePartitioning == null && rangePartitioning == null) {
                LOG.warn(String.format(
                  "The plugin is configured to auto-create a partitioned table, but table '%s' already " +
                    "exists without partitioning. Please verify the partitioning configuration.",
                  table.getTableId().getTable()));
            } else if (timePartitioning != null) {
                validateTimePartitionTableWithInputConfiguration(table, timePartitioning, collector);
            } else if (rangePartitioning != null) {
                validateRangePartitionTableWithInputConfiguration(table, rangePartitioning, collector);
            }
            validateColumnForPartition(partitionByField, schema, collector);
            return;
        }

    }

    private void validateTimePartitionTableWithInputConfiguration(Table table, TimePartitioning timePartitioning,
                                                                  FailureCollector collector) {
        PartitionType partitioningType = getPartitioningType();
        if (partitioningType == PartitionType.TIME && timePartitioning.getField() != null &&
          !timePartitioning.getField()
            .equals(partitionByField)) {
            collector.addFailure(String.format("Destination table '%s' is partitioned by column '%s'.",
              table.getTableId().getTable(),
              timePartitioning.getField()),
              String.format("Set the partition field to '%s'.", timePartitioning.getField()))
              .withConfigProperty(NAME_PARTITION_BY_FIELD);
        } else if (partitioningType != PartitionType.TIME) {
            LOG.warn(String.format("The plugin is configured to %s, but table '%s' already " +
                "exists with Time partitioning. Please verify the partitioning configuration.",
              partitioningType == PartitionType.INTEGER ? "auto-create a Integer partitioned table"
                : "auto-create table without partition",
              table.getTableId().getTable()));
        }
    }

    private void validateRangePartitionTableWithInputConfiguration(Table table, RangePartitioning rangePartitioning,
                                                                   FailureCollector collector) {
        PartitionType partitioningType = getPartitioningType();
        if (partitioningType != PartitionType.INTEGER) {
            LOG.warn(String.format("The plugin is configured to %s, but table '%s' already " +
                "exists with Integer partitioning. Please verify the partitioning configuration.",
              partitioningType == PartitionType.TIME ? "auto-create a Time partitioned table"
                : "auto-create table without partition",
              table.getTableId().getTable()));
        } else if (rangePartitioning.getField() != null && !rangePartitioning.getField().equals(partitionByField)) {
            collector.addFailure(String.format("Destination table '%s' is partitioned by column '%s'.",
              table.getTableId().getTable(),
              rangePartitioning.getField()),
              String.format("Set the partition field to '%s'.", rangePartitioning.getField()))
              .withConfigProperty(NAME_PARTITION_BY_FIELD);
        }
    }

    private void validateColumnForPartition(@Nullable String columnName, @Nullable Schema schema,
                                            FailureCollector collector) {
        if (containsMacro(NAME_PARTITION_BY_FIELD) || containsMacro(NAME_PARTITIONING_TYPE) || schema == null) {
            return;
        }
        PartitionType partitioningType = getPartitioningType();
        if (Strings.isNullOrEmpty(columnName)) {
            if (partitioningType == PartitionType.INTEGER) {
                collector.addFailure("Partition column not provided.",
                  "Set the column for integer partitioning.")
                  .withConfigProperty(NAME_PARTITION_BY_FIELD);
            }
            return;
        }
        Schema.Field field = schema.getField(columnName);
        if (field == null) {
            collector.addFailure(String.format("Partition column '%s' must be present in the schema.", columnName),
              "Change the Partition column to be one of the schema fields.")
              .withConfigProperty(NAME_PARTITION_BY_FIELD);
            return;
        }
        Schema fieldSchema = field.getSchema();
        fieldSchema = fieldSchema.isNullable() ? fieldSchema.getNonNullable() : fieldSchema;
        if (partitioningType == PartitionType.TIME) {
            validateTimePartitioningColumn(columnName, collector, fieldSchema);
        } else if (partitioningType == PartitionType.INTEGER) {
            validateIntegerPartitioningColumn(columnName, collector, fieldSchema);
            validateIntegerPartitioningRange(getRangeStart(), getRangeEnd(), getRangeInterval(), collector);
        }
    }

    private void validateTimePartitioningColumn(String columnName, FailureCollector collector, Schema fieldSchema) {
        Schema.LogicalType logicalType = fieldSchema.getLogicalType();
        if (logicalType != Schema.LogicalType.DATE && logicalType != Schema.LogicalType.TIMESTAMP_MICROS
          && logicalType != Schema.LogicalType.TIMESTAMP_MILLIS) {
            collector.addFailure(
              String.format("Partition column '%s' is of invalid type '%s'.", columnName, fieldSchema.getDisplayName()),
              "Partition column must be a date or timestamp.")
              .withConfigProperty(NAME_PARTITION_BY_FIELD)
              .withOutputSchemaField(columnName).withInputSchemaField(columnName);
        }
    }

    private void validateIntegerPartitioningRange(Long rangeStart, Long rangeEnd, Long rangeInterval,
                                                  FailureCollector collector) {
        if (!containsMacro(NAME_RANGE_START) && rangeStart == null) {
            collector.addFailure("Range Start is not defined.",
              "For Integer Partitioning, Range Start must be defined.")
              .withConfigProperty(NAME_RANGE_START);
        }
        if (!containsMacro(NAME_RANGE_END) && rangeEnd == null) {
            collector.addFailure("Range End is not defined.",
              "For Integer Partitioning, Range End must be defined.")
              .withConfigProperty(NAME_RANGE_END);
        }

        if (!containsMacro(NAME_RANGE_INTERVAL)) {
            if (rangeInterval == null) {
                collector.addFailure(
                  "Range Interval is not defined.",
                  "For Integer Partitioning, Range Interval must be defined.")
                  .withConfigProperty(NAME_RANGE_INTERVAL);
            } else if (rangeInterval <= 0) {
                collector.addFailure(
                  "Range Interval is not a positive number.",
                  "Range interval must be a valid positive integer.")
                  .withConfigProperty(NAME_RANGE_INTERVAL);
            }
        }
    }

    private void validateIntegerPartitioningColumn(String columnName, FailureCollector collector, Schema fieldSchema) {
        if (fieldSchema.getType() != Schema.Type.INT && fieldSchema.getType() != Schema.Type.LONG) {
            collector.addFailure(
              String.format("Partition column '%s' is of invalid type '%s'.", columnName, fieldSchema.getDisplayName()),
              "Partition column must be a int  or long.").withConfigProperty(NAME_PARTITION_BY_FIELD)
              .withOutputSchemaField(columnName).withInputSchemaField(columnName);
        }
    }

    private void validateClusteringOrder(@Nullable Schema schema, FailureCollector collector) {
        if (Strings.isNullOrEmpty(clusteringOrder) || schema == null) {
            return;
        }

        if (!containsMacro(NAME_PARTITION_BY_FIELD) && !containsMacro(NAME_CLUSTERING_ORDER) &&
          !Strings.isNullOrEmpty(clusteringOrder) && (Strings.isNullOrEmpty(partitionByField))) {
            collector.addFailure("Clustering order cannot be validated.",
              "Partition field must have a value.");
            return;
        }

        List<String> columnsNames = Arrays.stream(clusteringOrder.split(",")).map(String::trim)
          .collect(Collectors.toList());
        if (columnsNames.size() > MAX_NUMBER_OF_COLUMNS) {
            collector.addFailure(String.format("Found '%d' number of clustering fields.", columnsNames.size()),
              String.format("Expected at most '%d' clustering fields.", MAX_NUMBER_OF_COLUMNS))
              .withConfigProperty(NAME_CLUSTERING_ORDER);
            return;
        }

        for (String column : columnsNames) {
            Schema.Field field = schema.getField(column);
            if (field == null) {
                collector.addFailure(String.format("Clustering field '%s' does not exist in the schema.", column),
                  "Ensure all clustering fields exist in the schema.")
                  .withConfigElement(NAME_CLUSTERING_ORDER, column);
                continue;
            }
            Schema nonNullSchema = BigQueryUtil.getNonNullableSchema(field.getSchema());

            Schema.Type type = nonNullSchema.getType();
            Schema.LogicalType logicalType = nonNullSchema.getLogicalType();

            if (!SUPPORTED_CLUSTERING_TYPES.contains(type) && !isSupportedLogicalType(logicalType)) {
                collector.addFailure(
                  String.format("Field '%s' is of unsupported type '%s'.", column, nonNullSchema.getDisplayName()),
                  "Supported types are : string, bytes, int, long, boolean, date, timestamp and decimal.")
                  .withConfigElement(NAME_CLUSTERING_ORDER, column).withInputSchemaField(column)
                  .withOutputSchemaField(column);
            }
        }
    }

    private void validateOperationProperties(@Nullable Schema schema, FailureCollector collector) {
        if (containsMacro(NAME_OPERATION) || containsMacro(NAME_TABLE_KEY) || containsMacro(NAME_DEDUPE_BY)) {
            return;
        }
        Operation assetOperation = getOperation();
        if (Arrays.stream(Operation.values()).noneMatch(assetOperation::equals)) {
            collector.addFailure(
              String.format("Operation has incorrect value '%s'.", assetOperation),
              "Set the operation to 'Insert', 'Update', or 'Upsert'.")
              .withConfigElement(NAME_OPERATION, assetOperation.name().toLowerCase());
            return;
        }
        if (Operation.INSERT.equals(assetOperation)) {
            return;
        }
        if ((Operation.UPDATE.equals(assetOperation) || Operation.UPSERT.equals(assetOperation))
          && getTableKey() == null) {
            collector.addFailure(
              "Table key must be set if the operation is 'Update' or 'Upsert'.", null)
              .withConfigProperty(NAME_TABLE_KEY).withConfigProperty(NAME_OPERATION);
            return;
        }

        if (schema == null) {
            return;
        }
        List<String> fields = Objects.requireNonNull(schema.getFields()).stream().map(Schema.Field::getName)
          .collect(Collectors.toList());
        List<String> keyFields = Arrays.stream(Objects.requireNonNull(getTableKey()).split(","))
          .map(String::trim).collect(Collectors.toList());

        for (String keyField : keyFields) {
            if (!fields.contains(keyField)) {
                collector.addFailure(
                  String.format("Table key field '%s' does not exist in the schema.", keyField),
                  "Change the Table key field to be one of the schema fields.")
                  .withConfigElement(NAME_TABLE_KEY, keyField);
            }
        }

        Map<String, Integer> keyMap = calculateDuplicates(keyFields);
        keyMap.keySet().stream()
          .filter(key -> keyMap.get(key) != 1)
          .forEach(key -> collector.addFailure(
            String.format("Table key field '%s' is duplicated.", key),
            String.format("Remove duplicates of Table key field '%s'.", key))
            .withConfigElement(NAME_TABLE_KEY, key)
          );

        if ((Operation.UPDATE.equals(assetOperation) ||
                Operation.UPSERT.equals(assetOperation)) && getDedupeBy() != null) {
            List<String> dedupeByList = Arrays.stream(Objects.requireNonNull(getDedupeBy()).split(","))
              .collect(Collectors.toList());

            dedupeByList.stream()
              .filter(v -> !fields.contains(v.split(" ")[0]))
              .forEach(v -> collector.addFailure(
                String.format("Dedupe by field '%s' does not exist in the schema.", v.split(" ")[0]),
                "Change the Dedupe by field to be one of the schema fields.")
                .withConfigElement(NAME_DEDUPE_BY, v));

            Map<String, Integer> orderedByFieldMap = calculateDuplicates(dedupeByList);
            Map<String, String> orderedByFieldValueMap = dedupeByList.stream()
              .collect(Collectors.toMap(p -> p.split(" ")[0], p -> p, (x, y) -> y));

            orderedByFieldMap.keySet().stream()
              .filter(key -> orderedByFieldMap.get(key) != 1)
              .forEach(key -> collector.addFailure(
                String.format("Dedupe by field '%s' is duplicated.", key),
                String.format("Remove duplicates of Dedupe by field '%s'.", key))
                .withConfigElement(NAME_DEDUPE_BY, orderedByFieldValueMap.get(key))
              );
        }
    }

    private Map<String, Integer> calculateDuplicates(List<String> values) {
        return values.stream()
          .map(v -> v.split(" ")[0])
          .collect(Collectors.toMap(p -> p, p -> 1, (x, y) -> x + y));
    }

    private boolean isSupportedLogicalType(Schema.LogicalType logicalType) {
        if (logicalType != null) {
            return logicalType == Schema.LogicalType.DATE || logicalType == Schema.LogicalType.TIMESTAMP_MICROS ||
              logicalType == Schema.LogicalType.TIMESTAMP_MILLIS || logicalType == Schema.LogicalType.DECIMAL;
        }
        return false;
    }

    /**
     * Returns true if bigquery table can be connected to or schema is not a macro.
     */
    boolean shouldConnect() {
        return !containsMacro(NAME_ASSET) && !containsMacro(NAME_TABLE) &&
                !containsMacro(connection.NAME_SERVICE_ACCOUNT_TYPE) &&
          !(containsMacro(connection.NAME_SERVICE_ACCOUNT_FILE_PATH) ||
                  containsMacro(connection.NAME_SERVICE_ACCOUNT_JSON)) &&
          !containsMacro(connection.NAME_PROJECT) && !containsMacro(NAME_SCHEMA);
    }

    public void validateStorageBucket(FailureCollector collector) {
        IdUtils.validateReferenceName(referenceName, collector);

        if (!containsMacro(NAME_FORMAT)) {
            try {
                getFormat();
            } catch (IllegalArgumentException e) {
                collector.addFailure(e.getMessage(), null).withConfigProperty(NAME_FORMAT)
                  .withStacktrace(e.getStackTrace());
            }
        }

        if (!containsMacro(NAME_CONTENT_TYPE) && !containsMacro(NAME_CUSTOM_CONTENT_TYPE)
          && !Strings.isNullOrEmpty(contentType) && !contentType.equalsIgnoreCase(CONTENT_TYPE_OTHER)
          && !containsMacro(NAME_FORMAT) && !contentType.equalsIgnoreCase(DEFAULT_CONTENT_TYPE)) {
            validateContentType(collector);
        }

        try {
            getSchema(collector);
        } catch (IllegalArgumentException e) {
            collector.addFailure(e.getMessage(), null).withConfigProperty(NAME_SCHEMA)
              .withStacktrace(e.getStackTrace());
        }

    }

    //This method validates the specified content type for the used format.
    public void validateContentType(FailureCollector failureCollector) {
        switch (format) {
            case FORMAT_AVRO:
                if (!contentType.equalsIgnoreCase(CONTENT_TYPE_APPLICATION_AVRO)) {
                    failureCollector.addFailure(String.format("Valid content types for avro are %s, %s.",
                      CONTENT_TYPE_APPLICATION_AVRO, DEFAULT_CONTENT_TYPE), null)
                      .withConfigProperty(NAME_CONTENT_TYPE);
                }
                break;
            case FORMAT_JSON:
                if (!contentType.equalsIgnoreCase(CONTENT_TYPE_APPLICATION_JSON)
                  && !contentType.equalsIgnoreCase(CONTENT_TYPE_TEXT_PLAIN)) {
                    failureCollector.addFailure(String.format(
                      "Valid content types for json are %s, %s, %s.", CONTENT_TYPE_APPLICATION_JSON,
                      CONTENT_TYPE_TEXT_PLAIN, DEFAULT_CONTENT_TYPE), null
                    ).withConfigProperty(NAME_CONTENT_TYPE);
                }
                break;
            case FORMAT_CSV:
                if (!contentType.equalsIgnoreCase(CONTENT_TYPE_APPLICATION_CSV)
                  && !contentType.equalsIgnoreCase(CONTENT_TYPE_TEXT_CSV)
                  && !contentType.equalsIgnoreCase(CONTENT_TYPE_TEXT_PLAIN)) {
                    failureCollector.addFailure(String.format(
                      "Valid content types for csv are %s, %s, %s, %s.", CONTENT_TYPE_APPLICATION_CSV,
                      CONTENT_TYPE_TEXT_PLAIN, CONTENT_TYPE_TEXT_CSV, DEFAULT_CONTENT_TYPE), null
                    ).withConfigProperty(NAME_CONTENT_TYPE);
                }
                break;
            case FORMAT_DELIMITED:
                failureCollector.addFailure(String.format(
                  "Valid content types for delimited are %s, %s, %s, %s, %s.", CONTENT_TYPE_TEXT_PLAIN,
                  CONTENT_TYPE_TEXT_CSV, CONTENT_TYPE_APPLICATION_CSV, CONTENT_TYPE_TEXT_TSV, DEFAULT_CONTENT_TYPE),
                  null
                ).withConfigProperty(NAME_CONTENT_TYPE);
                break;
            case FORMAT_PARQUET:
                if (!contentType.equalsIgnoreCase(DEFAULT_CONTENT_TYPE)) {
                    failureCollector
                      .addFailure(String.format("Valid content type for parquet is %s.", DEFAULT_CONTENT_TYPE),
                        null).withConfigProperty(NAME_CONTENT_TYPE);
                }
                break;
            case FORMAT_ORC:
                if (!contentType.equalsIgnoreCase(DEFAULT_CONTENT_TYPE)) {
                    failureCollector
                      .addFailure(String.format("Valid content type for orc is %s.", DEFAULT_CONTENT_TYPE),
                        null).withConfigProperty(NAME_CONTENT_TYPE);
                }
                break;
            case FORMAT_TSV:

                failureCollector.addFailure(String.format(
                  "Valid content types for tsv are %s, %s, %s.", CONTENT_TYPE_TEXT_TSV, CONTENT_TYPE_TEXT_PLAIN,
                  DEFAULT_CONTENT_TYPE), null).withConfigProperty(NAME_CONTENT_TYPE);

                break;
        }
    }

}
