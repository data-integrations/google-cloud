/*
 * Copyright © 2018 Cask Data, Inc.
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

package io.cdap.plugin.gcp.bigquery.sink;

import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TimePartitioning;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.gcp.bigquery.util.BigQueryUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * This class <code>BigQuerySinkConfig</code> provides all the configuration required for
 * configuring the <code>BigQuerySink</code> plugin.
 */
public final class BigQuerySinkConfig extends AbstractBigQuerySinkConfig {
  private static final Logger LOG = LoggerFactory.getLogger(BigQuerySinkConfig.class);
  private static final String WHERE = "WHERE";
  public static final Set<Schema.Type> SUPPORTED_CLUSTERING_TYPES =
    ImmutableSet.of(Schema.Type.INT, Schema.Type.LONG, Schema.Type.STRING, Schema.Type.BOOLEAN, Schema.Type.BYTES);
  private static final Pattern FIELD_PATTERN = Pattern.compile("[a-zA-Z0-9_]+");

  public static final String NAME_TABLE = "table";
  public static final String NAME_SCHEMA = "schema";
  public static final String NAME_TABLE_KEY = "relationTableKey";
  public static final String NAME_DEDUPE_BY = "dedupeBy";
  public static final String NAME_PARTITION_BY_FIELD = "partitionByField";
  public static final String NAME_CLUSTERING_ORDER = "clusteringOrder";
  public static final String NAME_OPERATION = "operation";
  public static final String PARTITION_FILTER = "partitionFilter";

  public static final int MAX_NUMBER_OF_COLUMNS = 4;

  @Name(NAME_TABLE)
  @Macro
  @Description("The table to write to. A table contains individual records organized in rows. "
    + "Each record is composed of columns (also called fields). "
    + "Every table is defined by a schema that describes the column names, data types, and other information.")
  private String table;

  @Name(NAME_SCHEMA)
  @Macro
  @Nullable
  @Description("The schema of the data to write. If provided, must be compatible with the table schema.")
  private String schema;

  @Macro
  @Nullable
  @Description("Whether to create the BigQuery table with time partitioning. This value is ignored if the table " +
    "already exists.")
  protected Boolean createPartitionedTable;

  @Name(NAME_PARTITION_BY_FIELD)
  @Macro
  @Nullable
  @Description("Partitioning column for the BigQuery table. This should be left empty if the BigQuery table is an " +
    "ingestion-time partitioned table.")
  protected String partitionByField;

  @Name(NAME_OPERATION)
  @Macro
  @Nullable
  @Description("Type of write operation to perform. This can be set to Insert, Update or Upsert.")
  protected String operation;

  @Name(NAME_TABLE_KEY)
  @Macro
  @Nullable
  @Description("List of fields that determines relation between tables during Update and Upsert operations.")
  protected String relationTableKey;

  @Name(NAME_DEDUPE_BY)
  @Macro
  @Nullable
  @Description("Column names and sort order used to choose which input record to update/upsert when there are " +
    "multiple input records with the same key. For example, if this is set to 'updated_time desc', then if there are " +
    "multiple input records with the same key, the one with the largest value for 'updated_time' will be applied.")
  protected String dedupeBy;

  @Macro
  @Nullable
  @Description("Whether to create a table that requires a partition filter. This value is ignored if the table " +
    "already exists.")
  protected Boolean partitionFilterRequired;

  @Name(NAME_CLUSTERING_ORDER)
  @Macro
  @Nullable
  @Description("List of fields that determines the sort order of the data. Fields must be of type INT, LONG, " +
    "STRING, DATE, TIMESTAMP, BOOLEAN or DECIMAL. Tables cannot be clustered on more than 4 fields. This value is " +
    "only used when the BigQuery table is automatically created and ignored if the table already exists.")
  protected String clusteringOrder;

  @Name(PARTITION_FILTER)
  @Macro
  @Nullable
  @Description("Partition filter that can be used for partition elimination during Update or Upsert operations." +
          "This value is ignored if operation is not UPDATE or UPSERT.")
  protected String partitionFilter;

  public BigQuerySinkConfig(String referenceName, String dataset, String table,
                            @Nullable String bucket, @Nullable String schema) {
    this.referenceName = referenceName;
    this.dataset = dataset;
    this.table = table;
    this.bucket = bucket;
    this.schema = schema;
  }

  public String getTable() {
    return table;
  }

  public boolean shouldCreatePartitionedTable() {
    return createPartitionedTable == null ? false : createPartitionedTable;
  }

  @Nullable
  public String getPartitionByField() {
    return Strings.isNullOrEmpty(partitionByField) ? null : partitionByField;
  }

  public boolean isPartitionFilterRequired() {
    return partitionFilterRequired == null ? false : partitionFilterRequired;
  }

  @Nullable
  public String getClusteringOrder() {
    return Strings.isNullOrEmpty(clusteringOrder) ? null : clusteringOrder;
  }

  public Operation getOperation() {
    return Strings.isNullOrEmpty(operation) ? Operation.INSERT : Operation.valueOf(operation.toUpperCase());
  }

  @Nullable
  public String getRelationTableKey() {
    return Strings.isNullOrEmpty(relationTableKey) ? null : relationTableKey;
  }

  @Nullable
  public String getDedupeBy() {
    return Strings.isNullOrEmpty(dedupeBy) ? null : dedupeBy;
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
    return  partitionFilter;
  }

  /**
   * @return the schema of the dataset
   */
  @Nullable
  public Schema getSchema(FailureCollector collector) {
    if (Strings.isNullOrEmpty(schema)) {
      return null;
    }
    try {
      return Schema.parseJson(schema);
    } catch (IOException e) {
      collector.addFailure("Invalid schema: " + e.getMessage(), null).withConfigProperty(NAME_SCHEMA);
    }
    // if there was an error that was added, it will throw an exception, otherwise, this statement will not be executed
    throw collector.getOrThrowException();
  }

  @Override
  public void validate(FailureCollector collector) {
    super.validate(collector);

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

  /**
   * Verifies if output schema only contains simple types. It also verifies if all the output schema fields are
   * present in input schema.
   *
   * @param inputSchema input schema to BigQuery sink
   * @param outputSchema output schema to BigQuery sink
   * @param collector failure collector
   */
  public void validate(@Nullable Schema inputSchema, @Nullable Schema outputSchema, FailureCollector collector) {
    validate(collector);
    if (!containsMacro(NAME_SCHEMA)) {
      Schema schema = outputSchema == null ? inputSchema : outputSchema;
      validatePartitionProperties(schema, collector);
      validateClusteringOrder(schema, collector);
      validateOperationProperties(schema, collector);
      if (outputSchema == null) {
        return;
      }

      for (Schema.Field field : outputSchema.getFields()) {
        String name = field.getName();
        // BigQuery column names only allow alphanumeric characters and _
        // https://cloud.google.com/bigquery/docs/schemas#column_names
        if (!FIELD_PATTERN.matcher(name).matches()) {
          collector.addFailure(String.format("Output field '%s' must only contain alphanumeric characters and '_'.",
                                             name), null).withOutputSchemaField(name);
        }

        // check if the required fields are present in the input schema.
        if (!field.getSchema().isNullable() && inputSchema != null && inputSchema.getField(field.getName()) == null) {
          collector.addFailure(
            String.format("Required output field '%s' must be present in input schema.", field.getName()),
            "Change the field to be nullable.")
            .withOutputSchemaField(name);
        }
      }
    }
  }

  private void validatePartitionProperties(@Nullable Schema schema, FailureCollector collector) {
    if (tryGetProject() == null) {
      return;
    }
    String project = getProject();
    String dataset = getDataset();
    String tableName = getTable();
    String serviceAccountPath = getServiceAccountFilePath();

    if (project == null || dataset == null || tableName == null || serviceAccountPath == null) {
      return;
    }

    Table table = BigQueryUtil.getBigQueryTable(project, dataset, tableName, serviceAccountPath, collector);
    if (table != null) {
      StandardTableDefinition tableDefinition = table.getDefinition();
      TimePartitioning timePartitioning = tableDefinition.getTimePartitioning();
      if (timePartitioning == null && createPartitionedTable != null && createPartitionedTable) {
        LOG.warn(String.format("The plugin is configured to auto-create a partitioned table, but table '%s' already " +
                                 "exists without partitioning. Please verify the partitioning configuration.",
                               table.getTableId().getTable()));
      }
      if (timePartitioning != null && timePartitioning.getField() != null
        && !timePartitioning.getField().equals(partitionByField)) {
        collector.addFailure(String.format("Destination table '%s' is partitioned by column '%s'.",
                                           table.getTableId().getTable(),
                                           timePartitioning.getField()),
                             String.format("Set the partition field to '%s'.", timePartitioning.getField()))
          .withConfigProperty(NAME_PARTITION_BY_FIELD);
      }
      validateColumnForPartition(partitionByField, schema, collector);
      return;
    }
    if (createPartitionedTable == null || !createPartitionedTable) {
      return;
    }
    validateColumnForPartition(partitionByField, schema, collector);
  }

  private void validateColumnForPartition(@Nullable String columnName, @Nullable Schema schema,
                                          FailureCollector collector) {
    if (columnName == null || schema == null) {
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
    Schema.LogicalType logicalType = fieldSchema.getLogicalType();
    if (logicalType != Schema.LogicalType.DATE && logicalType != Schema.LogicalType.TIMESTAMP_MICROS
      && logicalType != Schema.LogicalType.TIMESTAMP_MILLIS) {
      collector.addFailure(
        String.format("Partition column '%s' is of invalid type '%s'.", columnName, fieldSchema.getDisplayName()),
        "Partition column must be a date or timestamp.").withConfigProperty(NAME_PARTITION_BY_FIELD)
        .withOutputSchemaField(columnName).withInputSchemaField(columnName);
    }
  }

  private void validateClusteringOrder(@Nullable Schema schema, FailureCollector collector) {
    if (!shouldCreatePartitionedTable() || Strings.isNullOrEmpty(clusteringOrder) || schema == null) {
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
          .withConfigElement(NAME_CLUSTERING_ORDER, column).withInputSchemaField(column).withOutputSchemaField(column);
      }
    }
  }

  private void validateOperationProperties(@Nullable Schema schema, FailureCollector collector) {
    if (containsMacro(NAME_OPERATION) || containsMacro(NAME_TABLE_KEY) || containsMacro(NAME_DEDUPE_BY)) {
      return;
    }
    Operation operation = getOperation();
    if (Arrays.stream(Operation.values()).noneMatch(operation::equals)) {
      collector.addFailure(
        String.format("Operation has incorrect value '%s'.", operation),
        "Set the operation to 'Insert', 'Update', or 'Upsert'.")
        .withConfigElement(NAME_OPERATION, operation.name().toLowerCase());
      return;
    }
    if (Operation.INSERT.equals(operation)) {
      return;
    }
    if ((Operation.UPDATE.equals(operation) || Operation.UPSERT.equals(operation))
      && getRelationTableKey() == null) {
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
    List<String> keyFields = Arrays.stream(Objects.requireNonNull(getRelationTableKey()).split(","))
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

    if ((Operation.UPDATE.equals(operation) || Operation.UPSERT.equals(operation)) && getDedupeBy() != null) {
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
    return !containsMacro(BigQuerySinkConfig.NAME_DATASET) && !containsMacro(BigQuerySinkConfig.NAME_TABLE) &&
      !containsMacro(BigQuerySinkConfig.NAME_SERVICE_ACCOUNT_FILE_PATH) &&
      !containsMacro(BigQuerySinkConfig.NAME_PROJECT) && !containsMacro(BigQuerySinkConfig.NAME_SCHEMA);
  }
}
