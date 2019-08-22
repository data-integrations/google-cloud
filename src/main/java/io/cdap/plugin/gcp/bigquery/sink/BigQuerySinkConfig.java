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
import java.util.Objects;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * This class <code>BigQuerySinkConfig</code> provides all the configuration required for
 * configuring the <code>BigQuerySink</code> plugin.
 */
public final class BigQuerySinkConfig extends AbstractBigQuerySinkConfig {
  private static final Logger LOG = LoggerFactory.getLogger(BigQuerySinkConfig.class);

  public static final int MAX_NUMBER_OF_COLUMNS = 4;

  @Macro
  @Description("The table to write to. A table contains individual records organized in rows. "
    + "Each record is composed of columns (also called fields). "
    + "Every table is defined by a schema that describes the column names, data types, and other information.")
  private String table;

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
    return partitionByField;
  }

  public boolean isPartitionFilterRequired() {
    return partitionFilterRequired == null ? false : partitionFilterRequired;
  }

  @Nullable
  public String getClusteringOrder() {
    return clusteringOrder;
  }

  public Operation getOperation() {
    return operation == null ? Operation.INSERT : Operation.valueOf(operation.toUpperCase());
  }

  @Nullable
  public String getRelationTableKey() {
    return relationTableKey;
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
      collector.addFailure("Invalid schema: " + e.getMessage(),
                           "Provided schema must be parsed as json.").withConfigProperty("schema");
    }
    // if there was an error that was added, it will throw an exception, otherwise, this statement will not be executed
    throw collector.getOrThrowException();
  }

  @Override
  public void validate(FailureCollector collector) {
    super.validate(collector);
    if (getWriteDisposition().equals(JobInfo.WriteDisposition.WRITE_TRUNCATE)
      && !getOperation().equals(Operation.INSERT)) {
      collector.addFailure("Truncate must only be used with operation 'Insert'.", null)
        .withConfigProperty(NAME_TRUNCATE_TABLE).withConfigProperty(NAME_OPERATION);
    }
  }

  /**
   * Verifies if output schema only contains simple types. It also verifies if all the output schema fields are
   * present in input schema.
   *
   * @param inputSchema input schema to BigQuery sink
   * @param collector failure collector
   */
  public void validate(@Nullable Schema inputSchema, FailureCollector collector) {
    super.validate(collector);
    if (!containsMacro("schema")) {
      Schema outputSchema = getSchema(collector);
      Schema schema = outputSchema != null ? outputSchema : inputSchema;
      validatePartitionProperties(schema, collector);
      validateClusteringOrder(schema, collector);
      validateOperationProperties(schema, collector);
      if (outputSchema == null) {
        return;
      }
      for (Schema.Field field : outputSchema.getFields()) {
        String name = field.getName();

        // check if the required fields are present in the input schema.
        if (!field.getSchema().isNullable() && inputSchema != null && inputSchema.getField(field.getName()) == null) {
          collector.addFailure(String.format("Required output field '%s' must be present in input schema.",
                                             field.getName()), null)
            .withOutputSchemaField(name, null);
          continue;
        }

        Schema fieldSchema = BigQueryUtil.getNonNullableSchema(field.getSchema());
        Schema.Type type = fieldSchema.getType();

        if (!BigQueryUtil.SUPPORTED_TYPES.contains(type)) {
          collector.addFailure(String.format("Field '%s' is of unsupported type '%s'.", name,
                                             type.name().toLowerCase()),
                               String.format("Supported types are: %s",
                                             BigQueryUtil.SUPPORTED_TYPES.stream().map(t -> t.name()
                                               .toLowerCase()).collect(Collectors.joining(","))))
            .withOutputSchemaField(name, null);
          continue;
        }

        Schema.LogicalType logicalType = fieldSchema.getLogicalType();
        // BigQuery schema precision must be at most 38 and scale at most 9
        if (logicalType == Schema.LogicalType.DECIMAL) {
          if (fieldSchema.getPrecision() > 38 || fieldSchema.getScale() > 9) {
            collector.addFailure(String.format("Field '%s' has invalid precision '%d' or scale '%d'. ",
                                               name, fieldSchema.getPrecision(), fieldSchema.getScale()),
                                 "Precision must be at most 38 and scale must be at most 9.")
              .withOutputSchemaField(name, null);
            continue;
          }
        }

        if (type == Schema.Type.ARRAY) {
          BigQueryUtil.validateArraySchema(field.getSchema(), name, collector);
        }
      }
    }
    collector.getOrThrowException();
  }

  private void validatePartitionProperties(@Nullable Schema schema, FailureCollector collector) {
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
          .withConfigProperty("partitionByField");
      }
      validateColumnForPartition(partitionByField, schema, collector);
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
      collector.addFailure(String.format("Partition column '%s' must be present in the table schema.", columnName),
                           null)
        .withConfigProperty(NAME_PARTITION_BY_FIELD);
      return;
    }
    Schema fieldSchema = field.getSchema();
    fieldSchema = fieldSchema.isNullable() ? fieldSchema.getNonNullable() : fieldSchema;
    Schema.LogicalType logicalType = fieldSchema.getLogicalType();
    if (logicalType != Schema.LogicalType.DATE && logicalType != Schema.LogicalType.TIMESTAMP_MICROS
      && logicalType != Schema.LogicalType.TIMESTAMP_MILLIS) {
      String type = logicalType != null ? logicalType.getToken() : fieldSchema.getType().name();
      collector.addFailure(String.format("Partition column '%s' is of invalid type '%s'.",
                                         columnName, type.toLowerCase()),
                           "Partition column must be either of type date or timestamp.")
        .withConfigProperty(NAME_PARTITION_BY_FIELD)
        .withOutputSchemaField(columnName, null);
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
                           String.format("Expected no more than '%d' clustering fields.", MAX_NUMBER_OF_COLUMNS))
        .withConfigProperty(NAME_CLUSTERING_ORDER);
      return;
    }
    for (String column : columnsNames) {
      Schema.Field field = schema.getField(column);
      if (field == null) {
        collector.addFailure(String.format("Clustering column '%s' is missing from the table schema.", column),
                             "It must be present in table schema.")
          .withConfigElement(NAME_CLUSTERING_ORDER, column);
        continue;
      }
      Schema nonNullSchema = BigQueryUtil.getNonNullableSchema(field.getSchema());

      Schema.Type type = nonNullSchema.getType();
      Schema.LogicalType logicalType = nonNullSchema.getLogicalType();

      if (!BigQueryUtil.SUPPORTED_CLUSTERING_TYPES.contains(type)) {
        collector.addFailure(
          String.format("Field '%s' is of unsupported type '%s'.", column, type),
          "Supported types are : string, bytes, int, long, boolean, date, time or timestamp.")
          .withConfigElement(NAME_CLUSTERING_ORDER, column).withOutputSchemaField(column, null);
      }

      if (logicalType != null && logicalType != Schema.LogicalType.DATE &&
        logicalType != Schema.LogicalType.TIMESTAMP_MICROS && logicalType != Schema.LogicalType.TIMESTAMP_MILLIS &&
        logicalType != Schema.LogicalType.DECIMAL) {
        collector.addFailure(
          String.format("Field '%s' is of unsupported type '%s'.", column,
                        logicalType.getToken().toLowerCase()),
          "Supported types are : string, bytes, int, long, boolean, date, time or timestamp.")
          .withConfigElement(NAME_CLUSTERING_ORDER, column).withOutputSchemaField(column, null);
      }
    }
  }

  private void validateOperationProperties(@Nullable Schema schema, FailureCollector collector) {
    if (Arrays.stream(Operation.values()).map(Enum::name).noneMatch(operation.toUpperCase()::equals)) {
      collector.addFailure(
        String.format("Operation has incorrect value '%s'.", operation),
        "The operation must be 'Insert', 'Update' or 'Upsert'.")
        .withConfigElement(NAME_OPERATION, operation);
      return;
    }
    if (Operation.INSERT.equals(getOperation())) {
      return;
    }
    if ((Operation.UPDATE.equals(getOperation()) || Operation.UPSERT.equals(getOperation()))
      && getRelationTableKey() == null) {
      collector.addFailure(
        "Table key must be set if the operation is 'Update' or 'Upsert'.", null)
        .withConfigProperty(NAME_TABLE_KEY).withConfigProperty("operation");
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
          String.format("Field '%s' is in the table key, but not in the input schema.", keyField),
          "It must be present in input schema.")
          .withConfigElement(NAME_TABLE_KEY, keyField).withInputSchemaField(keyField, null);
      }
    }
  }
}
