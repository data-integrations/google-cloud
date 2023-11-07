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
import com.google.cloud.bigquery.RangePartitioning;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TimePartitioning;
import com.google.cloud.kms.v1.CryptoKeyName;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.data.schema.Schema.LogicalType;
import io.cdap.cdap.api.data.schema.Schema.Type;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.gcp.bigquery.connector.BigQueryConnectorConfig;
import io.cdap.plugin.gcp.bigquery.util.BigQueryUtil;
import io.cdap.plugin.gcp.common.CmekUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
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
  public static final String NAME_PARTITIONING_TYPE = "partitioningType";
  public static final String NAME_TIME_PARTITIONING_TYPE = "timePartitioningType";
  public static final String NAME_RANGE_START = "rangeStart";
  public static final String NAME_RANGE_END = "rangeEnd";
  public static final String NAME_RANGE_INTERVAL = "rangeInterval";

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
  @Description("DEPRECATED!. Whether to create the BigQuery table with time partitioning. "
    + "This value is ignored if the table already exists."
    + " When this is set to false, value of Partitioning type will be used. Use 'Partitioning type' property")
  protected Boolean createPartitionedTable;

  @Name(NAME_PARTITIONING_TYPE)
  @Macro
  @Nullable
  @Description("Specifies the partitioning type. Can either be Integer or Time or None. "
    + "Ignored when table already exists")
  protected String partitioningType;

  @Name(NAME_TIME_PARTITIONING_TYPE)
  @Macro
  @Nullable
  @Description("Specifies the time partitioning type. Can either be Daily or Hourly or Monthly or Yearly. "
    + "Ignored when table already exists")
  protected String timePartitioningType;

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

  @VisibleForTesting
  public BigQuerySinkConfig(@Nullable String referenceName, String dataset, String table, @Nullable String bucket,
                            @Nullable String schema, @Nullable String partitioningType, @Nullable Long rangeStart,
                            @Nullable Long rangeEnd, @Nullable Long rangeInterval, @Nullable String gcsChunkSize) {
    super(null, dataset, null, bucket);
    this.referenceName = referenceName;
    this.table = table;
    this.schema = schema;
    this.partitioningType = partitioningType;
    this.rangeStart = rangeStart;
    this.rangeEnd = rangeEnd;
    this.rangeInterval = rangeInterval;
    this.gcsChunkSize = gcsChunkSize;
  }

  private BigQuerySinkConfig(@Nullable String referenceName, @Nullable String project,
                             @Nullable String serviceAccountType, @Nullable String serviceFilePath,
                             @Nullable String serviceAccountJson,
                             @Nullable String dataset, @Nullable String table, @Nullable String location,
                             @Nullable String cmekKey, @Nullable String bucket, @Nullable String jobLabelKeyValue) {
    super(new BigQueryConnectorConfig(project, project, serviceAccountType,
            serviceFilePath, serviceAccountJson), dataset, cmekKey, bucket);
    this.referenceName = referenceName;
    this.table = table;
    this.location = location;
    this.jobLabelKeyValue = jobLabelKeyValue;
  }

  public String getTable() {
    return table;
  }

  public boolean shouldCreatePartitionedTable() {
    return getPartitioningType() != PartitionType.NONE;
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

  public PartitionType getPartitioningType() {
    if (createPartitionedTable != null && createPartitionedTable) {
      return PartitionType.TIME;
    }
    return Strings.isNullOrEmpty(partitioningType) ? PartitionType.TIME
      : PartitionType.valueOf(partitioningType.toUpperCase());
  }

  public TimePartitioning.Type getTimePartitioningType() {
    return Strings.isNullOrEmpty(timePartitioningType) ? TimePartitioning.Type.DAY :
            TimePartitioning.Type.valueOf(timePartitioningType.toUpperCase());
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
  public void validate(FailureCollector collector, Map<String, String> arguments) {
    super.validate(collector, arguments);

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
  public void validate(@Nullable Schema inputSchema, @Nullable Schema outputSchema, FailureCollector collector,
                       Map<String, String> arguments) {
    validate(collector, arguments);
    if (!containsMacro(NAME_SCHEMA)) {
      Schema schema = outputSchema == null ? inputSchema : outputSchema;
      validatePartitionProperties(schema, collector);
      validateClusteringOrder(schema, collector);
      validateOperationProperties(schema, collector);
      if (outputSchema == null) {
        return;
      }

      List<String> schemaFields = Objects.requireNonNull(schema.getFields()).stream().
        map(Schema.Field::getName).map(String::toLowerCase).collect(Collectors.toList());

      final Set<String> duplicatedFields = BigQuerySinkUtils.getDuplicatedFields(schemaFields);

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

  private void validatePartitionProperties(@Nullable Schema schema, FailureCollector collector) {
    if (tryGetProject() == null) {
      return;
    }
    String project = getDatasetProject();
    String dataset = getDataset();
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
      if (timePartitioning == null && createPartitionedTable != null && createPartitionedTable) {
        LOG.warn(String.format("The plugin is configured to auto-create a partitioned table, but table '%s' already " +
                                 "exists without partitioning. Please verify the partitioning configuration.",
                               table.getTableId().getTable()));
      }
      RangePartitioning rangePartitioning = tableDefinition.getRangePartitioning();
      if (timePartitioning == null && rangePartitioning == null && shouldCreatePartitionedTable()) {
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
    if (shouldCreatePartitionedTable()) {
      validateColumnForPartition(partitionByField, schema, collector);
    }
  }

  private void validateTimePartitionTableWithInputConfiguration(Table table, TimePartitioning timePartitioning,
                                                                FailureCollector collector) {
    PartitionType partitioningType = getPartitioningType();
    if (partitioningType == PartitionType.TIME && timePartitioning.getField() != null && !timePartitioning.getField()
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
      validateTimePartitioningColumn(columnName, collector, fieldSchema, getTimePartitioningType());
    } else if (partitioningType == PartitionType.INTEGER) {
      validateIntegerPartitioningColumn(columnName, collector, fieldSchema);
      validateIntegerPartitioningRange(getRangeStart(), getRangeEnd(), getRangeInterval(), collector);
    }
  }

  private void validateIntegerPartitioningColumn(String columnName, FailureCollector collector, Schema fieldSchema) {
    if (fieldSchema.getType() != Type.INT && fieldSchema.getType() != Type.LONG) {
      collector.addFailure(
        String.format("Partition column '%s' is of invalid type '%s'.", columnName, fieldSchema.getDisplayName()),
        "Partition column must be a int  or long.").withConfigProperty(NAME_PARTITION_BY_FIELD)
        .withOutputSchemaField(columnName).withInputSchemaField(columnName);
    }
  }

  private void validateTimePartitioningColumn(String columnName, FailureCollector collector,
                                              Schema fieldSchema, TimePartitioning.Type timePartitioningType) {

    Schema.LogicalType logicalType = fieldSchema.getLogicalType();

    boolean isTimestamp = logicalType == LogicalType.TIMESTAMP_MICROS || logicalType == LogicalType.TIMESTAMP_MILLIS;
    boolean isDate = logicalType == LogicalType.DATE;
    boolean isTimestampOrDate = isTimestamp || isDate;

    // If timePartitioningType is HOUR, then logicalType cannot be DATE Only TIMESTAMP_MICROS and TIMESTAMP_MILLIS
    if (timePartitioningType == TimePartitioning.Type.HOUR && !isTimestamp) {
      collector.addFailure(
                      String.format("Partition column '%s' is of invalid type '%s'.",
                              columnName, fieldSchema.getDisplayName()),
                      "Partition column must be a timestamp.").withConfigProperty(NAME_PARTITION_BY_FIELD)
              .withOutputSchemaField(columnName).withInputSchemaField(columnName);

    // For any other timePartitioningType (DAY, MONTH, YEAR) logicalType can be DATE, TIMESTAMP_MICROS, TIMESTAMP_MILLIS
    } else if (!isTimestampOrDate) {
      collector.addFailure(
                      String.format("Partition column '%s' is of invalid type '%s'.",
                              columnName, fieldSchema.getDisplayName()),
                      "Partition column must be a date or timestamp.").withConfigProperty(NAME_PARTITION_BY_FIELD)
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

  private void validateClusteringOrder(@Nullable Schema schema, FailureCollector collector) {
    if (!shouldCreatePartitionedTable() || Strings.isNullOrEmpty(clusteringOrder) || schema == null) {
      return;
    }

    if (!containsMacro(NAME_PARTITION_BY_FIELD) && !containsMacro(NAME_CLUSTERING_ORDER) &&
      !Strings.isNullOrEmpty(clusteringOrder) && (Strings.isNullOrEmpty(partitionByField))) {
      collector.addFailure(String.format("Clustering order cannot be validated."),
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

      if (!SUPPORTED_CLUSTERING_TYPES.contains(type) && !BigQuerySinkUtils.isSupportedLogicalType(logicalType)) {
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

    Map<String, Integer> keyMap = BigQuerySinkUtils.calculateDuplicates(keyFields);
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

      Map<String, Integer> orderedByFieldMap = BigQuerySinkUtils.calculateDuplicates(dedupeByList);
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

  @Override
  void validateCmekKey(FailureCollector failureCollector, Map<String, String> arguments) {
    CryptoKeyName cmekKeyName = CmekUtils.getCmekKey(cmekKey, arguments, failureCollector);
    //these fields are needed to check if bucket exists or not and for location validation
    if (containsMacro(NAME_LOCATION) || containsMacro(NAME_TABLE) || Strings.isNullOrEmpty(table)) {
      return;
    }
    validateCmekKeyLocation(cmekKeyName, getTable(), location, failureCollector);
  }

  /**
   * Returns true if bigquery table can be connected to or schema is not a macro.
   */
  boolean shouldConnect() {
    return !containsMacro(BigQuerySinkConfig.NAME_DATASET) &&
      !containsMacro(BigQuerySinkConfig.NAME_TABLE) && connection != null && connection.canConnect() &&
      !containsMacro(BigQuerySinkConfig.NAME_SCHEMA);
  }

  public static Builder builder() {
    return new Builder();
  }

  /**
   * BigQuery Sink configuration builder.
   */
  public static class Builder {
    private String referenceName;
    private String serviceAccountType;
    private String serviceFilePath;
    private String serviceAccountJson;
    private String project;
    private String dataset;
    private String table;
    private String cmekKey;
    private String location;
    private String bucket;
    private String jobLabelKeyValue;

    public BigQuerySinkConfig.Builder setReferenceName(@Nullable String referenceName) {
      this.referenceName = referenceName;
      return this;
    }

    public BigQuerySinkConfig.Builder setProject(@Nullable String project) {
      this.project = project;
      return this;
    }

    public BigQuerySinkConfig.Builder setServiceAccountType(@Nullable String serviceAccountType) {
      this.serviceAccountType = serviceAccountType;
      return this;
    }

    public BigQuerySinkConfig.Builder setServiceFilePath(@Nullable String serviceFilePath) {
      this.serviceFilePath = serviceFilePath;
      return this;
    }

    public BigQuerySinkConfig.Builder setServiceAccountJson(@Nullable String serviceAccountJson) {
      this.serviceAccountJson = serviceAccountJson;
      return this;
    }

    public BigQuerySinkConfig.Builder setDataset(@Nullable String dataset) {
      this.dataset = dataset;
      return this;
    }

    public BigQuerySinkConfig.Builder setTable(@Nullable String table) {
      this.table = table;
      return this;
    }

    public BigQuerySinkConfig.Builder setCmekKey(@Nullable String cmekKey) {
      this.cmekKey = cmekKey;
      return this;
    }

    public BigQuerySinkConfig.Builder setLocation(@Nullable String location) {
      this.location = location;
      return this;
    }

    public BigQuerySinkConfig.Builder setBucket(@Nullable String bucket) {
      this.bucket = bucket;
      return this;
    }
    public BigQuerySinkConfig.Builder setJobLabelKeyValue(@Nullable String jobLabelKeyValue) {
      this.jobLabelKeyValue = jobLabelKeyValue;
      return this;
    }

    public BigQuerySinkConfig build() {
      return new BigQuerySinkConfig(
        referenceName,
        project,
        serviceAccountType,
        serviceFilePath,
        serviceAccountJson,
        dataset,
        table,
        location,
        cmekKey,
        bucket,
        jobLabelKeyValue
      );
    }

  }
}
