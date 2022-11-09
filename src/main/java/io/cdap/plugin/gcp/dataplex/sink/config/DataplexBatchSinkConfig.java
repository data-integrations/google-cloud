/*
 * Copyright © 2022 Cask Data, Inc.
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

import com.google.api.gax.rpc.ApiException;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.RangePartitioning;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TimePartitioning;
import com.google.cloud.dataplex.v1.Asset;
import com.google.cloud.dataplex.v1.AssetName;
import com.google.cloud.dataplex.v1.DataplexServiceClient;
import com.google.cloud.dataplex.v1.LakeName;
import com.google.cloud.dataplex.v1.Zone;
import com.google.cloud.dataplex.v1.ZoneName;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.InvalidPluginConfigException;
import io.cdap.cdap.api.plugin.InvalidPluginProperty;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.validation.FormatContext;
import io.cdap.cdap.etl.api.validation.ValidatingOutputFormat;
import io.cdap.plugin.common.IdUtils;
import io.cdap.plugin.format.FileFormat;
import io.cdap.plugin.gcp.bigquery.sink.BigQuerySinkUtils;
import io.cdap.plugin.gcp.bigquery.sink.Operation;
import io.cdap.plugin.gcp.bigquery.sink.PartitionType;
import io.cdap.plugin.gcp.bigquery.util.BigQueryUtil;
import io.cdap.plugin.gcp.common.GCPConnectorConfig;
import io.cdap.plugin.gcp.dataplex.common.config.DataplexBaseConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

/**
 * Dataplex plugin UI configuration parameters and validation wrapper
 */
public class DataplexBatchSinkConfig extends DataplexBaseConfig {
  private static final Set<Schema.Type> SUPPORTED_CLUSTERING_TYPES =
    ImmutableSet.of(Schema.Type.INT, Schema.Type.LONG, Schema.Type.STRING, Schema.Type.BOOLEAN, Schema.Type.BYTES);
  private static final Set<FileFormat> SUPPORTED_FORMATS_FOR_CURATED_ZONE =
    ImmutableSet.of(FileFormat.AVRO, FileFormat.ORC, FileFormat.PARQUET);
  private static final int MAX_NUMBER_OF_COLUMNS = 4;
  private static final String NAME_SUFFIX = "suffix";
  private static final String NAME_TABLE = "table";
  private static final String NAME_ASSET = "asset";
  private static final String NAME_ASSET_TYPE = "assetType";
  private static final Logger LOG = LoggerFactory.getLogger(DataplexBatchSinkConfig.class);
  private static final String WHERE = "WHERE";
  protected static final String NAME_FORMAT = "format";
  private static final String NAME_TABLE_KEY = "tableKey";
  private static final String NAME_DEDUPE_BY = "dedupeBy";
  private static final String NAME_OPERATION = "operation";
  private static final String NAME_PARTITION_FILTER = "partitionFilter";
  private static final String NAME_PARTITIONING_TYPE = "partitioningType";
  private static final String NAME_TRUNCATE_TABLE = "truncateTable";
  private static final String NAME_UPDATE_DATAPLEX_METADATA = "updateDataplexMetadata";
  private static final String NAME_UPDATE_SCHEMA = "allowSchemaRelaxation";
  private static final String NAME_PARTITION_BY_FIELD = "partitionField";
  private static final String NAME_REQUIRE_PARTITION_FIELD = "requirePartitionField";
  private static final String NAME_CLUSTERING_ORDER = "clusteringOrder";
  private static final String NAME_RANGE_START = "rangeStart";
  private static final String NAME_RANGE_END = "rangeEnd";
  private static final String NAME_RANGE_INTERVAL = "rangeInterval";
  private static final String NAME_SCHEMA = "schema";
  private static final String CONTENT_TYPE_APPLICATION_JSON = "application/json";
  private static final String CONTENT_TYPE_APPLICATION_AVRO = "application/x-avro";
  private static final String CONTENT_TYPE_APPLICATION_PARQUET = "application/x-parquet";
  private static final String CONTENT_TYPE_APPLICATION_ORC = "application/x-orc";
  private static final String CONTENT_TYPE_TEXT_CSV = "text/csv";
  private static final String FORMAT_AVRO = "avro";
  private static final String FORMAT_CSV = "csv";
  private static final String FORMAT_JSON = "json";
  private static final String FORMAT_ORC = "orc";
  private static final String FORMAT_PARQUET = "parquet";
  private static final Pattern FIELD_PATTERN = Pattern.compile("[a-zA-Z0-9_]+");

  private static final Map<String, String> contentTypeMap = ImmutableMap.of(
    FORMAT_AVRO, CONTENT_TYPE_APPLICATION_AVRO,
    FORMAT_CSV, CONTENT_TYPE_TEXT_CSV,
    FORMAT_JSON, CONTENT_TYPE_APPLICATION_JSON,
    FORMAT_PARQUET, CONTENT_TYPE_APPLICATION_PARQUET,
    FORMAT_ORC, CONTENT_TYPE_APPLICATION_ORC
  );


  @Name(NAME_ASSET)
  @Macro
  @Description("ID of the Dataplex asset. It represents a cloud resource that is being managed within a lake as a " +
    "member of a zone.")
  protected String asset;

  @Name(NAME_ASSET_TYPE)
  @Nullable
  @Description("Type of asset selected to ingest the data in Dataplex.")
  protected String assetType;

  @Name(NAME_FORMAT)
  @Nullable
  @Macro
  @Description("The format to write the records in. The format for a raw zone must be one of ‘json’, ‘avro’," +
    " ‘csv’,‘orc’, or ‘parquet’.  The format for a curated zone must be one of ‘avro’, ‘orc’, or ‘parquet’.")
  protected String format;

  @Name(NAME_TABLE)
  @Nullable
  @Macro
  @Description("The table to write to. It can be BigQuery table if Asset is of Type 'BigQuery Dataset' or" +
    " a directory if Asset is of type 'Storage Bucket'")
  protected String table;

  @Name(NAME_TABLE_KEY)
  @Nullable
  @Macro
  @Description("List of fields that determine relation between tables during Update and Upsert operations.")
  protected String tableKey;

  @Name(NAME_DEDUPE_BY)
  @Nullable
  @Macro
  @Description("Column names and sort order used to choose which input record to update/upsert when there are " +
    "multiple input records with the same key. For example, if this is set to ‘updated_time desc’, then if there " +
    "are multiple input records with the same key, the one with the largest value for ‘updated_time’ will be " +
    "applied.")
  protected String dedupeBy;

  @Name(NAME_OPERATION)
  @Nullable
  @Macro
  @Description("Type of write operation to perform. This can be set to Insert, Update, or Upsert.")
  protected String operation;

  @Name(NAME_PARTITION_FILTER)
  @Nullable
  @Macro
  @Description("Partition filter that can be used for partition elimination during Update or Upsert operations." +
    " Only Use with Update or Upsert operations for tables where Require Partition Filter is enabled. For example," +
    " if the table is partitioned and the Partition Filter  is ‘_PARTITIONTIME > “2020-01-01” " +
    "and _PARTITIONTIME < “2020-03-01”‘, the update operation will be performed only in the" +
    " partitions meeting the criteria.")
  protected String partitionFilter;

  @Name(NAME_PARTITIONING_TYPE)
  @Nullable
  @Macro
  @Description("Specifies the partitioning type. Can either be Integer, Time, or None. Defaults to Time. " +
    "This value is ignored if the table already exists.")
  protected String partitioningType;

  @Name(NAME_RANGE_START)
  @Nullable
  @Macro
  @Description("Start value for range partitioning. The start value is inclusive. Ignored when table already exists")
  protected Long rangeStart;

  @Name(NAME_RANGE_END)
  @Nullable
  @Macro
  @Description("End value for range partitioning. The end value is exclusive. Ignored when table already exists")
  protected Long rangeEnd;

  @Name(NAME_RANGE_INTERVAL)
  @Nullable
  @Macro
  @Description("Interval value for range partitioning. The interval value must be a positive integer. Ignored when " +
    "table already exists")
  protected Long rangeInterval;

  @Name(NAME_TRUNCATE_TABLE)
  @Nullable
  @Macro
  @Description("Whether or not to truncate the table before writing to it. Only use with the Insert " +
    "operation.")
  protected Boolean truncateTable;

  @Name(NAME_UPDATE_DATAPLEX_METADATA)
  @Nullable
  @Macro
  @Description("Whether to update Dataplex metadata for the newly created entities." + 
    "If enabled, the pipeline will automatically copy the output schema to the destination " + 
    "Dataplex entities, and the automated Dataplex Discovery won't run for them.")
  protected Boolean updateDataplexMetadata;

  @Name(NAME_UPDATE_SCHEMA)
  @Nullable
  @Macro
  @Description("Whether the BigQuery table schema should be modified when it does not match the schema expected " +
    "by the pipeline.")
  protected Boolean allowSchemaRelaxation;

  @Name(NAME_PARTITION_BY_FIELD)
  @Nullable
  @Macro
  @Description("Partitioning column for the BigQuery table. Leave blank if the BigQuery table " +
    "is an ingestion-time partitioned table.")
  protected String partitionByField;

  @Name(NAME_REQUIRE_PARTITION_FIELD)
  @Nullable
  @Macro
  @Description("Whether to create a table that requires a partition filter. This value is ignored if the table " +
    "already exists.")
  protected Boolean requirePartitionField;

  @Name(NAME_CLUSTERING_ORDER)
  @Nullable
  @Macro
  @Description("List of fields that determines the sort order of the data. Fields must be of type INT, LONG, " +
    "STRING, DATE, TIMESTAMP, BOOLEAN, or DECIMAL. Tables cannot be clustered on more than 4 fields. This " +
    "value is only used when the BigQuery table is automatically created and ignored if the table already exists.")
  protected String clusteringOrder;

  @Nullable
  @Macro
  @Description("The time format for the output directory that will be appended to the path. " +
    "For example, the format 'yyyy-MM-dd-HH-mm' will result in a directory of the form '2015-01-01-20-42'.")
  private String suffix;

  @Name(NAME_SCHEMA)
  @Nullable
  @Macro
  @Description("The schema of the data to write. If provided, must be compatible with the table schema.")
  private String schema;

  public String getAsset() {
    return asset;
  }

  public String getAssetType() {
    return assetType;
  }

  @Nullable
  public FileFormat getFormat() {
    return FileFormat.from(format, FileFormat::canWrite);
  }

  @Nullable
  public String getFormatStr() {
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
    // remove the WHERE keyword from the filter if the user adds it at the beginning of the expression
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
  public Boolean isUpdateDataplexMetadata() {
    return updateDataplexMetadata != null && updateDataplexMetadata;
  }

  @Nullable
  public Boolean isUpdateTableSchema() {
    return allowSchemaRelaxation != null && allowSchemaRelaxation;
  }

  @Nullable
  public String getPartitionByField() {
    return Strings.isNullOrEmpty(partitionByField) ? null : partitionByField;
  }

  @Nullable
  public Boolean isRequirePartitionField() {
    return requirePartitionField != null && requirePartitionField;
  }

  @Nullable
  public String getClusteringOrder() {
    return Strings.isNullOrEmpty(clusteringOrder) ? null : clusteringOrder;
  }

  @Nullable
  public String getSuffix() {
    return suffix;
  }

  /**
   * Checks whether table name is as per standards and truncating can be performed only while insert operation.
   *
   * @param collector
   */
  public void validateBigQueryDataset(FailureCollector collector) {
    if (!containsMacro(NAME_TABLE)) {
      if (table == null) {
        collector.addFailure(String.format("Required property '%s' has no value.", NAME_TABLE), null)
          .withConfigProperty(NAME_TABLE);

        collector.getOrThrowException();
      }
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

  /**
   * This method will validate the location, lake, zone and asset configurations
   *
   * @param collector
   * @param dataplexServiceClient
   */
  public void validateAssetConfiguration(FailureCollector collector, DataplexServiceClient dataplexServiceClient) {
    if (!Strings.isNullOrEmpty(referenceName)) {
      IdUtils.validateReferenceName(referenceName, collector);
    }
    String projectID = tryGetProject();
    if (!Strings.isNullOrEmpty(location) && !containsMacro(NAME_LOCATION)) {
      if (!Strings.isNullOrEmpty(lake) && !containsMacro(NAME_LAKE)) {
        try {
          dataplexServiceClient.getLake(LakeName.newBuilder().setProject(projectID).setLocation(location)
            .setLake(lake).build());
        } catch (ApiException e) {
          if (e.getMessage().contains("Location")) {
            configureDataplexException(location, NAME_LOCATION, e, collector);
          } else {
            configureDataplexException(lake, NAME_LAKE, e, collector);
          }
          return;
        }

        if (!Strings.isNullOrEmpty(zone) && !containsMacro(NAME_ZONE)) {
          Zone zoneBean;
          try {
            zoneBean =
              dataplexServiceClient.getZone(ZoneName.newBuilder().setProject(projectID).setLocation(location)
                .setLake(lake).setZone(zone).build());
          } catch (ApiException e) {
            configureDataplexException(zone, NAME_ZONE, e, collector);
            return;
          }
          if (!Strings.isNullOrEmpty(asset) && !containsMacro(NAME_ASSET)) {
            try {
              Asset assetBean = dataplexServiceClient.getAsset(AssetName.newBuilder().setProject(projectID)
                .setLocation(location)
                .setLake(lake).setZone(zone).setAsset(asset).build());
              if (!assetType.equalsIgnoreCase(assetBean.getResourceSpec().getType().toString())) {
                collector.addFailure("Asset type doesn't match with actual asset. ", null).
                  withConfigProperty(NAME_ASSET_TYPE);
              }
              if (zoneBean != null && assetBean != null && assetBean.getResourceSpec().getType().
                equals(Asset.ResourceSpec.Type.STORAGE_BUCKET) && zoneBean.getType()
                .equals(Zone.Type.CURATED) && !containsMacro(NAME_FORMAT) &&
                !Strings.isNullOrEmpty(format)) {
                FileFormat fileFormat = getFormat();
                // For curated zone only avro, orc and parquet are supported.
                if (!SUPPORTED_FORMATS_FOR_CURATED_ZONE.contains(fileFormat)) {
                  collector.addFailure(String.format("Format '%s' is not supported for curated zone",
                        fileFormat.toString().toLowerCase()),
                      null).
                    withConfigProperty(NAME_FORMAT);
                }
              }
            } catch (ApiException e) {
              configureDataplexException(asset, NAME_ASSET, e, collector);
              return;
            }
          }
        }
      }
    }
    collector.getOrThrowException();
  }

  /**
   * validates BigQuery Dataset, Table  and columns with selected properties .
   *
   * @param inputSchema           InputSchema
   * @param outputSchema          OutputSchema
   * @param collector             FailureCollector
   * @param dataplexServiceClient DataplexServiceClient
   */
  public void validateBigQueryDataset(@Nullable Schema inputSchema, @Nullable Schema outputSchema,
                                      FailureCollector collector,
                                      DataplexServiceClient dataplexServiceClient) {
    if (containsMacro(NAME_LOCATION) || containsMacro(NAME_LAKE) || containsMacro(NAME_ZONE) ||
      containsMacro(NAME_ASSET)) {
      return;
    }
    validateBigQueryDataset(collector);
    if (!containsMacro(NAME_SCHEMA)) {
      Schema schema = outputSchema == null ? inputSchema : outputSchema;
      try {
        Asset assetBean =
          dataplexServiceClient.getAsset(AssetName.newBuilder().setProject(tryGetProject()).setLocation(location)
            .setLake(lake).setZone(zone).setAsset(asset).build());
        String[] assetValues = assetBean.getResourceSpec().getName().split("/");
        String dataset = assetValues[assetValues.length - 1];
        String datasetProject = assetValues[assetValues.length - 3];
        validatePartitionProperties(schema, collector, dataset, datasetProject);
        validateClusteringOrder(schema, collector);
        validateOperationProperties(schema, collector);
        validateConfiguredSchema(schema, collector, dataset);
      } catch (Exception e) {
        LOG.debug(String.format("%s: %s", e.getLocalizedMessage(), e.getMessage()));
      }

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


  private void validateConfiguredSchema(Schema schema, FailureCollector collector, String dataset) {
    if (!this.shouldConnect()) {
      return;
    }
    String tableName = this.getTable();
    Table table = BigQueryUtil.getBigQueryTable(this.tryGetProject(), dataset, tableName,
      connection.getServiceAccount(), connection.isServiceAccountFilePath(),
      collector);

    if (table != null && !this.containsMacro(NAME_UPDATE_SCHEMA)) {
      // if table already exists, validate schema against underlying bigquery table
      com.google.cloud.bigquery.Schema bqSchema = table.getDefinition().getSchema();
      if (this.getOperation().equals(Operation.INSERT)) {
        BigQuerySinkUtils.validateInsertSchema(table, schema, isUpdateTableSchema(), isTruncateTable(), dataset,
          collector);
      } else if (this.getOperation().equals(Operation.UPSERT)) {
        BigQuerySinkUtils.validateSchema(tableName, bqSchema, schema, isUpdateTableSchema(), isTruncateTable(),
          dataset, collector);
      }
    }
  }


  private void validatePartitionProperties(@Nullable Schema schema, FailureCollector collector, String dataset,
                                           String datasetProject) {
    if (tryGetProject() == null) {
      return;
    }
    String project = datasetProject;
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
      } else {
        validateRangePartitionTableWithInputConfiguration(table, rangePartitioning, collector);
      }
      validateColumnForPartition(partitionByField, schema, collector);
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

      if (!SUPPORTED_CLUSTERING_TYPES.contains(type) && !BigQuerySinkUtils.isSupportedLogicalType(logicalType)) {
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
    boolean updateOrUpsertOperation =
      Operation.UPDATE.equals(assetOperation) || Operation.UPSERT.equals(assetOperation);

    if ((updateOrUpsertOperation) && getTableKey() == null) {
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

    Map<String, Integer> keyMap = BigQuerySinkUtils.calculateDuplicates(keyFields);
    keyMap.keySet().stream()
      .filter(key -> keyMap.get(key) != 1)
      .forEach(key -> collector.addFailure(
          String.format("Table key field '%s' is duplicated.", key),
          String.format("Remove duplicates of Table key field '%s'.", key))
        .withConfigElement(NAME_TABLE_KEY, key)
      );

    if ((updateOrUpsertOperation) && getDedupeBy() != null) {
      List<String> dedupeByList = Arrays.stream(Objects.requireNonNull(getDedupeBy()).split(","))
        .collect(Collectors.toList());

      //Validating the list of dedup key fields against fields received from bigquery input table
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


  /**
   * Returns true if dataplex can be connected to or schema is not a macro.
   */
  public boolean shouldConnect() {
    return !containsMacro(NAME_ASSET) && !containsMacro(NAME_TABLE) &&
      !containsMacro(GCPConnectorConfig.NAME_SERVICE_ACCOUNT_TYPE) &&
      !(containsMacro(GCPConnectorConfig.NAME_SERVICE_ACCOUNT_FILE_PATH) ||
        containsMacro(GCPConnectorConfig.NAME_SERVICE_ACCOUNT_JSON)) &&
      !containsMacro(GCPConnectorConfig.NAME_PROJECT) && !containsMacro(NAME_SCHEMA);
  }

  protected ValidatingOutputFormat getValidatingOutputFormat(PipelineConfigurer pipelineConfigurer) {
    return pipelineConfigurer.usePlugin("validatingOutputFormat",
      format.toLowerCase(), format.toLowerCase(), this.getRawProperties());
  }

  /**
   * Validates if format is a valid one that can be ingested in Dataplex.
   *
   * @param pipelineConfigurer
   * @param collector
   */
  public void validateFormatForStorageBucket(PipelineConfigurer pipelineConfigurer, FailureCollector collector) {
    if (!this.containsMacro(NAME_FORMAT) && Strings.isNullOrEmpty(format)) {
      collector.addFailure(String.format("Required field '%s' has no value.", NAME_FORMAT), null)
        .withConfigProperty(NAME_FORMAT);
      collector.getOrThrowException();
    }

    if (!this.containsMacro(NAME_FORMAT)) {
      // Validates output format for the selected format type
      String fileFormat = null;
      try {
        fileFormat = getFormat().toString().toLowerCase();
      } catch (IllegalArgumentException e) {
        collector.addFailure(e.getMessage(), null).withConfigProperty(NAME_FORMAT)
          .withStacktrace(e.getStackTrace());
      }
      ValidatingOutputFormat validatingOutputFormat = this.getValidatingOutputFormat(pipelineConfigurer);
      FormatContext context = new FormatContext(collector, pipelineConfigurer.getStageConfigurer().getInputSchema());
      this.validateOutputFormatProvider(context, fileFormat, validatingOutputFormat);
    } else {
      // Verifying all the file formats have its corresponding validating output format classes or not
      FileFormat[] fileFormats = FileFormat.values();
      int fileFormatLength = fileFormats.length;

      for (int i = 0; i < fileFormatLength; ++i) {
        FileFormat f = fileFormats[i];
        try {
          pipelineConfigurer.usePlugin("validatingOutputFormat", f.name().toLowerCase(),
            f.name().toLowerCase(), this.getRawProperties());
        } catch (InvalidPluginConfigException var8) {
          LOG.warn(
            "Failed to register format '{}', which means it cannot be used when the pipeline is run. " +
              "Missing properties: {}, invalid properties: {}",
            f.name(), var8.getMissingProperties(), var8.getInvalidProperties().stream().map(
              InvalidPluginProperty::getName).collect(Collectors.toList()));
        }
      }
    }
  }

  /**
   * Validates the schema based on validating output format.
   *
   * @param context                FormatContext
   * @param format
   * @param validatingOutputFormat ValidatingOutputFormat
   */
  public void validateOutputFormatProvider(FormatContext context, String format,
                                           @Nullable ValidatingOutputFormat validatingOutputFormat) {
    FailureCollector collector = context.getFailureCollector();
    if (validatingOutputFormat == null) {
      collector.addFailure(String.format("Could not find the '%s' output format plugin.", format), null)
        .withPluginNotFound(format, format, "validatingOutputFormat");
    } else {
      validatingOutputFormat.validate(context);
    }
  }

  /**
   * Validates the Cloud Storage asset properties before ingestion to dataplex.
   *
   * @param collector FailureCollector
   */
  public void validateStorageBucket(FailureCollector collector) {
    if (containsMacro(NAME_LOCATION) || containsMacro(NAME_LAKE) || containsMacro(NAME_ZONE) ||
      containsMacro(NAME_ASSET)) {
      return;
    }
    if (!containsMacro(NAME_TABLE)) {
      if (table == null) {
        collector.addFailure(String.format("Required property '%s' has no value.", NAME_TABLE), null)
          .withConfigProperty(NAME_TABLE);
        collector.getOrThrowException();
      }
    }
    if (!Strings.isNullOrEmpty(suffix) && !containsMacro(NAME_SUFFIX)) {
      try {
        new SimpleDateFormat(suffix);
      } catch (IllegalArgumentException e) {
        collector.addFailure("Invalid suffix.", "Ensure provided suffix is valid.")
          .withConfigProperty(NAME_SUFFIX).withStacktrace(e.getStackTrace());
      }
    }
    try {
      getSchema(collector);
    } catch (IllegalArgumentException e) {
      collector.addFailure(e.getMessage(), null).withConfigProperty(NAME_SCHEMA)
        .withStacktrace(e.getStackTrace());
    }
  }

  /*  This method gets the value of content type. Valid content types for each format are:
   *
   *  avro -> application/avro, application/octet-stream
   *  json -> application/json, text/plain, application/octet-stream
   *  csv -> application/csv, text/csv, text/plain, application/octet-stream
   *  orc -> application/octet-stream
   *  parquet -> application/octet-stream
   */
  @Nullable
  public String getContentType(String format) {
    return contentTypeMap.get(format.toLowerCase());
  }


  private DataplexBatchSinkConfig(@Nullable String referenceName, String asset, @Nullable String assetType,
                                  @Nullable String location, @Nullable String lake, @Nullable String zone,
                                  @Nullable String format, @Nullable GCPConnectorConfig connection,
                                  @Nullable String table, @Nullable String tableKey, @Nullable String dedupeBy,
                                  @Nullable String operation, @Nullable String partitionFilter,
                                  @Nullable String partitioningType, @Nullable Long rangeStart,
                                  @Nullable Long rangeEnd, @Nullable Long rangeInterval,
                                  @Nullable Boolean truncateTable, @Nullable Boolean updateDataplexMetadata, 
                                  @Nullable Boolean allowSchemaRelaxation, @Nullable String partitionByField,
                                  @Nullable Boolean requirePartitionField, @Nullable String clusteringOrder, 
                                  @Nullable String suffix, @Nullable String schema) {
    this.referenceName = referenceName;
    this.connection = connection;
    this.location = location;
    this.lake = lake;
    this.zone = zone;
    this.asset = asset;
    this.assetType = assetType;
    this.format = format;
    this.table = table;
    this.tableKey = tableKey;
    this.dedupeBy = dedupeBy;
    this.operation = operation;
    this.partitionFilter = partitionFilter;
    this.partitioningType = partitioningType;
    this.rangeStart = rangeStart;
    this.rangeEnd = rangeEnd;
    this.rangeInterval = rangeInterval;
    this.truncateTable = truncateTable;
    this.updateDataplexMetadata = updateDataplexMetadata;
    this.allowSchemaRelaxation = allowSchemaRelaxation;
    this.partitionByField = partitionByField;
    this.requirePartitionField = requirePartitionField;
    this.clusteringOrder = clusteringOrder;
    this.suffix = suffix;
    this.schema = schema;
  }

  public static DataplexBatchSinkConfig.Builder builder() {
    return new DataplexBatchSinkConfig.Builder();
  }

  /**
   * Dataplex Batch Sink configuration builder.
   */

  public static class Builder {
    private String asset;
    private String assetType;
    private String format;
    private String table;
    private String tableKey;
    private String dedupeBy;
    private String operation;
    private String partitionFilter;
    private String partitioningType;
    private Long rangeStart;
    private Long rangeEnd;
    private Long rangeInterval;
    private Boolean truncateTable;
    private Boolean updateDataplexMetadata;
    private Boolean allowSchemaRelaxation;
    private String partitionByField;
    private Boolean requirePartitionField;
    private String clusteringOrder;
    private String suffix;
    private String schema;
    private String location;
    private String lake;
    private String zone;
    private GCPConnectorConfig connection;
    private String referenceName;

    public Builder setAsset(String asset) {
      this.asset = asset;
      return this;
    }

    public Builder setAssetType(String assetType) {
      this.assetType = assetType;
      return this;
    }

    public Builder setFormat(String format) {
      this.format = format;
      return this;
    }

    public Builder setTable(String table) {
      this.table = table;
      return this;
    }

    public Builder setTableKey(String tableKey) {
      this.tableKey = tableKey;
      return this;
    }

    public Builder setDedupeBy(String dedupeBy) {
      this.dedupeBy = dedupeBy;
      return this;
    }

    public Builder setOperation(String operation) {
      this.operation = operation;
      return this;
    }

    public Builder setPartitionFilter(String partitionFilter) {
      this.partitionFilter = partitionFilter;
      return this;
    }

    public Builder setPartitioningType(String partitioningType) {
      this.partitioningType = partitioningType;
      return this;
    }

    public Builder setRangeStart(Long rangeStart) {
      this.rangeStart = rangeStart;
      return this;
    }

    public Builder setRangeEnd(Long rangeEnd) {
      this.rangeEnd = rangeEnd;
      return this;
    }

    public Builder setRangeInterval(Long rangeInterval) {
      this.rangeInterval = rangeInterval;
      return this;
    }

    public Builder setTruncateTable(Boolean truncateTable) {
      this.truncateTable = truncateTable;
      return this;
    }

    public Builder setUpdateDataplexMetadata(Boolean updateDataplexMetadata) {
      this.updateDataplexMetadata = updateDataplexMetadata;
      return this;
    }

    public Builder setAllowSchemaRelaxation(Boolean allowSchemaRelaxation) {
      this.allowSchemaRelaxation = allowSchemaRelaxation;
      return this;
    }

    public Builder setPartitionByField(String partitionByField) {
      this.partitionByField = partitionByField;
      return this;
    }

    public Builder setRequirePartitionField(Boolean requirePartitionField) {
      this.requirePartitionField = requirePartitionField;
      return this;
    }

    public Builder setClusteringOrder(String clusteringOrder) {
      this.clusteringOrder = clusteringOrder;
      return this;
    }

    public Builder setSuffix(String suffix) {
      this.suffix = suffix;
      return this;
    }

    public Builder setSchema(String schema) {
      this.schema = schema;
      return this;
    }

    public Builder setLocation(String location) {
      this.location = location;
      return this;
    }

    public Builder setLake(String lake) {
      this.lake = lake;
      return this;
    }

    public Builder setZone(String zone) {
      this.zone = zone;
      return this;
    }

    public Builder setConnection(GCPConnectorConfig connection) {
      this.connection = connection;
      return this;
    }

    public Builder setReferenceName(String referenceName) {
      this.referenceName = referenceName;
      return this;
    }

    public DataplexBatchSinkConfig build() {
      return new DataplexBatchSinkConfig(referenceName, asset, assetType, location, lake, zone, format, connection,
        table, tableKey, dedupeBy, operation, partitionFilter,
        partitioningType, rangeStart, rangeEnd, rangeInterval, truncateTable,
        updateDataplexMetadata, allowSchemaRelaxation, partitionByField,
        requirePartitionField, clusteringOrder, suffix, schema);
    }

  }

}
