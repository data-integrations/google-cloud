/*
 * Copyright © 2019 Cask Data, Inc.
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

import com.google.auth.Credentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.hadoop.io.bigquery.BigQueryConfiguration;
import com.google.cloud.hadoop.io.bigquery.BigQueryFileFormat;
import com.google.cloud.hadoop.io.bigquery.output.BigQueryOutputConfiguration;
import com.google.cloud.hadoop.io.bigquery.output.BigQueryTableFieldSchema;
import com.google.cloud.hadoop.io.bigquery.output.BigQueryTableSchema;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.common.base.Strings;
import io.cdap.cdap.api.data.batch.Output;
import io.cdap.cdap.api.data.batch.OutputFormatProvider;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.batch.BatchRuntimeContext;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;
import io.cdap.cdap.etl.api.validation.ValidationFailure;
import io.cdap.plugin.common.LineageRecorder;
import io.cdap.plugin.format.avro.StructuredToAvroTransformer;
import io.cdap.plugin.gcp.bigquery.util.BigQueryConstants;
import io.cdap.plugin.gcp.bigquery.util.BigQueryUtil;
import io.cdap.plugin.gcp.common.GCPUtils;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Base class for Big Query batch sink plugins.
 */
public abstract class AbstractBigQuerySink extends BatchSink<StructuredRecord, AvroKey<GenericRecord>, NullWritable> {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractBigQuerySink.class);

  private static final String gcsPathFormat = "gs://%s";
  private static final String temporaryBucketFormat = gcsPathFormat + "/input/%s-%s";
  public static final String RECORDS_UPDATED_METRIC = "records.updated";

  // UUID for the run. Will be used as bucket name if bucket is not provided.
  // UUID is used since GCS bucket names must be globally unique.
  private final UUID uuid = UUID.randomUUID();
  protected Configuration baseConfiguration;
  private StructuredToAvroTransformer avroTransformer;
  protected BigQuery bigQuery;

  /**
   * Executes main prepare run logic. Child classes cannot override this method,
   * instead they should implement two methods {@link #prepareRunValidation(BatchSinkContext)}
   * and {@link #prepareRunInternal(BatchSinkContext, BigQuery, String)} in order to add custom logic.
   *
   * @param context batch sink context
   */
  @Override
  public final void prepareRun(BatchSinkContext context) throws Exception {
    prepareRunValidation(context);

    AbstractBigQuerySinkConfig config = getConfig();
    String serviceAccountFilePath = config.getServiceAccountFilePath();
    Credentials credentials = serviceAccountFilePath == null ?
      null : GCPUtils.loadServiceAccountCredentials(serviceAccountFilePath);
    String project = config.getProject();
    bigQuery = GCPUtils.getBigQuery(project, credentials);
    String cmekKey = context.getArguments().get(GCPUtils.CMEK_KEY);
    baseConfiguration = getBaseConfiguration(cmekKey);
    String bucket = configureBucket();
    if (!context.isPreviewEnabled()) {
      createResources(bigQuery, GCPUtils.getStorage(project, credentials), config.getDataset(), bucket,
                      config.getLocation(), cmekKey);
    }

    prepareRunInternal(context, bigQuery, bucket);
  }

  @Override
  public void onRunFinish(boolean succeeded, BatchSinkContext context) {
    if (getConfig().getBucket() == null) {
      Path gcsPath = new Path(String.format(gcsPathFormat, uuid.toString()));
      try {
        FileSystem fs = gcsPath.getFileSystem(baseConfiguration);
        if (fs.exists(gcsPath)) {
          fs.delete(gcsPath, true);
          LOG.debug("Deleted temporary bucket '{}'", gcsPath);
        }
      } catch (IOException e) {
        LOG.warn("Failed to delete bucket '{}': {}", gcsPath, e.getMessage());
      }
    }
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    avroTransformer = new StructuredToAvroTransformer(null);
  }

  /**
   * Transform the given {@link StructuredRecord} into avro {@link GenericRecord} with the same schema.
   */
  protected final GenericRecord toAvroRecord(StructuredRecord record) throws IOException {
    return avroTransformer.transform(record, record.getSchema());
  }

  /**
   * Initializes output along with lineage recording for given table and its schema.
   *
   * @param context batch sink context
   * @param bigQuery big query client for the configured project
   * @param outputName output name
   * @param tableName table name
   * @param tableSchema table schema
   * @param bucket bucket name
   */
  protected final void initOutput(BatchSinkContext context, BigQuery bigQuery, String outputName, String tableName,
                                  @Nullable Schema tableSchema, String bucket,
                                  FailureCollector collector) throws IOException {
    LOG.debug("Init output for table '{}' with schema: {}", tableName, tableSchema);

    List<BigQueryTableFieldSchema> fields = getBigQueryTableFields(bigQuery, tableName, tableSchema,
                                                                   getConfig().isAllowSchemaRelaxation(), collector);
    Configuration configuration = getOutputConfiguration(bucket, tableName, fields);

    // Both emitLineage and setOutputFormat internally try to create an external dataset if it does not already exist.
    // We call emitLineage before since it creates the dataset with schema which is used.
    List<String> fieldNames = fields.stream()
      .map(BigQueryTableFieldSchema::getName)
      .collect(Collectors.toList());
    recordLineage(context, outputName, tableSchema, fieldNames);
    context.addOutput(Output.of(outputName, getOutputFormatProvider(configuration, tableName, tableSchema)));
  }

  /**
   * Child classes must provide configuration based on {@link AbstractBigQuerySinkConfig}.
   *
   * @return config instance
   */
  protected abstract AbstractBigQuerySinkConfig getConfig();

  /**
   * Child classes must override this method to provide specific validation logic to executed before
   * actual {@link #prepareRun(BatchSinkContext)} method execution.
   * For example, Batch Sink plugin can validate schema right away,
   * Batch Multi Sink does not have information at this point to do the validation.
   *
   * @param context batch sink context
   */
  protected abstract void prepareRunValidation(BatchSinkContext context);

  /**
   * Executes main prepare run logic, i.e. prepares output for given table (for Batch Sink plugin)
   * or for a number of tables (for Batch Multi Sink plugin).
   *
   * @param context batch sink context
   * @param bigQuery a big query client for the configured project
   * @param bucket bucket name
   */
  protected abstract void prepareRunInternal(BatchSinkContext context, BigQuery bigQuery,
                                             String bucket) throws IOException;

  /**
   * Returns output format provider instance specific to the child classes that extend this class.
   *
   * @param configuration Hadoop configuration
   * @param tableName table name
   * @param tableSchema table schema
   * @return output format provider
   */
  protected abstract OutputFormatProvider getOutputFormatProvider(Configuration configuration,
                                                                  String tableName,
                                                                  Schema tableSchema);

  /**
   * Initialized base configuration needed to load data into BigQuery table.
   *
   * @return base configuration
   */
  private Configuration getBaseConfiguration(@Nullable String cmekKey) throws IOException {
    AbstractBigQuerySinkConfig config = getConfig();
    Configuration baseConfiguration = BigQueryUtil.getBigQueryConfig(config.getServiceAccountFilePath(),
                                                                     config.getProject(), cmekKey);
    baseConfiguration.setBoolean(BigQueryConstants.CONFIG_ALLOW_SCHEMA_RELAXATION,
                                 config.isAllowSchemaRelaxation());
    baseConfiguration.setStrings(BigQueryConfiguration.OUTPUT_TABLE_WRITE_DISPOSITION_KEY,
                                 config.getWriteDisposition().name());
    // this setting is needed because gcs has default chunk size of 64MB. This is large default chunk size which can
    // cause OOM issue if there are many tables being written. See this - CDAP-16670
    String gcsChunkSize = "8388608";
    if (!Strings.isNullOrEmpty(config.getGcsChunkSize())) {
      gcsChunkSize = config.getGcsChunkSize();
    }
    baseConfiguration.set("fs.gs.outputstream.upload.chunk.size", gcsChunkSize);
    return baseConfiguration;
  }

  /**
   * Generates full path to temporary bucket based on given bucket and table names.
   *
   * @param bucket bucket name
   * @param tableName table name
   * @return full path to temporary bucket
   */
  private String getTemporaryGcsPath(String bucket, String tableName) {
    return String.format(temporaryBucketFormat, bucket, tableName, uuid);
  }

  /**
   * Updates {@link #baseConfiguration} with bucket details.
   * Uses provided bucket, otherwise generates random.
   *
   * @return bucket name
   */
  private String configureBucket() {
    String bucket = getConfig().getBucket();
    if (bucket == null) {
      bucket = uuid.toString();
      // By default, this option is false, meaning the job can not delete the bucket.
      // So enable it only when bucket name is not provided.
      baseConfiguration.setBoolean("fs.gs.bucket.delete.enable", true);
    }
    baseConfiguration.set("fs.gs.system.bucket", bucket);
    baseConfiguration.setBoolean("fs.gs.impl.disable.cache", true);
    baseConfiguration.setBoolean("fs.gs.metadata.cache.enable", false);
    return bucket;
  }

  /**
   * Validates output schema against Big Query table schema. It throws {@link IllegalArgumentException}
   * if the output schema has more fields than Big Query table or output schema field types does not match
   * Big Query column types unless schema relaxation policy is allowed.
   *
   * @param table big query table
   * @param tableSchema table schema
   * @param allowSchemaRelaxation allows schema relaxation policy
   * @param collector failure collector
   */
  protected void validateSchema(Table table, Schema tableSchema, boolean allowSchemaRelaxation,
                                FailureCollector collector) {
    String tableName = table.getTableId().getTable();
    com.google.cloud.bigquery.Schema bqSchema = table.getDefinition().getSchema();
    if (bqSchema == null || bqSchema.getFields().isEmpty()) {
      // Table is created without schema, so no further validation is required.
      return;
    }

    if (getConfig().isTruncateTableSet()) {
      //no validation required for schema if truncate table is set.
      // BQ will overwrite the schema for normal tables when write disposition is WRITE_TRUNCATE
      //note - If write to single partition is supported in future, schema validation will be necessary
      return;
    }

    FieldList bqFields = bqSchema.getFields();
    List<Schema.Field> outputSchemaFields = Objects.requireNonNull(tableSchema.getFields());

    List<String> missingBQFields = BigQueryUtil.getSchemaMinusBqFields(outputSchemaFields, bqFields);

    if (allowSchemaRelaxation) {
      List<String> nonNullableFields = missingBQFields.stream()
        .map(tableSchema::getField)
        .filter(Objects::nonNull)
        .filter(field -> !field.getSchema().isNullable())
        .map(Schema.Field::getName)
        .collect(Collectors.toList());

      for (String nonNullableField : nonNullableFields) {
        collector.addFailure(
          String.format("Required field '%s' does not exist in BigQuery table '%s.%s'.",
                        nonNullableField, getConfig().getDataset(), tableName),
          "Change the field to be nullable.")
          .withInputSchemaField(nonNullableField).withOutputSchemaField(nonNullableField);
      }
    } else {
      // schema should not have fields that are not present in BigQuery table,
      for (String missingField : missingBQFields) {
        collector.addFailure(
          String.format("Field '%s' does not exist in BigQuery table '%s.%s'.",
                        missingField, getConfig().getDataset(), tableName),
          String.format("Remove '%s' from the input, or add a column to the BigQuery table.", missingField))
          .withInputSchemaField(missingField).withOutputSchemaField(missingField);
      }

      // validate the missing columns in output schema are nullable fields in BigQuery
      List<String> remainingBQFields = BigQueryUtil.getBqFieldsMinusSchema(bqFields, outputSchemaFields);
      for (String field : remainingBQFields) {
        if (bqFields.get(field).getMode() != Field.Mode.NULLABLE) {
          collector.addFailure(String.format("Required Column '%s' is not present in the schema.", field),
                               String.format("Add '%s' to the schema.", field));
        }
      }
    }

    // Match output schema field type with BigQuery column type
    for (Schema.Field field : tableSchema.getFields()) {
      String fieldName = field.getName();
      // skip checking schema if field is missing in BigQuery
      if (!missingBQFields.contains(fieldName)) {
        ValidationFailure failure = BigQueryUtil.validateFieldSchemaMatches(
          bqFields.get(field.getName()), field, getConfig().getDataset(), tableName,
          AbstractBigQuerySinkConfig.SUPPORTED_TYPES, collector);
        if (failure != null) {
          failure.withInputSchemaField(fieldName).withOutputSchemaField(fieldName);
        }
      }
    }
    collector.getOrThrowException();
  }

  /**
   * Generates Big Query field instances based on given CDAP table schema after schema validation.
   *
   * @param bigQuery big query object
   * @param tableName table name
   * @param tableSchema table schema
   * @param allowSchemaRelaxation if schema relaxation policy is allowed
   * @param collector failure collector
   * @return list of Big Query fields
   */
  private List<BigQueryTableFieldSchema> getBigQueryTableFields(BigQuery bigQuery, String tableName,
                                                                @Nullable Schema tableSchema,
                                                                boolean allowSchemaRelaxation,
                                                                FailureCollector collector) {
    if (tableSchema == null) {
      return Collections.emptyList();
    }

    TableId tableId = TableId.of(getConfig().getProject(), getConfig().getDataset(), tableName);
    try {
      Table table = bigQuery.getTable(tableId);
      // if table is null that mean it does not exist. So there is no need to perform validation
      if (table != null) {
        validateSchema(table, tableSchema, allowSchemaRelaxation, collector);
      }
    } catch (BigQueryException e) {
      collector.addFailure("Unable to get details about the BigQuery table: " + e.getMessage(), null)
        .withConfigProperty("table");
      throw collector.getOrThrowException();
    }

    List<Schema.Field> inputFields = Objects.requireNonNull(tableSchema.getFields(), "Schema must have fields");
    return inputFields.stream()
      .map(this::generateTableFieldSchema)
      .collect(Collectors.toList());
  }

  private BigQueryTableFieldSchema generateTableFieldSchema(Schema.Field field) {
    BigQueryTableFieldSchema fieldSchema = new BigQueryTableFieldSchema();
    fieldSchema.setName(field.getName());
    fieldSchema.setMode(getMode(field.getSchema()).name());
    LegacySQLTypeName type = getTableDataType(field.getSchema());
    fieldSchema.setType(type.name());
    if (type == LegacySQLTypeName.RECORD) {
      List<Schema.Field> schemaFields;
      if (Schema.Type.ARRAY == field.getSchema().getType()) {
        schemaFields = Objects.requireNonNull(field.getSchema().getComponentSchema()).getFields();
      } else {
        schemaFields = field.getSchema().isNullable()
          ? field.getSchema().getNonNullable().getFields()
          : field.getSchema().getFields();
      }
      fieldSchema.setFields(Objects.requireNonNull(schemaFields).stream()
                              .map(this::generateTableFieldSchema)
                              .collect(Collectors.toList()));

    }
    return fieldSchema;
  }

  private Field.Mode getMode(Schema schema) {
    if (schema.isNullable()) {
      return Field.Mode.NULLABLE;
    } else if (schema.getType() == Schema.Type.ARRAY) {
      return Field.Mode.REPEATED;
    }
    return Field.Mode.REQUIRED;
  }

  /**
   * Creates Hadoop configuration for the given table and its fields.
   *
   * @param bucket bucket name
   * @param tableName table name
   * @param fields list of Big Query fields
   * @return Hadoop configuration
   */
  private Configuration getOutputConfiguration(String bucket,
                                               String tableName,
                                               List<BigQueryTableFieldSchema> fields) throws IOException {
    Configuration configuration = new Configuration(baseConfiguration);
    String temporaryGcsPath = getTemporaryGcsPath(bucket, tableName);

    BigQueryTableSchema outputTableSchema = new BigQueryTableSchema();
    if (!fields.isEmpty()) {
      outputTableSchema.setFields(fields);
    }

    BigQueryOutputConfiguration.configure(
      configuration,
      String.format("%s.%s", getConfig().getDataset(), tableName),
      outputTableSchema,
      temporaryGcsPath,
      BigQueryFileFormat.AVRO,
      AvroOutputFormat.class);

    return configuration;
  }

  private void recordLineage(BatchSinkContext context,
                             String outputName,
                             Schema tableSchema,
                             List<String> fieldNames) {
    LineageRecorder lineageRecorder = new LineageRecorder(context, outputName);
    lineageRecorder.createExternalDataset(tableSchema);
    if (!fieldNames.isEmpty()) {
      lineageRecorder.recordWrite("Write", "Wrote to BigQuery table.", fieldNames);
    }
  }

  private LegacySQLTypeName getTableDataType(Schema schema) {
    schema = BigQueryUtil.getNonNullableSchema(schema);
    Schema.LogicalType logicalType = schema.getLogicalType();

    if (logicalType != null) {
      switch (logicalType) {
        case DATE:
          return LegacySQLTypeName.DATE;
        case TIME_MILLIS:
        case TIME_MICROS:
          return LegacySQLTypeName.TIME;
        case TIMESTAMP_MILLIS:
        case TIMESTAMP_MICROS:
          return LegacySQLTypeName.TIMESTAMP;
        case DECIMAL:
          return LegacySQLTypeName.NUMERIC;
        default:
          throw new IllegalStateException("Unsupported type " + logicalType.getToken());
      }
    }

    Schema.Type type = schema.getType();
    switch (type) {
      case INT:
      case LONG:
        return LegacySQLTypeName.INTEGER;
      case STRING:
        return LegacySQLTypeName.STRING;
      case FLOAT:
      case DOUBLE:
        return LegacySQLTypeName.FLOAT;
      case BOOLEAN:
        return LegacySQLTypeName.BOOLEAN;
      case BYTES:
        return LegacySQLTypeName.BYTES;
      case ARRAY:
        return getTableDataType(schema.getComponentSchema());
      case RECORD:
        return LegacySQLTypeName.RECORD;
      default:
        throw new IllegalStateException("Unsupported type " + type);
    }
  }

  /**
   * Creates the given dataset and bucket if they do not already exist. If the dataset already exists but the
   * bucket does not, the bucket will be created in the same location as the dataset. If the bucket already exists
   * but the dataset does not, the dataset will attempt to be created in the same location. This may fail if the bucket
   * is in a location that BigQuery does not yet support.
   *
   * @param bigQuery the bigquery client for the project
   * @param storage the storage client for the project
   * @param datasetName the name of the dataset
   * @param bucketName the name of the bucket
   * @param location the location of the resources, this is only applied if both the bucket and dataset do not exist
   * @param cmekKey the name of the cmek key
   * @throws IOException if there was an error creating or fetching any GCP resource
   */
  private static void createResources(BigQuery bigQuery, Storage storage,
                                     String datasetName, String bucketName, @Nullable String location,
                                     @Nullable String cmekKey) throws IOException {
    Dataset dataset = bigQuery.getDataset(datasetName);
    Bucket bucket = storage.get(bucketName);

    if (dataset == null && bucket == null) {
      createBucket(storage, bucketName, location, cmekKey,
                   () -> String.format("Unable to create Cloud Storage bucket '%s'", bucketName));
      createDataset(bigQuery, datasetName, location,
                    () -> String.format("Unable to create BigQuery dataset '%s'", datasetName));
    } else if (bucket == null) {
      createBucket(
        storage, bucketName, dataset.getLocation(), cmekKey,
        () -> String.format(
          "Unable to create Cloud Storage bucket '%s' in the same location ('%s') as BigQuery dataset '%s'. "
            + "Please use a bucket that is in the same location as the dataset.",
          bucketName, dataset.getLocation(), datasetName));
    } else if (dataset == null) {
      createDataset(
        bigQuery, datasetName, bucket.getLocation(),
        () -> String.format(
          "Unable to create BigQuery dataset '%s' in the same location ('%s') as Cloud Storage bucket '%s'. "
            + "Please use a bucket that is in a supported location.",
          datasetName, bucket.getLocation(), bucketName));
    }
  }

  private static void createDataset(BigQuery bigQuery, String dataset, @Nullable String location,
                                    Supplier<String> errorMessage) throws IOException {
    DatasetInfo.Builder builder = DatasetInfo.newBuilder(dataset);
    if (location != null) {
      builder.setLocation(location);
    }
    try {
      bigQuery.create(builder.build());
    } catch (BigQueryException e) {
      if (e.getCode() != 409) {
        // A conflict means the dataset already exists (https://cloud.google.com/bigquery/troubleshooting-errors)
        // This most likely means multiple stages in the same pipeline are trying to create the same dataset.
        // Ignore this and move on, since all that matters is that the dataset exists.
        throw new IOException(errorMessage.get(), e);
      }
    }
  }

  private static void createBucket(Storage storage, String bucket, @Nullable String location,
                                   @Nullable String cmekKey, Supplier<String> errorMessage) throws IOException {
    try {
      GCPUtils.createBucket(storage, bucket, location, cmekKey);
    } catch (StorageException e) {
      if (e.getCode() != 409) {
        // A conflict means the bucket already exists
        // This most likely means multiple stages in the same pipeline are trying to create the same dataset.
        // Ignore this and move on, since all that matters is that the dataset exists.
        throw new IOException(errorMessage.get(), e);
      }
    }
  }
}
